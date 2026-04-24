from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import logging
import re

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Mm, Pt

from bot import config

log = logging.getLogger(__name__)

SERVICE_URLS = {
    "kapusta": "https://kapusta.by",
    "finkit": "https://finkit.by",
    "zaimis": "https://zaimis.by",
}

GENERATED_DOCS_DIR = Path(config.BASE_DIR) / "data" / "generated-docs"
SMS_MAX_LEN = 140
SMS_SERVICE_NAMES = {
    "finkit": "ФинКит",
    "zaimis": "ЗАЙМись",
    "kapusta": "Kapusta",
}
ADDRESS_ABBREVIATIONS = (
    (r"^город\s+", "г. "),
    (r"^г\.?\s+", "г. "),
    (r"^деревня\s+", "д. "),
    (r"^д\.?\s+", "д. "),
    (r"^агрогородок\s+", "аг. "),
    (r"^аг\.?\s+", "аг. "),
    (r"^пос[её]лок\s+", "пос. "),
    (r"^пос\.?\s+", "пос. "),
    (r"^улица\s+", "ул. "),
    (r"^ул\.?\s+", "ул. "),
    (r"^проспект\s+", "пр-т "),
    (r"^пр\-т\s+", "пр-т "),
    (r"^переулок\s+", "пер. "),
    (r"^пер\.?\s+", "пер. "),
    (r"^дом\s+", "д. "),
    (r"^д\.?\s+", "д. "),
    (r"^квартира\s+", "кв. "),
    (r"^кв\.?\s+", "кв. "),
)
LOCALITY_PREFIXES = ("г.", "город ", "д.", "деревня ", "аг.", "агрогородок ", "пос.", "поселок ", "посёлок ")


def _money(value: float | int | None) -> str:
    try:
        return f"{float(value or 0):.2f} BYN"
    except (TypeError, ValueError):
        return "0.00 BYN"


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    for candidate in (value, value.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def _date(value: str | None) -> str:
    dt = _parse_date(value)
    if dt:
        return dt.strftime("%d.%m.%Y")
    return value or "—"


def _service_url(case: dict) -> str:
    return SERVICE_URLS.get(case.get("service"), "")


def _loan_ref(case: dict) -> str:
    return case.get("loan_number") or case.get("loan_id") or case.get("external_id") or "—"


def _voluntary_term(case: dict) -> str:
    days = case.get("voluntary_term_days")
    return str(days) if days else "—"


def _sms_name(full_name: str | None) -> str:
    parts = [part for part in str(full_name or "").strip().split() if part]
    if not parts:
        return "Должник"
    if len(parts) == 1:
        return parts[0][:20]
    surname = parts[0].title()[:18]
    initials = "".join(part[0].upper() for part in parts[1:] if part)
    return f"{surname} {initials}".strip()[:24]


def _sms_ref(case: dict) -> str:
    ref = _loan_ref(case)
    return ref if len(ref) <= 8 else ref[-5:]


def _sms_date(case: dict) -> str:
    dt = _parse_date(case.get("issued_at"))
    if dt:
        return dt.strftime("%d.%m.%y")
    text = case.get("issued_at") or ""
    return text[:10] if text else "—"


def _fit_sms(text: str) -> str:
    compact = " ".join(text.split()).strip()
    if len(compact) <= SMS_MAX_LEN:
        return compact
    trimmed = compact[:SMS_MAX_LEN + 1]
    cut_at = trimmed.rfind(" ")
    if cut_at >= 90:
        return trimmed[:cut_at].rstrip(" ,.-")
    return compact[:SMS_MAX_LEN].rstrip(" ,.-")


def _sms_service(case: dict) -> str:
    return SMS_SERVICE_NAMES.get(str(case.get("service") or "").lower(), str(case.get("service") or "займ"))


def _sms_amount(case: dict) -> str:
    try:
        value = float(case.get("total_due") or 0)
    except (TypeError, ValueError):
        return "0р"
    if value.is_integer():
        return f"{int(value)}р"
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return f"{text}р"


def _address_line(zip_code: str | None, address: str | None) -> str:
    parts = []
    if zip_code:
        parts.append(str(zip_code).strip())
    if address:
        parts.append(str(address).strip())
    line = ", ".join(part for part in parts if part)
    if "беларус" not in line.lower():
        line = f"Республика Беларусь, {line}" if line else "Республика Беларусь"
    return line


def _parse_case_raw(case: dict) -> dict:
    raw = case.get("raw_data")
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _normalize_address_token(token: str) -> str:
    text = " ".join((token or "").split()).strip(" ,")
    if not text:
        return ""
    for pattern, replacement in ADDRESS_ABBREVIATIONS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    text = text.lower().title()
    return (
        text.replace("Г. ", "г. ")
        .replace("Д. ", "д. ")
        .replace("Аг. ", "аг. ")
        .replace("Пос. ", "пос. ")
        .replace("Ул. ", "ул. ")
        .replace("Пер. ", "пер. ")
        .replace("Пр-Т ", "пр-т ")
        .replace("Кв. ", "кв. ")
        .replace(" Область", " область")
        .replace(" Район", " район")
    )


def _split_address(address: str | None) -> dict[str, str | None]:
    parts = [
        _normalize_address_token(part)
        for part in str(address or "").split(",")
        if str(part).strip()
    ]
    city: str | None = None
    street_parts: list[str] = []
    extra: list[str] = []
    for part in parts:
        low = part.lower()
        if "беларус" in low:
            continue
        if city is None and low.startswith(LOCALITY_PREFIXES):
            city = part
            continue
        if low.startswith(("ул.", "пер.", "пр-т", "д.", "кв.")):
            street_parts.append(part)
        else:
            extra.append(part)
    street_line = ", ".join(street_parts) if street_parts else ", ".join(extra)
    return {"city": city, "street_line": street_line or None}


def _postal_lookup_meta(case: dict) -> dict[str, str | None]:
    payload = _parse_case_raw(case)
    lookup = payload.get("postal_lookup") or {}
    postcode = str(lookup.get("postcode") or case.get("borrower_zip") or "").strip() or None
    match_address = str(lookup.get("match_address") or "").strip()
    region = None
    locality = None
    if match_address:
        parts = [part.strip() for part in match_address.split(",") if part.strip()]
        if parts and re.fullmatch(r"\d{6}", parts[0]):
            parts = parts[1:]
        if parts:
            region = _normalize_address_token(parts[0])
        if len(parts) >= 2:
            locality = _normalize_address_token(parts[1])
    return {"postcode": postcode, "region": region, "locality": locality}


def _postal_address_lines(case: dict) -> list[str]:
    address_parts = _split_address(case.get("borrower_address"))
    lookup = _postal_lookup_meta(case)
    lines = [case.get("full_name") or "Получатель не указан"]
    if address_parts.get("street_line"):
        lines.append(str(address_parts["street_line"]))
    if address_parts.get("city") or lookup.get("locality"):
        lines.append(str(address_parts.get("city") or lookup.get("locality")))
    zip_region = ", ".join(part for part in [lookup.get("postcode"), lookup.get("region")] if part)
    if zip_region:
        lines.append(zip_region)
    if case.get("borrower_phone"):
        lines.append(f"Тел: {case['borrower_phone']}")
    return lines


def _debtor_header_lines(case: dict) -> list[str]:
    address_parts = _split_address(case.get("borrower_address"))
    lines = [case.get("full_name") or "—"]
    compact_address = ", ".join(part for part in [address_parts.get("city"), address_parts.get("street_line")] if part)
    if compact_address:
        lines.append(compact_address)
    if case.get("borrower_phone"):
        lines.append(f"Тел: {case['borrower_phone']}")
    if case.get("borrower_email"):
        lines.append(f"Email: {case['borrower_email']}")
    return lines


def _build_contacts_block(name: str | None, address: str | None, phone: str | None, email: str | None) -> list[str]:
    lines = [name or "—"]
    if address:
        lines.append(address)
    if phone:
        lines.append(f"Тел: {phone}")
    if email:
        lines.append(f"Email: {email}")
    return lines


def collect_sms_missing_fields(case: dict, creditor: dict | None) -> list[str]:
    del creditor
    missing: list[str] = []
    if not case.get("full_name"):
        missing.append("ФИО заемщика")
    if not case.get("total_due"):
        missing.append("сумма долга")
    if not _loan_ref(case) or _loan_ref(case) == "—":
        missing.append("номер договора / займа")
    if not case.get("issued_at"):
        missing.append("дата договора / займа")
    return list(dict.fromkeys(missing))


def collect_claim_missing_fields(case: dict, creditor: dict | None, signature: dict | None) -> list[str]:
    missing = collect_sms_missing_fields(case, creditor)
    if not creditor or not creditor.get("full_name"):
        missing.append("ФИО кредитора")
    if not case.get("document_id"):
        missing.append("ИН заемщика")
    if not case.get("borrower_address"):
        missing.append("адрес заемщика")
    if not case.get("borrower_zip"):
        missing.append("ZIP-код заемщика")
    if not case.get("issued_at"):
        missing.append("дата договора / займа")
    if not creditor or not creditor.get("address"):
        missing.append("адрес кредитора")
    if not signature or not signature.get("file_path"):
        missing.append("подпись пользователя")
    return list(dict.fromkeys(missing))


def _join_non_empty(parts: list[str | None]) -> str:
    return ", ".join(part for part in parts if part)


def build_sms_text(case: dict, creditor: dict) -> str:
    del creditor
    text = f"{_sms_name(case.get('full_name'))}, {_sms_service(case)} от {_sms_date(case)}, долг {_sms_amount(case)}."
    optional_parts = [
        f" Займ {_sms_ref(case)}.",
        " Прошу оплатить добровольно.",
        " Иначе обращусь в суд с взысканием расходов.",
    ]
    for part in optional_parts:
        if len(text + part) <= SMS_MAX_LEN:
            text = text + part
    return _fit_sms(text)


def build_postal_address_text(case: dict) -> str:
    return "📮 <b>Адрес для отправки через Белпочту</b>\n\n<pre>" + "\n".join(_postal_address_lines(case)) + "</pre>"


def build_claim_text(case: dict, creditor: dict) -> str:
    service_name = _sms_service(case)
    debt_calc = (
        f"Сумма займа: {_money(case.get('amount'))} | "
        f"Проценты: {_money(case.get('accrued_percent'))} | "
        f"Пени за просрочку: {_money(case.get('fine_outstanding'))}\n"
        f"ИТОГО к оплате: {_money(case.get('total_due'))} на дату отправки данного предложения."
    )
    sections = [
        (
            f"Направляю настоящее письмо-предложение по договору займа {_loan_ref(case)} "
            f"от {_date(case.get('issued_at'))} через сервис {service_name}, "
            "с целью урегулирования задолженности в добровольном досудебном порядке."
        ),
        (
            "Убедительно прошу Вас погасить задолженность в полном объеме в кратчайший срок. "
            "Это позволит избежать судебного разбирательства и дополнительных расходов."
        ),
        (
            "При обращении в суд сумма требований увеличится за счет дальнейшего начисления процентов, "
            "пени, государственной пошлины и иных расходов. После вынесения решения задолженность может "
            "взыскиваться принудительно, включая арест имущества и другие меры исполнения."
        ),
        "Готов урегулировать вопрос добровольно без обращения в суд.",
        debt_calc,
    ]
    return "\n\n".join(part for part in sections if part).strip()


def _set_default_style(doc: Document) -> None:
    style = doc.styles["Normal"]
    style.font.name = "Times New Roman"
    style.font.size = Pt(10)
    section = doc.sections[0]
    section.top_margin = Mm(8)
    section.bottom_margin = Mm(8)
    section.left_margin = Mm(10)
    section.right_margin = Mm(10)


def _add_block(doc: Document, title: str, lines: list[str]) -> None:
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p.paragraph_format.space_after = Pt(1)
    run = p.add_run(title)
    run.bold = True
    run.font.size = Pt(10.5)
    for line in lines:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        p.paragraph_format.space_after = Pt(0)
        run = p.add_run(line)
        run.font.size = Pt(10)


def _add_body_paragraph(doc: Document, text: str) -> None:
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.first_line_indent = Mm(4)
    p.paragraph_format.space_after = Pt(2)
    run = p.add_run(text)
    run.font.size = Pt(10)


def render_claim_docx(case: dict, creditor: dict, signature_path: str) -> tuple[Path, str]:
    GENERATED_DOCS_DIR.mkdir(parents=True, exist_ok=True)
    case_dir = GENERATED_DOCS_DIR / f"chat_{case['chat_id']}" / f"case_{case['id']}"
    case_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = case_dir / f"claim_{case['service']}_{case['id']}_{timestamp}.docx"

    doc = Document()
    _set_default_style(doc)

    debtor_lines = _debtor_header_lines(case)
    creditor_lines = _build_contacts_block(
        creditor.get("full_name"),
        creditor.get("address"),
        creditor.get("phone"),
        creditor.get("email"),
    )

    _add_block(doc, "Кому:", debtor_lines)
    doc.add_paragraph().paragraph_format.space_after = Pt(1)
    _add_block(doc, "От:", creditor_lines)
    doc.add_paragraph().paragraph_format.space_after = Pt(2)

    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title.paragraph_format.space_after = Pt(1)
    run = title.add_run("ПРЕДЛОЖЕНИЕ")
    run.bold = True
    run.font.size = Pt(11.5)

    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    subtitle.paragraph_format.space_after = Pt(4)
    run = subtitle.add_run("о добровольном урегулировании задолженности")
    run.bold = True
    run.font.size = Pt(10)

    for paragraph in build_claim_text(case, creditor).split("\n\n"):
        _add_body_paragraph(doc, paragraph)

    doc.add_paragraph().paragraph_format.space_after = Pt(2)
    footer = doc.add_table(rows=1, cols=2)
    footer.alignment = WD_TABLE_ALIGNMENT.CENTER
    footer.cell(0, 0).text = datetime.now().strftime("%d.%m.%Y")
    sign_cell = footer.cell(0, 1)
    sign_par = sign_cell.paragraphs[0]
    sign_par.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sign_run = sign_par.add_run()
    sign_run.add_picture(str(signature_path), width=Mm(25))
    sign_name = sign_cell.add_paragraph()
    sign_name.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sign_name.add_run(creditor.get("full_name") or "").font.size = Pt(10)

    doc.save(out_path)
    claim_text = build_claim_text(case, creditor)
    return out_path, claim_text


def serialize_case_payload(case: dict, creditor: dict | None = None) -> dict:
    return {
        "case": case,
        "creditor": creditor,
        "generated_at": datetime.now().isoformat(),
    }


def dump_payload(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
