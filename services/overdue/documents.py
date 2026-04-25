from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import logging
import re

from docx import Document
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT, WD_TABLE_ALIGNMENT
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


def _number(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _latest_claim(case: dict) -> dict:
    payload = _parse_case_raw(case)
    detail = payload.get("detail") or {}
    claims = detail.get("claims") or payload.get("claims") or []
    if not claims:
        return {}
    return sorted(
        (claim for claim in claims if isinstance(claim, dict)),
        key=lambda claim: (
            str(claim.get("claim_date") or ""),
            str(claim.get("sent_at") or ""),
            str(claim.get("id") or ""),
        ),
        reverse=True,
    )[0]


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


def _build_generic_claim_text(case: dict, creditor: dict) -> str:
    del creditor
    service_name = _sms_service(case)
    debt_calc = "\n".join([
        f"Ваша задолженность по состоянию на {_date(datetime.now().isoformat())} составляет:",
        f"- сумма займа: {_money(case.get('amount'))}",
        f"- проценты за пользование займом: {_money(case.get('accrued_percent'))}",
        f"- пени за просрочку: {_money(case.get('fine_outstanding'))}",
        f"- ИТОГО к оплате: {_money(case.get('total_due'))}",
    ])
    sections = [
        (
            f"Направляю настоящее письмо-предложение по договору займа {_loan_ref(case)} "
            f"от {_date(case.get('issued_at'))} через сервис {service_name}, "
            "с целью урегулирования задолженности в добровольном досудебном порядке."
        ),
        debt_calc,
        (
            "Прошу в кратчайший срок погасить задолженность в полном объеме. "
            "Это позволит урегулировать вопрос добровольно и без обращения в суд."
        ),
        (
            "Обращаю внимание, что при обращении в суд сумма требований будет увеличена, поскольку проценты "
            "и пени продолжают начисляться по день фактического исполнения обязательства."
        ),
        (
            "Кроме того, при судебном и последующем принудительном взыскании на должника будут возложены "
            "государственная пошлина, расходы на представителя и иные издержки, связанные с рассмотрением дела "
            "и принудительным исполнением."
        ),
        (
            "В пределах срока добровольного урегулирования готов рассмотреть конструктивные предложения "
            "по погашению задолженности."
        ),
        (
            "При отсутствии оплаты либо приемлемых предложений задолженность будет взыскана в судебном и "
            "последующем принудительном порядке."
        ),
    ]
    return "\n\n".join(part for part in sections if part).strip()


def _build_finkit_claim_text(case: dict, creditor: dict) -> str:
    del creditor
    claim = _latest_claim(case)
    snapshot_date = _date(claim.get("claim_date") or datetime.now().isoformat())
    deadline = _date(claim.get("expires_at"))
    amount_total = _money(_number(claim.get("amount")) or case.get("total_due"))
    debt_lines = "\n".join([
        f"- сумма невозвращенного в срок займа: {_money(case.get('principal_outstanding'))}",
        f"- проценты за пользование займом: {_money(case.get('accrued_percent'))}",
        f"- пени: {_money(case.get('fine_outstanding'))}",
        f"- ИТОГО: {amount_total}",
    ])
    consequences = "\n".join([
        "- возложение на вас судебных расходов по уплате государственной пошлины, юридической помощи и иных издержек;",
        "- дальнейшее увеличение суммы долга за счет начисления процентов и пеней по день фактической оплаты;",
        "- взыскание принудительного сбора и иных расходов исполнительного производства;",
        "- арест имущества, ограничение права на выезд и иные меры принудительного исполнения;",
        "- обращение взыскания на имущество, заработную плату и иные доходы;",
        "- ухудшение кредитной истории и сложности с получением новых займов и кредитов.",
    ])
    sections = [
        (
            f"По договору займа № {_loan_ref(case)} от {_date(case.get('issued_at'))} вам предоставлена сумма займа "
            f"в размере {_money(case.get('amount'))} с обязательством возврата займа и оплаты процентов за весь срок пользования."
        ),
        (
            f"Установленный договором срок возврата займа и оплаты процентов наступил {_date(case.get('due_at'))}, "
            "принятые по договору обязательства вами не исполнены."
        ),
        f"Ваша задолженность по договору по состоянию на {snapshot_date} составляет:\n{debt_lines}",
        (
            "На основании условий договора займа, пункта 22 Положения, утвержденного Указом Президента Республики Беларусь "
            "от 25.05.2021 № 196, а также статей 290, 311, 760 и 762 Гражданского кодекса Республики Беларусь требую "
            "добровольно погасить образовавшуюся задолженность."
        ),
        (
            f"Требую в кратчайший срок погасить задолженность по договору в общей сумме {amount_total}. "
            "Задолженность подлежит погашению в сервисе онлайн-заимствования https://finkit.by платежом банковской "
            "платежной картой, привязанной к вашему личному кабинету."
        ),
        (
            "Направляю настоящую претензию в дополнение к уведомлениям, ранее доступным через сервис FinKit, "
            "с предложением урегулировать вопрос в досудебном порядке и без обращения в суд."
        ),
        (
            "Обращаю внимание, что при обращении в суд сумма требований будет увеличена, поскольку проценты и пени "
            "продолжают начисляться по день фактического исполнения обязательства. Соответственно, на дату подачи искового "
            "заявления размер задолженности будет выше, чем на дату направления настоящей претензии."
        ),
        (
            "Кроме того, при судебном и последующем принудительном взыскании на должника будут возложены дополнительные "
            "расходы, связанные с рассмотрением дела судом и принудительным исполнением."
        ),
        (
            "В случае дальнейшего уклонения от оплаты задолженность будет взыскана в судебном и последующем "
            f"принудительном порядке, что повлечет для вас следующие последствия:\n{consequences}"
        ),
        (
            f"До {deadline} готов рассмотреть конструктивные предложения по добровольному урегулированию вопроса и погашению задолженности."
            if deadline != "—"
            else "Готов рассмотреть конструктивные предложения по добровольному урегулированию вопроса и погашению задолженности."
        ),
        (
            "При добровольном и полном погашении долга в пределах срока, указанного в претензии, возможно обсуждение "
            "уменьшения отдельных начислений."
        ),
        (
            "По истечении указанного срока при отсутствии оплаты либо приемлемых предложений задолженность будет взыскана "
            "в судебном и последующем принудительном порядке с увеличением суммы требований и взысканием дополнительных расходов."
        ),
        (
            "Настоятельно рекомендую воспользоваться возможностью добровольного урегулирования, поскольку дальнейшее промедление "
            "повлечет увеличение суммы задолженности и дополнительных расходов, связанных с ее взысканием."
        ),
    ]
    return "\n\n".join(part for part in sections if part).strip()


def build_claim_text(case: dict, creditor: dict) -> str:
    if case.get("service") == "finkit":
        return _build_finkit_claim_text(case, creditor)
    return _build_generic_claim_text(case, creditor)


def _claim_titles(case: dict) -> tuple[str, str]:
    if case.get("service") == "finkit":
        return "ПРЕТЕНЗИЯ", "с предложением о добровольном урегулировании задолженности"
    return "ПРЕДЛОЖЕНИЕ", "о добровольном урегулировании задолженности"


def _set_default_style(doc: Document) -> None:
    style = doc.styles["Normal"]
    style.font.name = "Times New Roman"
    style.font.size = Pt(12)
    section = doc.sections[0]
    section.top_margin = Mm(18)
    section.bottom_margin = Mm(18)
    section.left_margin = Mm(20)
    section.right_margin = Mm(15)


def _add_block(doc: Document, title: str, lines: list[str]) -> None:
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p.paragraph_format.space_after = Pt(3)
    run = p.add_run(title)
    run.bold = True
    run.font.size = Pt(12)
    for line in lines:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        p.paragraph_format.space_after = Pt(1)
        run = p.add_run(line)
        run.font.size = Pt(12)


def _add_body_paragraph(doc: Document, text: str) -> None:
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.first_line_indent = Mm(8)
    p.paragraph_format.space_after = Pt(5)
    p.paragraph_format.line_spacing = 1.15
    run = p.add_run(text)
    run.font.size = Pt(12)


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
    doc.add_paragraph().paragraph_format.space_after = Pt(3)
    _add_block(doc, "От:", creditor_lines)
    doc.add_paragraph().paragraph_format.space_after = Pt(6)

    claim_title, claim_subtitle = _claim_titles(case)
    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title.paragraph_format.space_after = Pt(2)
    run = title.add_run(claim_title)
    run.bold = True
    run.font.size = Pt(14)

    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    subtitle.paragraph_format.space_after = Pt(2)
    run = subtitle.add_run(claim_subtitle)
    run.bold = True
    run.font.size = Pt(12)

    subject = doc.add_paragraph()
    subject.alignment = WD_ALIGN_PARAGRAPH.CENTER
    subject.paragraph_format.space_after = Pt(8)
    run = subject.add_run(f"по договору займа № {_loan_ref(case)} от {_date(case.get('issued_at'))}")
    run.bold = True
    run.font.size = Pt(12)

    for paragraph in build_claim_text(case, creditor).split("\n\n"):
        _add_body_paragraph(doc, paragraph)

    doc.add_paragraph().paragraph_format.space_after = Pt(6)
    footer = doc.add_table(rows=1, cols=3)
    footer.alignment = WD_TABLE_ALIGNMENT.CENTER
    footer.autofit = False
    left_cell, sign_cell, right_cell = footer.rows[0].cells
    left_cell.width = Mm(25)
    sign_cell.width = Mm(125)
    right_cell.width = Mm(25)
    for cell in (left_cell, sign_cell, right_cell):
        cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER

    left_par = left_cell.paragraphs[0]
    left_par.alignment = WD_ALIGN_PARAGRAPH.LEFT
    left_run = left_par.add_run(datetime.now().strftime("%d.%m.%Y"))
    left_run.font.size = Pt(12)

    sign_par = sign_cell.paragraphs[0]
    sign_par.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sign_run = sign_par.add_run()
    sign_run.add_picture(str(signature_path), width=Mm(25))

    right_par = right_cell.paragraphs[0]
    right_par.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    right_run = right_par.add_run(creditor.get("full_name") or "")
    right_run.font.size = Pt(12)

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
