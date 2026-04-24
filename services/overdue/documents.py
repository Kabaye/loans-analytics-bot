from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import logging

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
        missing.append("ФИО / название кредитора")
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
    recipient = case.get("full_name") or "Получатель не указан"
    address_line = _address_line(case.get("borrower_zip"), case.get("borrower_address"))
    details: list[str] = [recipient]
    if case.get("document_id"):
        details.append(f"ИН: {case['document_id']}")
    details.append(address_line)
    return "📮 <b>Адрес для отправки через Белпочту</b>\n\n<pre>" + "\n".join(details) + "</pre>"


def build_claim_text(case: dict, creditor: dict) -> str:
    service_url = _service_url(case)
    debtor_line = _join_non_empty([
        case.get("full_name"),
        f"ИН {case.get('document_id')}" if case.get("document_id") else None,
        case.get("borrower_address"),
    ])
    debt_calc = (
        f"Основной долг: {_money(case.get('principal_outstanding'))}\n"
        f"Проценты: {_money(case.get('accrued_percent'))}\n"
        f"Пеня: {_money(case.get('fine_outstanding'))}\n"
        f"Итого к оплате: {_money(case.get('total_due'))} на дату отправки документа."
    )
    sections = [
        "ПРЕДЛОЖЕНИЕ\nо добровольном урегулировании задолженности",
        (
            f"Я, {creditor.get('full_name')}, направляю настоящее письмо-предложение в адрес {debtor_line or 'заемщика'} "
            f"по договору займа {_loan_ref(case)} "
            f"от {_date(case.get('issued_at'))} через сервис онлайн-заимствования {service_url}, "
            "с целью урегулирования задолженности в добровольном досудебном порядке."
        ),
        debt_calc,
        (
            f"Убедительно прошу Вас погасить задолженность в полном объеме в течение "
            f"{_voluntary_term(case)} дней с даты получения настоящего документа."
        ),
        (
            "Обращаю Ваше внимание на то, что при передаче дела в суд размер требований "
            "будет увеличен за счет дальнейшего начисления процентов, пени и судебных расходов."
        ),
        (
            f"Для связи и добровольного урегулирования: {creditor.get('full_name')}, "
            f"{creditor.get('phone') or '—'}, {creditor.get('email') or '—'}."
        ),
    ]
    return "\n\n".join(part for part in sections if part).strip()


def _set_default_style(doc: Document) -> None:
    style = doc.styles["Normal"]
    style.font.name = "Times New Roman"
    style.font.size = Pt(12)


def _add_block(doc: Document, title: str, lines: list[str]) -> None:
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    run = p.add_run(title)
    run.bold = True
    run.font.size = Pt(13)
    for line in lines:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        run = p.add_run(line)
        run.font.size = Pt(12)


def _add_body_paragraph(doc: Document, text: str) -> None:
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.first_line_indent = Mm(10)
    p.paragraph_format.space_after = Pt(8)
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

    debtor_lines = _build_contacts_block(
        case.get("full_name"),
        _address_line(case.get("borrower_zip"), case.get("borrower_address")),
        case.get("borrower_phone"),
        case.get("borrower_email"),
    )
    creditor_lines = _build_contacts_block(
        creditor.get("full_name"),
        creditor.get("address"),
        creditor.get("phone"),
        creditor.get("email"),
    )

    _add_block(doc, "Кому:", debtor_lines)
    doc.add_paragraph()
    _add_block(doc, "От:", creditor_lines)
    doc.add_paragraph()

    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title.add_run("ПРЕДЛОЖЕНИЕ")
    run.bold = True
    run.font.size = Pt(14)

    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = subtitle.add_run("о добровольном урегулировании задолженности")
    run.bold = True
    run.font.size = Pt(12)

    for paragraph in build_claim_text(case, creditor).split("\n\n"):
        _add_body_paragraph(doc, paragraph)

    totals = doc.add_table(rows=2, cols=2)
    totals.alignment = WD_TABLE_ALIGNMENT.CENTER
    totals.cell(0, 0).text = "Структура долга"
    totals.cell(0, 1).text = (
        f"Сумма займа: {_money(case.get('amount'))}\n"
        f"Проценты: {_money(case.get('accrued_percent'))}\n"
        f"Пени: {_money(case.get('fine_outstanding'))}"
    )
    totals.cell(1, 0).text = "Итого к оплате"
    totals.cell(1, 1).text = _money(case.get("total_due"))

    doc.add_paragraph()
    footer = doc.add_table(rows=1, cols=3)
    footer.alignment = WD_TABLE_ALIGNMENT.CENTER
    footer.cell(0, 0).text = datetime.now().strftime("%d.%m.%Y")
    sign_par = footer.cell(0, 1).paragraphs[0]
    sign_par.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sign_run = sign_par.add_run()
    sign_run.add_picture(str(signature_path), width=Mm(35))
    footer.cell(0, 2).text = creditor.get("full_name") or ""

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
