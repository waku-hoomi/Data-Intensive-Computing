"""Build the measured Week 3 evaluation Markdown and both submission PDFs."""
from __future__ import annotations

import html
import json
import statistics
from pathlib import Path

import reportlab
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    KeepTogether, ListFlowable, ListItem, PageBreak, Paragraph, SimpleDocTemplate,
    Spacer, Table, TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
REPORTS = ROOT / "reports"
ARTIFACTS = ROOT / "artifacts/week3"


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_run(kind: str) -> dict:
    candidates = []
    for path in ARTIFACTS.glob("*.json"):
        try:
            record = _json(path)
        except (ValueError, OSError):
            continue
        if record.get("kind") == kind and record.get("status") == "success":
            candidates.append((path.stat().st_mtime_ns, record))
    if not candidates:
        raise FileNotFoundError(f"No successful {kind} run under {ARTIFACTS}")
    return max(candidates, key=lambda item: item[0])[1]


def _fmt(value: float) -> str:
    return f"{value:.3f}"


def build_evaluation_markdown() -> str:
    from week3_evaluation_text import build_evaluation_markdown as build_text
    return build_text(ROOT)


FONT_DIR = Path(reportlab.__file__).parent / "fonts"
pdfmetrics.registerFont(TTFont("Body", str(FONT_DIR / "Vera.ttf")))
pdfmetrics.registerFont(TTFont("BodyBold", str(FONT_DIR / "VeraBd.ttf")))

NAVY = colors.HexColor("#18334D")
TEAL = colors.HexColor("#176B70")
LIGHT = colors.HexColor("#EDF3F5")
STYLES = {
    "title": ParagraphStyle("title", fontName="BodyBold", fontSize=20, leading=24,
                            textColor=NAVY, alignment=TA_CENTER, spaceAfter=12),
    "h2": ParagraphStyle("h2", fontName="BodyBold", fontSize=12.2, leading=15,
                         textColor=TEAL, spaceBefore=9, spaceAfter=5,
                         keepWithNext=True),
    "body": ParagraphStyle("body", fontName="Body", fontSize=10, leading=13.5,
                           textColor=colors.HexColor("#1D2830"), spaceAfter=5.5),
    "small": ParagraphStyle("small", fontName="Body", fontSize=7.5, leading=9.5),
    "cell": ParagraphStyle("cell", fontName="Body", fontSize=8, leading=10),
    "cellhead": ParagraphStyle("cellhead", fontName="BodyBold", fontSize=8,
                               leading=10, textColor=colors.white),
}


def _paragraph(text: str, style: str = "body") -> Paragraph:
    safe = html.escape(text).replace("`", "")
    return Paragraph(safe, STYLES[style])


def _parse_table(lines: list[str]) -> Table:
    rows = []
    for index, line in enumerate(lines):
        if index == 1:
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        rows.append([_paragraph(cell, "cellhead" if index == 0 else "cell") for cell in cells])
    page_width = A4[0] - 30 * mm
    widths = [page_width / len(rows[0])] * len(rows[0])
    table = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT, colors.white]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#C6D2D8")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def markdown_story(text: str):
    lines = text.splitlines()
    story = []
    index = 0
    paragraph = []

    def flush() -> None:
        if paragraph:
            story.append(_paragraph(" ".join(value.strip() for value in paragraph)))
            paragraph.clear()

    while index < len(lines):
        line = lines[index]
        if line.startswith("# "):
            flush()
            story.append(_paragraph(line[2:], "title"))
        elif line.startswith("## "):
            flush()
            story.append(_paragraph(line[3:], "h2"))
        elif line.strip() == "<!-- PAGEBREAK -->":
            flush()
            story.append(PageBreak())
        elif line.startswith("|") and index + 1 < len(lines) and lines[index + 1].startswith("| ---"):
            flush()
            table_lines = [line, lines[index + 1]]
            index += 2
            while index < len(lines) and lines[index].startswith("|"):
                table_lines.append(lines[index])
                index += 1
            story.append(KeepTogether([_parse_table(table_lines), Spacer(1, 5)]))
            continue
        elif line.startswith("- "):
            flush()
            items = []
            while index < len(lines) and lines[index].startswith("- "):
                items.append(ListItem(_paragraph(lines[index][2:]), leftIndent=8))
                index += 1
            story.append(ListFlowable(items, bulletType="bullet", leftIndent=14))
            continue
        elif not line.strip():
            flush()
        else:
            paragraph.append(line)
        index += 1
    flush()
    return story


def render(markdown_path: Path, output_path: Path, subtitle: str) -> None:
    story = markdown_story(markdown_path.read_text(encoding="utf-8"))

    def footer(canvas, document):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#CBD6DC"))
        canvas.line(15 * mm, 13 * mm, A4[0] - 15 * mm, 13 * mm)
        canvas.setFont("Body", 7)
        canvas.setFillColor(NAVY)
        canvas.drawString(15 * mm, 8.5 * mm, f"ID2221 | Week 3 | {subtitle}")
        canvas.drawRightString(A4[0] - 15 * mm, 8.5 * mm, str(document.page))
        canvas.restoreState()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    document = SimpleDocTemplate(
        str(output_path), pagesize=A4, leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=14 * mm, bottomMargin=17 * mm, title=subtitle,
        author="ID2221 group",
    )
    document.build(story, onFirstPage=footer, onLaterPages=footer)


def main() -> None:
    evaluation = build_evaluation_markdown()
    evaluation_path = DOCS / "Lab3_Evaluation_Report.md"
    evaluation_path.write_text(evaluation, encoding="utf-8")
    render(DOCS / "Lab3_Design_Report.md", REPORTS / "Lab3_Design_Report.pdf",
           "Design Report")
    render(evaluation_path, REPORTS / "Lab3_Evaluation_Report.pdf",
           "Evaluation Report")
    print(json.dumps({
        "design_pdf": str(REPORTS / "Lab3_Design_Report.pdf"),
        "evaluation_pdf": str(REPORTS / "Lab3_Evaluation_Report.pdf"),
    }, indent=2))


if __name__ == "__main__":
    main()
