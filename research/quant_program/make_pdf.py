"""Render the research documents to PDF (combined pack plus one file per document).

    python make_pdf.py                  # all documents
    python make_pdf.py DECISION_MEMO.md # just one

Reuses the minimal Markdown -> HTML converter from research/btc_confirmation_lag/make_pdf.py and prints with
headless Edge/Chrome (there is no pandoc or LaTeX on this machine). Images resolve relative to this folder.
"""

from __future__ import annotations

import base64
import html
import re
import subprocess
import time
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "btc_confirmation_lag"))
import make_pdf as MD  # noqa: E402  (converter: convert(), CSS, BROWSERS)

OUT = HERE / "results" / "pdf"
DOCS = ["EXECUTIVE_SUMMARY.md", "DECISION_MEMO.md", "TREND_CONFIRMATION_RESEARCH.md",
        "STRUCTURAL_ALPHA_RESULTS.md", "REVIEW_RESPONSE.md", "RESEARCH_BACKLOG.md"]
EXTRA_CSS = """
h1 { break-before: page; padding-top: 2px; }
h1:first-of-type { break-before: auto; }
.toc { font-size: 9.5pt; margin: 10px 0 0 0; color: #333; }
.cover { margin: 0 0 10px; }
.cover .t { font-size: 22pt; color: #0d2a4d; font-weight: 600; }
.cover .s { font-size: 10.5pt; color: #555; margin-top: 4px; }
"""


def image(line: str) -> str:
    """As upstream, but resolve image paths against quant_program/ and tolerate missing figures."""
    m = re.match(r"!\[([^\]]*)\]\(([^)]+)\)", line.strip())
    p = (HERE / m.group(2)).resolve()
    if not p.exists():
        return f"<p><i>[missing figure: {html.escape(m.group(2))}]</i></p>"
    data = base64.b64encode(p.read_bytes()).decode()
    return f'<img alt="{html.escape(m.group(1))}" src="data:image/png;base64,{data}">'


MD.image = image      # convert() resolves the helper as a module global


def to_pdf(body_html: str, title: str, out_pdf: Path) -> None:
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_pdf.with_suffix(".html")
    tmp.write_text('<!doctype html><html><head><meta charset="utf-8"><title>' + html.escape(title) +
                   "</title><style>" + MD.CSS + EXTRA_CSS + "</style></head><body>" + body_html + "</body></html>",
                   encoding="utf-8")
    browser = next(b for b in MD.BROWSERS if Path(b).exists())
    before = out_pdf.stat().st_mtime if out_pdf.exists() else 0
    subprocess.run([browser, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    "--print-to-pdf=" + str(out_pdf), tmp.as_uri()], check=True, timeout=300, capture_output=True)
    # Edge returns before the PDF is flushed to disk, so wait for a newer file to appear.
    for _ in range(600):
        if out_pdf.exists() and out_pdf.stat().st_mtime > before and out_pdf.stat().st_size > 0:
            break
        time.sleep(0.25)
    else:
        raise RuntimeError(f"browser did not write {out_pdf}")
    time.sleep(0.5)               # let the last bytes land before reporting the size
    tmp.unlink(missing_ok=True)
    print("wrote", out_pdf.relative_to(HERE), f"{out_pdf.stat().st_size / 1024:,.0f} KB")


def main(names: list[str]) -> None:
    docs = names or DOCS
    parts = []
    for name in docs:
        src = HERE / name
        if not src.exists():
            print("skip (missing):", name)
            continue
        body = MD.convert(src.read_text(encoding="utf-8"))
        parts.append(body)
        to_pdf(body, src.stem, OUT / (src.stem + ".pdf"))
    if len(parts) > 1:
        titles = " &middot; ".join(html.escape(Path(d).stem.replace("_", " ").title()) for d in docs)
        cover = ('<div class="cover"><div class="t">Crypto Quant Research Pack</div>'
                 '<div class="s">Trend confirmation (Track A) and structural alpha (Track B) &middot; 2026-09-16'
                 ' &middot; research only, not investment advice</div></div>'
                 '<div class="toc"><b>Contents:</b> ' + titles + "</div>")
        to_pdf(cover + "".join(parts), "Crypto Quant Research Pack", OUT / "QUANT_RESEARCH_PACK.pdf")


if __name__ == "__main__":
    main(sys.argv[1:])
