#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, pathlib
from playwright.sync_api import sync_playwright

def html_to_pdf(html_path: str):
    p = pathlib.Path(html_path).resolve()
    pdf = p.with_suffix(".pdf")
    with sync_playwright() as sp:
        browser = sp.chromium.launch()
        page = browser.new_page()
        page.goto(p.as_uri(), wait_until="networkidle")
        page.pdf(path=str(pdf), format="A4", print_background=True,
                 margin={"top":"16mm","bottom":"16mm","left":"14mm","right":"14mm"},
                 display_header_footer=True,
                 header_template="<div></div>",
                 footer_template=(
                     "<div style='width:100%;font-size:10px;color:#475569;"
                     "padding:6px 10px;text-align:right;'>"
                     "Página <span class='pageNumber'></span>/<span class='totalPages'></span></div>"
                 ))
        browser.close()
    print(f"PDF: {pdf}")

if __name__ == "__main__":
    html_to_pdf(sys.argv[1])

