import argparse
import re
from pathlib import Path

import nbformat
from jinja2 import Environment, FileSystemLoader
from nbconvert.preprocessors import ExecutePreprocessor

PLOTLY_INLINE_PATTERN = re.compile(
    r'<script type="text/javascript">window\.PlotlyConfig = \{MathJaxConfig: \'local\'\};</script>'
    r'\s*<script[^>]*>.*?"use strict";var Plotly=.*?</script>',
    re.DOTALL,
)

DEFAULT_PAGES = [
    {"name": "allgemein", "filename": "allgemein", "display_name": "Allgemein"},
    {"name": "zeitraum", "filename": "zeitraum", "display_name": "Zeitraum"},
    {"name": "zugverbindung", "filename": "zugverbindung", "display_name": "Zugverbindung"},
    {"name": "verspaetungsverlauf", "filename": "verspaetungsverlauf", "display_name": "Verspätungsverlauf"},
    {
        "name": "verspaetung_pro_bahnhof",
        "filename": "verspaetung_pro_bahnhof",
        "display_name": "Verspätung pro Bahnhof",
    },
    {
        "name": "zuggattungen_pro_bahnhof",
        "filename": "zuggattungen_pro_bahnhof",
        "display_name": "Zuggattungen pro Bahnhof",
    },
    {"name": "zuggattung", "filename": "zuggattung", "display_name": "Zuggattung"},
    {"name": "bahnhof", "filename": "bahnhof", "display_name": "Bahnhof"},
]


def strip_inline_plotly(content: str) -> str:
    """Remove inline Plotly.js library from HTML content."""
    return PLOTLY_INLINE_PATTERN.sub("", content)


def run_notebook(notebook_path: str) -> nbformat.NotebookNode:
    """Execute a notebook and return the executed notebook object."""
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600)
    ep.preprocess(nb, {"metadata": {"path": str(Path(notebook_path).parent)}})
    return nb


def extract_html_outputs(notebook_path: str, run: bool = False) -> list[str]:
    """Extract all HTML outputs from a notebook."""
    if run:
        nb = run_notebook(notebook_path)
        with open(notebook_path, "w") as f:
            nbformat.write(nb, f)
    else:
        with open(notebook_path) as f:
            nb = nbformat.read(f, as_version=4)

    html_outputs = []
    for cell in nb.cells:
        if cell.cell_type == "code" and "outputs" in cell:
            for output in cell.outputs:
                if output.output_type == "display_data" and "text/html" in output.data:
                    html_outputs.append(output.data["text/html"])
    return html_outputs


def convert_notebook_to_html(
    input_path: str,
    output_path: str,
    title: str = "Notebook Output",
    plotly_url: str | None = None,
    run: bool = False,
    current_page: str | None = None,
) -> None:
    """Convert a notebook to HTML by extracting HTML outputs and wrapping in page template."""
    html_outputs = extract_html_outputs(input_path, run=run)
    content = "\n".join(html_outputs)

    if plotly_url:
        content = strip_inline_plotly(content)

    templates_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(templates_dir), autoescape=False)
    template = env.get_template("page.html")

    html = template.render(
        title=title,
        content=content,
        plotly_url=plotly_url,
        pages=DEFAULT_PAGES,
        current_page=current_page,
    )

    with open(output_path, "w") as f:
        f.write(html)


NOTEBOOKS_DIR = Path(__file__).parent.parent
OUTPUT_DIR = Path(__file__).parent.parent.parent / "stats"


def convert_notebooks(
    pages: list[dict],
    plotly_url: str | None = None,
    run_pages: list[str] | None = None,
) -> None:
    """Convert notebooks to HTML.

    Args:
        pages: List of page definitions to convert.
        plotly_url: External Plotly.js URL (strips inline Plotly and loads from URL).
        run_pages: List of page names to execute before extracting. None means don't run any.
    """
    for page in pages:
        run = run_pages is not None and page["name"] in run_pages
        if run:
            print(f"Running {page['filename']}.ipynb...")
        input_path = NOTEBOOKS_DIR / f"{page['filename']}.ipynb"
        output_path = OUTPUT_DIR / f"{page['filename']}.html"
        convert_notebook_to_html(
            str(input_path),
            str(output_path),
            title=page["display_name"],
            plotly_url=plotly_url,
            run=run,
            current_page=page["name"],
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert notebook HTML outputs to HTML files")
    parser.add_argument("--plotly-url", help="External Plotly.js URL (strips inline Plotly and loads from URL)")
    parser.add_argument(
        "--run",
        nargs="?",
        const="all",
        default=None,
        metavar="NOTEBOOK",
        help="Execute notebook(s) before extracting outputs. Without argument: run all. With argument: run specific notebook (e.g., 'allgemein')",
    )

    args = parser.parse_args()

    run_pages = None
    if args.run is not None:
        if args.run == "all":
            run_pages = [p["name"] for p in DEFAULT_PAGES]
        else:
            valid_names = [p["name"] for p in DEFAULT_PAGES]
            if args.run not in valid_names:
                parser.error(f"Unknown notebook '{args.run}'. Valid options: {valid_names}")
            run_pages = [args.run]

    convert_notebooks(DEFAULT_PAGES, args.plotly_url, run_pages)
