from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from htpy import Element, h1, h2, h3, p, tbody, td, th, thead, tr
from htpy import table as htpy_table
from jinja2 import Environment, FileSystemLoader
from markupsafe import Markup

_id_counter = {"table": 0, "buttons": 0, "dropdown": 0}
_templates_dir: Path = Path(__file__).parent / "templates"


def _get_id(element_type: str) -> int:
    id_value = _id_counter[element_type]
    _id_counter[element_type] += 1
    return id_value


def _get_jinja_env() -> Environment:
    return Environment(loader=FileSystemLoader(_templates_dir), autoescape=False)


def _to_str(html) -> str:
    """Convert any HTML (including Markup) to plain string."""
    return "".join([str(html)])


def _generate_table_element(df: pd.DataFrame, table_id: int) -> Element:
    headers = list(df.columns)

    header_row = tr[[th(onclick=f"sortTable_{table_id}({i})")[f"{col} \u21d5"] for i, col in enumerate(headers)]]

    body_rows = [tr[[td[str(cell)] for cell in row]] for row in df.values]

    return htpy_table(id=f"sortableTable_{table_id}")[thead[header_row], tbody[body_rows]]


def table(pandas_table: pd.DataFrame) -> str:
    env = _get_jinja_env()
    template = env.get_template("table_div.html")

    table_id = _get_id("table")
    headers = list(pandas_table.columns)
    table_html = _to_str(_generate_table_element(pandas_table, table_id))

    return template.render(headers=headers, table_html=table_html, table_id=table_id)


def buttons(tabs: dict[str, str]) -> str:
    env = _get_jinja_env()
    template = env.get_template("buttons.html")

    buttons_id = _get_id("buttons")
    labels = list(tabs.keys())
    contents = list(tabs.values())

    return template.render(labels=labels, contents=contents, buttons_id=buttons_id)


def plot(fig: go.Figure) -> str:
    return fig.to_html(full_html=False, include_plotlyjs=True)


def heading(text: str, level: int = 1) -> str:
    if level == 1:
        return _to_str(h1[text])
    elif level == 2:
        return _to_str(h2[text])
    else:
        return _to_str(h3[text])


def paragraph(text: str) -> str:
    return _to_str(p[Markup(text)])


def cell(html_str: str) -> str:
    env = _get_jinja_env()
    template = env.get_template("cell.html")
    return template.render(content=html_str)


def image(src: str, alt: str = "") -> str:
    return f'<img src="{src}" alt="{alt}" style="width: 100%; max-width: 100%;">'


def dropdown(options: dict[str, str], default: str | None = None) -> str:
    env = _get_jinja_env()
    template = env.get_template("dropdown.html")

    dropdown_id = _get_id("dropdown")
    labels = list(options.keys())
    contents = list(options.values())

    default_index = 0
    if default is not None and default in labels:
        default_index = labels.index(default)

    return template.render(labels=labels, contents=contents, dropdown_id=dropdown_id, default_index=default_index)
