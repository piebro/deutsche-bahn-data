import os
import re
import uuid
from dataclasses import dataclass
from typing import Any, NamedTuple

import duckdb
import ipynbname
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from IPython.display import HTML, display

DEFAULT_LAYOUT = dict(
    margin=dict(l=55, r=55, b=55, t=55),
    font=dict(family="Times", size=15),
    title_x=0.5,
    paper_bgcolor="#f5f2f0",
    plot_bgcolor="#f5f2f0",
    xaxis=dict(tickcolor="black", linecolor="black", showgrid=True, gridcolor="darkgray", zerolinecolor="darkgray"),
    yaxis=dict(
        tickcolor="black",
        linecolor="black",
        showgrid=True,
        gridcolor="darkgray",
        zerolinecolor="darkgray",
        rangemode="tozero",
    ),
)

DEFAULT_MAP_LAYOUT = dict(
    images=[
        dict(source="../stats/background_map.png", xref="x", yref="y", x=0, y=180, sizex=360, sizey=180, layer="below")
    ],
    xaxis=dict(showgrid=False, visible=False, range=[0, 360]),
    yaxis=dict(showgrid=False, visible=False, scaleanchor="x", scaleratio=1, range=[0, 180]),
    coloraxis=dict(
        colorscale=[
            [0, "rgba(255,255,255,0)"],
            [0.00000001, "rgb(12,51,131)"],
            [1 / 1000, "rgb(10,136,186)"],
            [1 / 100, "rgb(242,211,56)"],
            [1 / 10, "rgb(242,143,56)"],
            [1, "rgb(217,30,30)"],
        ],
    ),
)


class TableConfig(NamedTuple):
    title: str
    query_or_df: str | pd.DataFrame
    x_axis_col: str
    y_axis_col: str
    value_col: str
    center_columns: tuple[str] = ()
    sum_col: str = None
    label: str = None


@dataclass
class FigureConfig:
    title: str
    x_col: str
    y_col: str
    z_col: str = None
    query_or_df: str | pd.DataFrame = None
    group_col: str = None
    label: str = None
    y_unit_hover_template: str = None
    plot_type: str = "scatter"
    trace_names: list[str] = None


def format_dataframe_for_display(df):
    """Format numeric columns in dataframe for better display."""
    df_display = df.copy()

    for col in df_display.columns:
        if df_display[col].dtype in ["int64", "float64", "int32", "float32"]:
            df_display[col] = df_display[col].map("{:,.0f}".format)

    return df_display


def execute_query(config):
    """Execute SQL query and process the data."""
    if isinstance(config.query_or_df, str):
        df = duckdb.sql(config.query_or_df).df()
    else:
        df = config.query_or_df

    if df.empty:
        raise ValueError("Query returned empty dataframe")
    return df


def get_pivot_table(config):
    if isinstance(config.query_or_df, str):
        df = duckdb.sql(config.query_or_df).df().fillna(0)
    else:
        df = config.query_or_df

    pivot_df = df.pivot_table(
        index=config.y_axis_col, columns=config.x_axis_col, values=config.value_col, fill_value=0
    ).reset_index()
    pivot_df.columns.name = None

    sort_column = pivot_df.columns[-1]
    if config.sum_col:
        sum_values = df.groupby(config.y_axis_col)[config.sum_col].first()
        pivot_df[config.sum_col] = pivot_df[config.y_axis_col].map(sum_values).fillna(0)
        sort_column = config.sum_col

    pivot_df = pivot_df.sort_values(sort_column, ascending=False)
    pivot_df.insert(0, "Rank", range(1, len(pivot_df) + 1))
    return pivot_df


def show_tables(table_configs, show_search=True):
    uid = str(uuid.uuid4())[:8]

    processed_tables = {config.title: (get_pivot_table(config), config.center_columns) for config in table_configs}

    # Build HTML
    html = f'<div class="tabs-container" id="tabs-{uid}"><div class="tab-buttons">'
    for i, (config, (df_display, center_columns)) in enumerate(zip(table_configs, processed_tables.values())):
        active = "active" if i == 0 else ""
        button_label = config.label if config.label else config.title
        html += f'<button id="btn-{uid}-{i}" class="tab-btn {active}" onclick="switchTab(\'{uid}\', {i})">{button_label}</button>'
    html += '</div><div class="tab-content">'

    for i, (name, (df_display, center_columns)) in enumerate(processed_tables.items()):
        display_style = "block" if i == 0 else "none"
        html += f'<div id="pane-{uid}-{i}" class="tab-pane" style="display: {display_style};"><div class="controls-container">'
        if show_search:
            html += f'<input type="text" id="search-{uid}-{i}" class="search-box" placeholder="Search in {name}..." onkeyup="updateTable(\'{uid}\', {i})">'
        else:
            html += "<div></div>"
        html += f"""
        <div class="pagination-controls">
            <select id="pagesize-{uid}-{i}" class="page-size-select" onchange="updateTable('{uid}', {i})">
                <option value="10" selected>10</option>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
            </select>
            <div class="pagination-nav">
                <button class="page-btn" onclick="changePage('{uid}', {i}, -1)" id="prev-{uid}-{i}">‹</button>
                <span id="pageinfo-{uid}-{i}" class="page-info"></span>
                <button class="page-btn" onclick="changePage('{uid}', {i}, 1)" id="next-{uid}-{i}">›</button>
            </div>
        </div>
        """  # noqa: RUF001
        html += '</div><div class="table-wrapper">'

        df_formatted = format_dataframe_for_display(df_display)
        table_html = df_formatted.to_html(index=False, classes="data-table", table_id=f"table-{uid}-{i}", escape=False)

        def make_sortable_header(match):
            header_content = match.group(1)
            return f'<th class="sortable sorting" onclick="sortTable(\'{uid}\', {i}, this)">{header_content}</th>'

        table_html = re.sub(r"<th>([^<]+)</th>", make_sortable_header, table_html)

        # Add inline CSS for centering specific columns
        center_css = ""
        if center_columns:
            for col_name in center_columns:
                if col_name in df_formatted.columns:
                    col_index = list(df_formatted.columns).index(col_name)
                    center_css += f"#table-{uid}-{i} td:nth-child({col_index + 1}) {{ text-align: center !important; }}"
        if center_css:
            table_html = f"<style>{center_css}</style>" + table_html

        html += table_html
        html += "</div></div>"

    html += "</div></div>"
    html += (
        f"<script>initializeTableInstance('{uid}', {len(processed_tables)}, 10, {str(show_search).lower()});</script>"
    )
    display(HTML(html))


def get_trace_names(df, group_col, x_col, y_col, type):
    if group_col is None:
        trace_names = [y_col]
    else:
        if type == "last":
            # Get the last highest value for each group to get order of the trace_names
            group_counts = df.groupby([x_col, group_col]).size().unstack(fill_value=0)
            complete_x_values = group_counts[group_counts.min(axis=1) >= 0].index
            last_data = df[df[x_col] == complete_x_values.max()]
            last_y_values = last_data.groupby(group_col)[y_col].max().sort_values(ascending=False)
            trace_names = last_y_values.index.tolist()
        elif type == "unique":
            trace_names = df[group_col].unique().tolist()
        elif type == "sum":
            # Sort by the sum of y values for each group
            sum_values = df.groupby(group_col)[y_col].sum().sort_values(ascending=False)
            trace_names = sum_values.index.tolist()
        else:
            raise ValueError(f"Invalid trace type: {type}")
    return trace_names


def add_traces_to_figure(fig, df, config, trace_names, is_first=False):
    """Add traces to figure for a given configuration."""
    y_unit_hover_template = config.y_unit_hover_template if config.y_unit_hover_template else f"{config.y_col}"
    for trace_name in trace_names:
        if len(trace_names) == 1:
            group_data = df
        else:
            group_data = df[df[config.group_col] == trace_name]

        if config.plot_type == "bar":
            fig.add_trace(
                go.Bar(
                    x=group_data[config.x_col],
                    y=group_data[config.y_col],
                    name=trace_name,
                    visible=is_first,
                    hovertemplate=f"{trace_name}" + f"<br>%{{x}}<br>%{{y:,}} {y_unit_hover_template}<extra></extra>",
                )
            )
        elif config.plot_type == "scatter":
            fig.add_trace(
                go.Scatter(
                    x=group_data[config.x_col],
                    y=group_data[config.y_col],
                    name=trace_name,
                    visible=is_first,
                    hovertemplate=f"{trace_name}" + f"<br>%{{x}}<br>%{{y:,}} {y_unit_hover_template}<extra></extra>",
                )
            )
        elif config.plot_type == "map":
            fig.add_trace(
                go.Histogram2d(
                    x=group_data[config.x_col],
                    y=group_data[config.y_col],
                    z=group_data[config.z_col],
                    histfunc="sum",
                    xbins=dict(start=0, end=360, size=1),
                    ybins=dict(start=0, end=180, size=1),
                    coloraxis="coloraxis",
                )
            )
        else:
            raise ValueError(f"Invalid plot type: {config.plot_type}")


def create_button_config(
    config: FigureConfig, trace_start_idx: int, trace_count: int, total_traces: int
) -> dict[str, Any]:
    """Create button configuration for updatemenus."""
    visibility = [False] * total_traces
    for i in range(trace_start_idx, trace_start_idx + trace_count):
        visibility[i] = True

    return {
        "label": config.label if config.label else config.title,
        "args": [
            {"visible": visibility},
            {"title.text": config.title, "yaxis.title.text": config.y_col, "xaxis.title.text": config.x_col},
        ],
        "method": "update",
    }


def get_figure(figure_configs):
    fig = go.Figure()
    total_number_of_traces = 0
    trace_names_list = []

    trace_names = None
    for i, config in enumerate(figure_configs):
        df = execute_query(config)

        if config.trace_names:
            trace_names = config.trace_names
        if trace_names is None:
            trace_names = get_trace_names(df, config.group_col, config.x_col, config.y_col, "sum")

        add_traces_to_figure(fig, df, config, trace_names, is_first=(i == 0))
        total_number_of_traces += len(trace_names)
        trace_names_list.append(trace_names)

    buttons = []
    current_trace_idx = 0
    for trace_names, config in zip(trace_names_list, figure_configs):
        button_config = create_button_config(config, current_trace_idx, len(trace_names), total_number_of_traces)
        buttons.append(button_config)
        current_trace_idx += len(trace_names)

    layout_config = {
        "title": buttons[0]["args"][1]["title.text"],
        "xaxis_title": buttons[0]["args"][1]["xaxis.title.text"],
        "yaxis_title": buttons[0]["args"][1]["yaxis.title.text"],
        **DEFAULT_LAYOUT,
        "barmode": "stack",
    }
    if figure_configs[0].plot_type == "map":
        layout_config.update(DEFAULT_MAP_LAYOUT)

    if len(buttons) > 1:
        layout_config["updatemenus"] = [{"type": "buttons", "buttons": buttons}]

    fig.update_layout(layout_config)
    return fig


def show_figure_with_dropdown(figure_configs):
    """
    Save each plotly figure as JSON file and return HTML with dropdown for lazy-loaded interactive selection.

    Args:
        figure_configs: List of FigureConfig objects
        save_folder_path: Path where to save the JSON files

    Returns:
        str: HTML code for notebook with dropdown selector that lazy loads figures
    """
    try:
        notebook_name = ipynbname.name()
    except:
        notebook_name = os.environ.get("NOTEBOOK_NAME", "unknown")
    save_folder_path = os.path.join("../notebooks/saved_figures", notebook_name)
    os.makedirs(save_folder_path, exist_ok=True)

    uid = str(uuid.uuid4())[:8]
    figure_data = []
    for i, config in enumerate(figure_configs):
        fig = get_figure([config])
        label = config.label if config.label else config.title
        filename = f"{label}.json".replace("#", "_")
        filepath = os.path.join(save_folder_path, filename)
        fig.write_json(filepath)
        figure_data.append((label, filename, i))

    selection_html = ""
    for i, (label, filename, index) in enumerate(figure_data):
        selected = "selected" if i == 0 else ""
        selection_html += f'<option value="{filename}" data-index="{index}" {selected}>{label}</option>'

    html = f"""
    <div id="figure-container-{uid}" class="figure-container">
        <div class="figure-selector">
            <select id="figure-select-{uid}" onchange="loadFigure(this.value, this.options[this.selectedIndex].text, '{save_folder_path}', '{uid}')">
                {selection_html}
            </select>
        </div>
        <div id="figure-display-{uid}" class="figure-display"></div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            const select = document.getElementById('figure-select-{uid}');
            if (select && select.options.length > 0) {{
                const firstOption = select.options[0];
                loadFigure(firstOption.value, firstOption.text, '{save_folder_path}', '{uid}');
            }}
        }});
        
        if (document.readyState !== 'loading') {{
            const select = document.getElementById('figure-select-{uid}');
            if (select && select.options.length > 0) {{
                const firstOption = select.options[0];
                loadFigure(firstOption.value, firstOption.text, '{save_folder_path}', '{uid}');
            }}
        }}
    </script>
    """
    display(HTML(html))


def show_figure(figure_configs, type="buttons"):
    if type == "buttons":
        fig = get_figure(figure_configs)
        display(fig)
    elif type == "dropdown":
        show_figure_with_dropdown(figure_configs)
    else:
        raise ValueError(f"Invalid figure type: {type}")


def init():
    pio.renderers.default = "plotly_mimetype"
    display(
        HTML("""
    <link rel="stylesheet" type="text/css" href="../notebooks/notebook.css">
    <script src="../notebooks/notebook.js"></script>
    <script src="https://cdn.plot.ly/plotly-3.0.1.min.js" charset="utf-8"></script>
    """)
    )
    duckdb.sql("SET TimeZone='UTC'")
