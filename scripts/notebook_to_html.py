import argparse
import json
import os
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

# Set up Jinja2 environment
template_dir = os.path.join(os.path.dirname(__file__), "html_templates")
jinja_env = Environment(loader=FileSystemLoader(template_dir))


def build_pages_list(all_notebooks):
    """Build a list of page objects from notebook paths, including the interactive page"""
    pages = []

    if not all_notebooks:
        return pages

    # Add all notebook pages
    for nb_path in sorted(all_notebooks):
        nb_name = Path(nb_path).stem
        display_name = nb_name.replace("_", " ").title()

        pages.append({"name": nb_name, "filename": nb_name, "display_name": display_name})

    # Add interactive page as the last page
    pages.append({"name": "interactive", "filename": "interactive", "display_name": "Interactive Query"})

    return pages


def convert_notebook_to_html(notebook_path: str, all_notebooks=None):
    """Convert a Jupyter notebook to a basic HTML page"""

    with open(notebook_path, encoding="utf-8") as f:
        notebook = json.load(f)

    notebook_name = Path(notebook_path).stem
    html_content = generate_html(notebook, notebook_name, all_notebooks)
    output_path = os.path.join("stats", f"{notebook_name}.html")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Converted {notebook_path} to {output_path}")
    return output_path


def extract_code_label(source_text, is_first_code_cell=False):
    """Extract label for code block"""
    if is_first_code_cell:
        return "Imports and Default Layout"
    return "code"


def extract_title_from_notebook(notebook):
    """Extract title from the first markdown cell"""
    cells = notebook.get("cells", [])

    for cell in cells:
        if cell.get("cell_type") == "markdown":
            source = cell.get("source", [])
            if isinstance(source, list):
                source_text = "".join(source)
            else:
                source_text = str(source)

            # Look for the first header
            lines = source_text.strip().split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith("# "):
                    return line[2:].strip()

            # If no header found, use first non-empty line
            for line in lines:
                line = line.strip()
                if line:
                    return line

    return "Converted Notebook"


def generate_html(notebook, notebook_name="", all_notebooks=None):
    """Generate basic HTML from notebook content using Jinja2 templates"""

    # Extract title from notebook
    title = extract_title_from_notebook(notebook)

    # Build pages list
    pages = build_pages_list(all_notebooks)

    # Process notebook cells to generate content
    content_parts = []
    first_code_cell = True
    cell_index = 0

    # Process each cell
    for cell in notebook.get("cells", []):
        cell_type = cell.get("cell_type", "")
        source = cell.get("source", [])

        # Join source lines
        if isinstance(source, list):
            source_text = "".join(source)
        else:
            source_text = str(source)

        if cell_type == "markdown":
            # Simple markdown processing (just convert headers)
            processed_content = process_simple_markdown(source_text)
            content_parts.append(f'<div class="cell markdown-cell">{processed_content}</div>')

        elif cell_type == "code":
            # Skip empty code cells
            if not source_text.strip():
                continue

            # Skip the first code cell (Imports and Default Layout)
            if first_code_cell:
                first_code_cell = False
                continue

            # Extract label for the code block
            code_label = extract_code_label(source_text, first_code_cell)
            code_id = f"code-{cell_index}"

            code_html = f'''<div class="code">
                <div class="code-toggle" onclick="toggleCode(this)">{escape_html(code_label)}</div>
                <div class="code-content" id="{code_id}">
                    <pre><code class="language-python">{escape_html(source_text)}</code></pre>
                </div>
            </div>'''

            # Add outputs if they exist
            outputs = cell.get("outputs", [])
            output_html = ""
            if outputs:
                for output_index, output in enumerate(outputs):
                    output_html += process_output(output, cell_index, output_index)

            content_parts.append(f'<div class="cell code-cell">{code_html}{output_html}</div>')
            cell_index += 1

    # Render the HTML using Jinja2 template
    template = jinja_env.get_template("base.html")
    html_content = template.render(
        title=title, pages=pages, current_page=notebook_name, content="\n".join(content_parts)
    )

    return html_content


def process_output(output, cell_index, output_index):
    """Process notebook cell output and return HTML"""
    output_html = ""

    # Handle different output types
    if output.get("output_type") == "display_data" and "data" in output:
        data = output["data"]

        # Handle Plotly plots
        if "application/vnd.plotly.v1+json" in data:
            plotly_data = data["application/vnd.plotly.v1+json"]
            plot_id = f"plotly-div-{cell_index}-{output_index}"

            # Extract the actual plot data and layout
            plot_data = plotly_data.get("data", [])
            plot_layout = plotly_data.get("layout", {})
            plot_config = plotly_data.get("config", {})

            # Create a div for the plot and JavaScript to render it
            # Configure for responsive behavior
            plot_config = plot_config or {}
            plot_config.update({"responsive": True, "displayModeBar": True, "displaylogo": False})

            output_html += f'''
<div id="{plot_id}" class="plotly-output"></div>
<script>
    Plotly.newPlot('{plot_id}', {json.dumps(plot_data)}, {json.dumps(plot_layout)}, {json.dumps(plot_config)});
</script>
'''

        # Handle HTML tables (pandas DataFrames)
        elif "text/html" in data:
            html_data = data["text/html"]
            if isinstance(html_data, list):
                html_data = "".join(html_data)

            # Check if it's a pandas DataFrame table
            if "<table" in html_data and "dataframe" in html_data:
                table_id = f"table-{cell_index}-{output_index}"
                output_html += f'<div class="table-output" id="{table_id}">{html_data}</div>'
                output_html += f"""
<script>
    // Format numbers in table with commas
    document.addEventListener('DOMContentLoaded', function() {{
        const table = document.querySelector('#{table_id} table');
        if (table) {{
            const cells = table.querySelectorAll('td:not(:first-child)');
            cells.forEach(cell => {{
                const text = cell.textContent.trim();
                if (/^\\d+$/.test(text)) {{
                    const number = parseInt(text);
                    cell.textContent = number.toLocaleString();
                }}
            }});
        }}
    }});
</script>
"""
            else:
                output_html += f'<div class="output">{html_data}</div>'

    # Handle execute_result outputs (like DataFrame displays)
    elif output.get("output_type") == "execute_result" and "data" in output:
        print("execute_result")
        data = output["data"]

        # Handle HTML tables (pandas DataFrames)
        if "text/html" in data:
            html_data = data["text/html"]
            if isinstance(html_data, list):
                html_data = "".join(html_data)

            # Check if it's a pandas DataFrame table
            if "<table" in html_data and "dataframe" in html_data:
                table_id = f"table-{cell_index}-{output_index}"
                output_html += f'<div class="table-output" id="{table_id}">{html_data}</div>'
                output_html += f"""
<script>
    // Format numbers in table with commas
    document.addEventListener('DOMContentLoaded', function() {{
        const table = document.querySelector('#{table_id} table');
        if (table) {{
            const cells = table.querySelectorAll('td:not(:first-child)');
            cells.forEach(cell => {{
                const text = cell.textContent.trim();
                if (/^\\d+$/.test(text)) {{
                    const number = parseInt(text);
                    cell.textContent = number.toLocaleString();
                }}
            }});
        }}
    }});
</script>
"""
            else:
                output_html += f'<div class="output">{html_data}</div>'

        # Handle text output as fallback
        elif "text/plain" in data:
            text_data = data["text/plain"]
            if isinstance(text_data, list):
                text_data = "".join(text_data)

            # Skip widget-related text outputs
            if not ("Progress(" in text_data or "Widget(" in text_data or "Layout(" in text_data):
                output_html += f'<div class="output">{escape_html(text_data)}</div>'

    # Handle text output from stream
    elif "text" in output:
        print("text")
        output_text = "".join(output["text"]) if isinstance(output["text"], list) else output["text"]
        output_html += f'<div class="output">{escape_html(output_text)}</div>'

    return output_html


def process_simple_markdown(text):
    """Basic markdown processing for headers and links"""
    lines = text.split("\n")
    processed_lines = []

    for line in lines:
        line = line.strip()
        if line.startswith("# "):
            processed_lines.append(f"<h1>{process_markdown_links(line[2:])}</h1>")
        elif line.startswith("## "):
            processed_lines.append(f"<h2>{process_markdown_links(line[3:])}</h2>")
        elif line.startswith("### "):
            processed_lines.append(f"<h3>{process_markdown_links(line[4:])}</h3>")
        elif line:
            processed_lines.append(f"<p>{process_markdown_links(line)}</p>")

    return "\n".join(processed_lines)


def process_markdown_links(text):
    """Convert markdown links to HTML links"""
    import re

    # Pattern to match markdown links: [text](url)
    link_pattern = r"\[([^\]]+)\]\(([^)]+)\)"

    def replace_link(match):
        link_text = escape_html(match.group(1))
        link_url = escape_html(match.group(2))
        return f'<a href="{link_url}">{link_text}</a>'

    # Replace markdown links with HTML links
    result = re.sub(link_pattern, replace_link, text)

    # Escape remaining HTML characters (but not the links we just created)
    result = escape_html_except_links(result)

    return result


def escape_html_except_links(text):
    """Escape HTML characters but preserve link tags"""
    import re

    # Find all link tags to preserve them
    link_pattern = r'<a href="[^"]*">[^<]*</a>'
    links = re.findall(link_pattern, text)

    # Replace links with placeholders
    placeholder_text = text
    for i, link in enumerate(links):
        placeholder = f"__LINK_PLACEHOLDER_{i}__"
        placeholder_text = placeholder_text.replace(link, placeholder, 1)

    # Escape HTML in the text with placeholders
    escaped_text = escape_html(placeholder_text)

    # Restore the links
    for i, link in enumerate(links):
        placeholder = f"__LINK_PLACEHOLDER_{i}__"
        escaped_text = escaped_text.replace(placeholder, link)

    return escaped_text


def escape_html(text):
    """Escape HTML special characters"""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def generate_interactive_html(all_notebooks):
    """Generate the interactive query page from template"""
    # Build pages list
    pages = build_pages_list(all_notebooks)

    # Load LLM hints content
    llm_hints_path = os.path.join(template_dir, "duckdb_wasm_llm_hints.md")
    with open(llm_hints_path, encoding="utf-8") as f:
        llm_hints_content = f.read()

    # Load and render the interactive template
    template = jinja_env.get_template("interactive.html")
    html_content = template.render(
        title="Interactive Data Query Explorer",
        pages=pages,
        current_page="interactive",
        llm_hints=llm_hints_content,
    )

    # Write the output file
    output_path = os.path.join("stats", "interactive.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Generated interactive page at {output_path}")
    return output_path


def generate_index_html(output_dir: str = "stats"):
    """Generate an index.html file listing all available statistics"""

    # Find all HTML files in the output directory (excluding index.html)
    html_files = []
    if os.path.exists(output_dir):
        for file in os.listdir(output_dir):
            if file.endswith(".html") and file != "index.html":
                html_files.append(file)

    # Sort files alphabetically
    html_files.sort()

    # Generate navigation links
    nav_links = []
    for html_file in html_files:
        # Create a display name from filename
        display_name = html_file.replace(".html", "").replace("_", " ").title()
        nav_links.append(f'            <li><a href="{html_file}">{display_name}</a></li>')

    # Generate header HTML
    header_html = """<header class='header'>
        <nav>
            <a href="https://github.com/piebro/openstreetmap-statistics/">GitHub</a>
            <a href="https://github.com/piebro/openstreetmap-statistics/blob/master/README.md">About</a>
        </nav>
    </header>"""

    # Generate footer HTML
    footer_html = """<footer class='footer'>
        <p>&copy; 2025 OpenStreetMap Statistics. Data from OpenStreetMap contributors.</p>
        <p><a href="https://github.com/piebro/openstreetmap-statistics/">View on GitHub</a></p>
    </footer>"""

    # Generate index content
    index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=1024, initial-scale=1.0, user-scalable=yes">
    <title>OpenStreetMap Statistics</title>
    <link rel="stylesheet" href="notebook_styles.css">
    <script defer data-domain="piebro.github.io/openstreetmap-statistics" src="https://plausible.io/js/script.js"></script>
</head>
<body>
    {header_html}
    
    <div class="cell markdown-cell">
        <h1>OpenStreetMap Statistics</h1>
        <p>Explore various statistics and analyses of OpenStreetMap data.</p>
    </div>
    
    <div class="cell markdown-cell">
        <h2>Available Statistics</h2>
        <ul>
{chr(10).join(nav_links)}
        </ul>
    </div>
    
    {footer_html}
</body>
</html>"""

    # Write index.html file
    index_path = os.path.join(output_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(index_html)

    print(f"Generated index file at {index_path}")
    return index_path


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description="Convert Jupyter notebooks to HTML")
    parser.add_argument("--notebook", help="Path to a specific Jupyter notebook file (optional)")
    parser.add_argument(
        "--directory", "-d", default="notebooks", help="Directory to search for notebooks (default: notebooks)"
    )

    args = parser.parse_args()

    # If a specific notebook is provided, convert only that one
    if args.notebook:
        if not os.path.exists(args.notebook):
            print(f"Error: Notebook file '{args.notebook}' not found")
            return 1

        try:
            convert_notebook_to_html(args.notebook)
            return 0
        except Exception as e:
            print(f"Error converting notebook: {e}")
            return 1

    # Otherwise, find and convert all notebooks
    if not os.path.exists(args.directory):
        print(f"Error: Directory '{args.directory}' not found")
        return 1

    # Find all .ipynb files in the directory and subdirectories
    notebook_files = []
    for root, dirs, files in os.walk(args.directory):
        for file in files:
            if file.endswith(".ipynb"):
                notebook_files.append(os.path.join(root, file))

    if not notebook_files:
        print(f"No notebook files found in '{args.directory}'")
        return 0

    print(f"Found {len(notebook_files)} notebook file(s)")

    # Convert each notebook
    for notebook_path in sorted(notebook_files):
        try:
            convert_notebook_to_html(notebook_path, notebook_files)
        except Exception as e:
            print(f"Error converting {notebook_path}: {e}")

    # Generate interactive page
    try:
        generate_interactive_html(notebook_files)
    except Exception as e:
        print(f"Error generating interactive page: {e}")

    return 0


if __name__ == "__main__":
    exit(main())
