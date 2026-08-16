# ruff: noqa: EXE001, EXE005, RUF100
#!/usr/bin/env python3
"""Convert XML-like notebook files to standard Jupyter notebook JSON format."""

import json
import os
import re
from pathlib import Path

try:
    import nbformat
except ImportError:
    print(
        "ERROR: nbformat is required. Install with: pip install nbformat",
    )  # noqa: PLR1722, EXE001, RUF100
    exit(1)  # noqa: PLR1722, RUF100


def parse_notebook_file(file_path: str) -> list[dict]:
    """Parse XML-like notebook file and extract cells."""
    with open(file_path, encoding="utf-8") as f:
        content = f.read()

    cells = []

    markdown_pattern = r"<markdown_cell>(.*?)</markdown_cell>"
    code_pattern = r"<code_cell>(.*?)</code_cell>"

    for match in re.finditer(markdown_pattern, content, re.DOTALL):
        source = match.group(1)
        lines = source.split("\n")
        result_lines = []
        for i, line in enumerate(lines):
            if i < len(lines) - 1:
                result_lines.append(line + "\n")
            else:
                result_lines.append(line)
        cells.append({"cell_type": "markdown", "source": result_lines, "metadata": {}})

    for match in re.finditer(code_pattern, content, re.DOTALL):
        source = match.group(1)
        lines = source.split("\n")
        result_lines = []
        for i, line in enumerate(lines):
            if i < len(lines) - 1:
                result_lines.append(line + "\n")
            else:
                result_lines.append(line)
        cells.append(
            {
                "cell_type": "code",
                "source": result_lines,
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
        )

    return cells


def create_notebook(cells: list[dict]) -> dict:
    """Create a Jupyter notebook structure with required metadata."""
    for i, cell in enumerate(cells):
        cell.setdefault("id", f"cell-{i}")

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.0",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def is_xml_format(file_path: str) -> bool:
    """Check if file is in XML-like format (not JSON)."""
    with open(file_path, encoding="utf-8") as f:
        first_line = f.readline().strip()
    return "<markdown_cell>" in first_line or "<code_cell>" in first_line


def convert_file(input_path: str, output_path: str) -> tuple[bool, str]:
    """Convert a single notebook file."""
    try:
        cells = parse_notebook_file(input_path)

        if not cells:
            return False, "No cells found"

        notebook = create_notebook(cells)

        nbformat.validate(notebook)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(notebook, f, indent=1)

        return True, None
    except Exception as e:  # noqa: BLE001, RUF100
        return False, str(e)


def find_xml_notebooks(source_dir: str) -> list[Path]:
    """Find all XML-formatted notebook files to convert."""
    source_paths = []

    for root, _dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".ipynb") or file.endswith(".bak"):  # noqa: PIE810
                file_path = Path(root) / file
                full_path = str(file_path)

                if file.endswith(".bak") or is_xml_format(full_path):
                    source_paths.append(file_path)

    return sorted(source_paths)


def main():
    """Main conversion function."""
    source_dir = "/workspaces/pat2vec/notebooks/test"
    output_dir = "/workspaces/pat2vec/notebooks/test"

    xml_files = find_xml_notebooks(source_dir)

    print(f"Found {len(xml_files)} XML-formatted notebook files to convert")
    print("-" * 60)

    converted_count = 0
    failed_count = 0

    for input_path in xml_files:
        rel_path = input_path.relative_to(source_dir)

        output_file_name = rel_path.name
        if output_file_name.endswith(".bak"):
            output_file_name = output_file_name[:-4] + ".ipynb"
        elif not output_file_name.endswith(".ipynb"):
            output_file_name = output_file_name + ".ipynb"

        output_path = Path(output_dir) / rel_path.parent / output_file_name

        success, error_msg = convert_file(str(input_path), str(output_path))

        if success:
            print(
                f"✓ Converted: {rel_path} -> {output_path.relative_to(Path(output_dir))}",
            )
            converted_count += 1
        else:
            print(f"✗ Failed: {rel_path} - Error: {error_msg}")
            failed_count += 1

    print("-" * 60)
    print("CONVERSION SUMMARY")
    print(f"  - Successfully converted: {converted_count} files")
    print(f"  - Failed: {failed_count} files")

    return {"converted": converted_count, "failed": failed_count}


if __name__ == "__main__":
    result = main()
