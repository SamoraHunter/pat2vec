#!/usr/bin/env python3
"""Generate requirements.txt from pyproject.toml for production deployments."""

import re
from pathlib import Path


def parse_deps_block(block):
    packages = []
    for line in block.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        m = re.match(r'["\']([^"\'=<>!~\[\],]+)[^"\']*["\']', stripped)
        if m:
            pkg = m.group(1).strip()
            if pkg and pkg != "...":
                packages.append(pkg)
    return packages


def extract_dependencies(pyproject_path=None):
    if pyproject_path is None:
        pyproject_path = Path(__file__).parent.parent / "pyproject.toml"

    content = pyproject_path.read_text()
    result = {"core": [], "all": [], "dev": []}

    m_deps = re.search(r"dependencies\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if m_deps:
        result["core"] = parse_deps_block(m_deps.group(1))

    # Match from [project.optional-dependencies] until the next section
    m_opt = re.search(
        r"\[project\.optional-dependencies\](.*?)(?=\n\[|\Z)",
        content,
        re.DOTALL,
    )
    if m_opt:
        opt_section = m_opt.group(1)

        for grp_name in ["all", "dev"]:
            m_grp = re.search(rf"{grp_name}\s*=\s*\[(.*?)\]", opt_section, re.DOTALL)
            if m_grp:
                result[grp_name] = parse_deps_block(m_grp.group(1))

    return result


def generate_requirements_txt(output_path=None):
    if output_path is None:
        output_path = Path(__file__).parent.parent / "requirements.txt"

    deps = extract_dependencies()

    lines = [
        "# Production dependencies for pat2vec",
        f"# Generated automatically from pyproject.toml on {Path.cwd()}",
        "# To update: python scripts/generate_requirements.py",
        "",
        "# Core dependencies (production required)",
    ]

    lines.extend(sorted(set(deps["core"])))

    if deps["all"]:
        lines.extend(
            [
                "",
                "# Optional features ('all' group)",
            ],
        )
        lines.extend(sorted(set(deps["all"])))

    if deps["dev"]:
        lines.extend(
            [
                "",
                "# Development and testing (not for production)",
            ],
        )
        lines.extend(sorted(set(deps["dev"])))

    output_path.write_text("\n".join(lines) + "\n")
    total_packages = len(set(deps["core"] + deps["all"] + deps["dev"]))

    print(f"Generated: {output_path}")
    print(f"Core packages: {len(set(deps['core']))}")
    print(f"All packages: {len(set(deps['all']))}")
    print(f"Dev packages: {len(set(deps['dev']))}")
    print(f"Total unique packages: {total_packages}")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(generate_requirements_txt())
