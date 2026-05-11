import json
from pathlib import Path

NOTEBOOK_DIR = Path("notebooks")

def convert_cell_to_markdown(cell):
    source = "".join(cell.get("source", []))

    # Zaten markdown ise dokunma
    if cell.get("cell_type") == "markdown":
        return cell

    # Python olmayan içerikleri markdown kod bloğuna çevir
    non_python_indicators = [
        "docker compose",
        "docker exec",
        "spark-submit",
        "services:",
        "FROM python",
        "COPY ",
        "RUN ",
        "CMD ",
        "EXPOSE ",
        "KAFKA_",
        "mlflow server",
        "version:",
        "image:",
        "container_name:",
        "ports:",
        "volumes:",
        "environment:",
    ]

    if any(indicator in source for indicator in non_python_indicators):
        return {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["```bash\n", source, "\n```"]
        }

    return cell


for notebook_path in NOTEBOOK_DIR.glob("*.ipynb"):
    with open(notebook_path, "r", encoding="utf-8") as f:
        notebook = json.load(f)

    notebook["cells"] = [
        convert_cell_to_markdown(cell)
        for cell in notebook["cells"]
    ]

    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, ensure_ascii=False, indent=2)

    print(f"Fixed: {notebook_path}")

print("Notebook hücreleri düzeltildi.")