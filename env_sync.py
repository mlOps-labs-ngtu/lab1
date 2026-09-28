"""Синхронизация Airflow constraints в [tool.uv] pyproject.toml с чтением параметров из .env."""

import os
import re
import urllib.request
from pathlib import Path

ENV_PATH = Path(".env")
PYPROJECT_PATH = Path("pyproject.toml")


def load_dotenv(path: Path = ENV_PATH) -> None:
    """Загружает переменные из .env в os.environ (без сторонних библиотек)."""
    if not path.is_file():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        # Пропуск пустых строк и комментариев
        if not line or line.startswith("#"):
            continue

        # Поддержка синтаксиса 'export VAR=val'
        if line.startswith("export "):
            line = line[7:].strip()

        if "=" in line:
            key, val = line.split("=", 1)
            key = key.strip()
            # Удаление пробелов и обрамляющих кавычек
            val = val.strip().strip("'\"")
            # Записываем в os.environ, если еще не задано системно
            _ = os.environ.setdefault(key, val)


# Сначала считываем .env
load_dotenv(ENV_PATH)

# Извлекаем параметры с дефолтными значениями
AIRFLOW_VERSION = os.getenv("AIRFLOW_VERSION", "3.3.2")
PYTHON_VERSION = os.getenv("PYTHON_VERSION", "3.13")

# Если в .env задан готовый URL — берем его, иначе собираем динамически
CONSTRAINT_URL = os.getenv(
    "CONSTRAINT_URL",
    f"https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt",
)


def fetch_constraints(url: str) -> list[str]:
    print(f"Fetching constraints from: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        content = resp.read().decode("utf-8")

    packages: list[str] = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        packages.append(line)
    return packages


def update_pyproject(packages: list[str]) -> None:
    text = PYPROJECT_PATH.read_text(encoding="utf-8")

    formatted_deps = "constraint-dependencies = [\n"
    for pkg in packages:
        escaped_pkg = pkg.replace('"', '\\"')
        formatted_deps += f'  "{escaped_pkg}",\n'
    formatted_deps += "]"

    pattern = r"constraint-dependencies\s*=\s*\[[^\]]*\]"

    if re.search(pattern, text):
        new_text = re.sub(pattern, formatted_deps, text)
    elif "[tool.uv]" in text:
        new_text = text.replace("[tool.uv]", f"[tool.uv]\n{formatted_deps}")
    else:
        new_text = text.rstrip() + f"\n\n[tool.uv]\n{formatted_deps}\n"

    _ = PYPROJECT_PATH.write_text(new_text, encoding="utf-8")
    print(f"Successfully updated {len(packages)} constraints in {PYPROJECT_PATH}")


if __name__ == "__main__":
    print(f"Target environment: Airflow {AIRFLOW_VERSION} on Python {PYTHON_VERSION}")
    deps = fetch_constraints(CONSTRAINT_URL)
    update_pyproject(deps)
