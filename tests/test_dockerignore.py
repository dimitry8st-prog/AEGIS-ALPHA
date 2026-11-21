"""The image build context must drop local secret files and keep the project."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _patterns() -> list[str]:
    lines = []
    for raw in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


def test_dockerignore_excludes_secret_files_and_keeps_project_paths():
    lines = _patterns()
    assert ".env" in lines
    assert ".env.*" in lines
    assert "!.env.example" in lines
    assert "secrets/" in lines
    for kept in (
        "Dockerfile",
        "docker-compose.yml",
        "requirements.txt",
        ".env.example",
        "app",
        "worker",
        "scripts",
        "alembic",
    ):
        assert kept not in lines
        assert f"{kept}/" not in lines
