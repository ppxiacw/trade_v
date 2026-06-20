import os
from pathlib import Path


def load_local_env(env_path=None):
    """Load simple KEY=VALUE pairs from .env without overriding existing env vars."""
    if env_path is None:
        env_path = Path(__file__).resolve().parents[1] / '.env'
    else:
        env_path = Path(env_path)

    if not env_path.exists():
        return

    try:
        lines = env_path.read_text(encoding='utf-8').splitlines()
    except UnicodeDecodeError:
        lines = env_path.read_text(encoding='utf-8-sig').splitlines()

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
