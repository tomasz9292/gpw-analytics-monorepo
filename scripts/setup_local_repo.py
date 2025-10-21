#!/usr/bin/env python3
"""Utility to clone this repository locally with optional configuration.

This script helps Windows users who only have the project hosted on GitHub
and want to obtain a fully initialized local Git working tree.  It accepts the
remote URL of the repository and destination directory, verifies that Git is
installed, and executes the clone command.  Optional flags can set default
Git user identity and checkout a specific branch once the repository is
cloned.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional


def _run_git_command(args: Iterable[str], cwd: Optional[Path] = None) -> None:
    """Run a git command and raise a descriptive error on failure."""
    process = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        cmd_display = " ".join(["git", *args])
        message = [
            f"Command `{cmd_display}` failed with exit code {process.returncode}.",
            "STDOUT:\n" + process.stdout.strip(),
            "STDERR:\n" + process.stderr.strip(),
        ]
        raise RuntimeError("\n".join(part for part in message if part.strip()))


def ensure_git_is_available() -> None:
    """Ensure that the git executable is accessible."""
    if shutil.which("git") is None:
        raise EnvironmentError(
            "Git executable not found in PATH. Install Git and try again."
        )


def clone_repository(remote: str, destination: Path, branch: Optional[str]) -> None:
    """Clone the repository to the destination path."""
    if destination.exists():
        raise FileExistsError(
            f"Destination path '{destination}' already exists. Choose an empty folder."
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    clone_args = ["clone", remote, str(destination)]
    if branch:
        clone_args.extend(["--branch", branch])
    _run_git_command(clone_args)


def configure_user(destination: Path, name: Optional[str], email: Optional[str]) -> None:
    """Configure Git user identity for the cloned repository."""
    if not name and not email:
        return
    if name:
        _run_git_command(["config", "user.name", name], cwd=destination)
    if email:
        _run_git_command(["config", "user.email", email], cwd=destination)


def create_virtualenv(
    destination: Path,
    env_name: str,
    python_executable: Optional[str],
) -> Path:
    """Create a Python virtual environment within the repository."""

    env_path = destination / env_name
    if env_path.exists():
        raise FileExistsError(
            f"Virtual environment path '{env_path}' already exists. Remove it or choose another name."
        )

    python_cmd = python_executable or sys.executable
    process = subprocess.run(
        [python_cmd, "-m", "venv", str(env_path)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        message = [
            "Failed to create virtual environment.",
            f"Command: {python_cmd} -m venv {env_path}",
            "STDOUT:\n" + process.stdout.strip(),
            "STDERR:\n" + process.stderr.strip(),
        ]
        raise RuntimeError("\n".join(part for part in message if part.strip()))

    return env_path


def parse_arguments(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clone the GPW analytics repository locally and optionally configure Git.",
    )
    parser.add_argument(
        "remote",
        help="Remote Git URL to clone (e.g. https://github.com/org/repo.git)",
    )
    parser.add_argument(
        "destination",
        nargs="?",
        default="gpw-analytics-monorepo",
        help="Destination directory for the clone (default: gpw-analytics-monorepo)",
    )
    parser.add_argument(
        "--branch",
        help="Optional branch to checkout immediately after cloning.",
    )
    parser.add_argument(
        "--user-name",
        help="Configure this Git user.name inside the cloned repository.",
    )
    parser.add_argument(
        "--user-email",
        help="Configure this Git user.email inside the cloned repository.",
    )
    parser.add_argument(
        "--create-venv",
        action="store_true",
        help="Create a Python virtual environment inside the cloned repository.",
    )
    parser.add_argument(
        "--venv-name",
        default=".venv",
        help="Name of the virtual environment directory (default: .venv).",
    )
    parser.add_argument(
        "--python",
        help="Python executable to use when creating the virtual environment (defaults to the interpreter running this script).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_arguments(argv)
    ensure_git_is_available()

    destination = Path(args.destination).expanduser().resolve()
    env_path: Optional[Path] = None

    try:
        clone_repository(args.remote, destination, args.branch)
        configure_user(destination, args.user_name, args.user_email)
        if args.create_venv:
            env_path = create_virtualenv(destination, args.venv_name, args.python)
    except Exception as error:  # noqa: BLE001
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(
        "Repository cloned successfully to",
        destination,
    )
    if args.user_name or args.user_email:
        print("Configured Git identity for this repository.")
    if args.create_venv and env_path is not None:
        if sys.platform.startswith("win"):
            activate_cmd = f"{env_path}\\Scripts\\activate"
        else:
            activate_cmd = f"source {env_path}/bin/activate"
        print("Created virtual environment at", env_path)
        print("Activate it with:")
        print("  ", activate_cmd)
        print(
            "After activation you can install backend dependencies with `pip install -r backend/requirements.txt`."
        )
    print(
        "You can now open the folder in your IDE or run development commands, e.g. `npm run dev` or `uvicorn`."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
