#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import plistlib
import re
import subprocess
import sys
from pathlib import Path
from typing import Tuple


DEFAULT_LABEL = "com.personalfund.daily"
DEFAULT_TIME = "19:00"


def parse_hhmm(value: str) -> Tuple[int, int]:
    text = value.strip()
    match = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", text)
    if not match:
        raise argparse.ArgumentTypeError(f"invalid time '{value}', expected HH:MM")
    return int(match.group(1)), int(match.group(2))


def ensure_macos() -> None:
    if sys.platform != "darwin":
        raise RuntimeError("launchd scheduler only supports macOS")


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_python_bin(root: Path) -> Path:
    venv_python = root / ".venv" / "bin" / "python"
    if venv_python.exists():
        return venv_python
    return Path(sys.executable).resolve()


def default_job_script(root: Path) -> Path:
    return root / "scripts" / "generate_daily_data.py"


def plist_path_for_label(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def service_name(label: str) -> str:
    return f"{launchctl_domain()}/{label}"


def run_cmd(args: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=check, text=True, capture_output=True)


def build_plist_content(
    *,
    label: str,
    hour: int,
    minute: int,
    python_bin: Path,
    job_script: Path,
    working_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
    success_popup: bool,
) -> dict:
    program_args = [str(python_bin), str(job_script), "--engine", "python", "--notify-on-error"]
    if success_popup:
        program_args.append("--notify-on-success")
    else:
        program_args.append("--no-notify-on-success")

    return {
        "Label": label,
        "ProgramArguments": program_args,
        "WorkingDirectory": str(working_dir),
        "StartCalendarInterval": {"Hour": hour, "Minute": minute},
        "RunAtLoad": False,
        "StandardOutPath": str(stdout_path),
        "StandardErrorPath": str(stderr_path),
    }


def write_plist(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        plistlib.dump(content, f, sort_keys=False)


def install_schedule(args: argparse.Namespace) -> int:
    ensure_macos()
    root = project_root()
    hour, minute = parse_hhmm(args.time)
    python_bin = Path(args.python_bin).expanduser().resolve() if args.python_bin else default_python_bin(root)
    job_script = Path(args.script).expanduser().resolve() if args.script else default_job_script(root)
    label = args.label
    plist_path = plist_path_for_label(label)
    logs_dir = root / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = logs_dir / "schedule_stdout.log"
    stderr_path = logs_dir / "schedule_stderr.log"

    if not python_bin.exists():
        raise FileNotFoundError(f"python not found: {python_bin}")
    if not job_script.exists():
        raise FileNotFoundError(f"job script not found: {job_script}")

    plist_content = build_plist_content(
        label=label,
        hour=hour,
        minute=minute,
        python_bin=python_bin,
        job_script=job_script,
        working_dir=root,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        success_popup=not args.no_success_popup,
    )
    write_plist(plist_path, plist_content)

    domain = launchctl_domain()
    run_cmd(["launchctl", "bootout", domain, str(plist_path)])
    boot = run_cmd(["launchctl", "bootstrap", domain, str(plist_path)])
    if boot.returncode != 0:
        raise RuntimeError((boot.stderr or boot.stdout).strip() or "launchctl bootstrap failed")

    run_cmd(["launchctl", "enable", service_name(label)])
    print(f"Installed: {plist_path}")
    print(f"Schedule: {hour:02d}:{minute:02d} every day")
    print(f"Service: {service_name(label)}")
    print(f"Logs: {stdout_path} | {stderr_path}")
    return 0


def uninstall_schedule(args: argparse.Namespace) -> int:
    ensure_macos()
    label = args.label
    plist_path = plist_path_for_label(label)
    domain = launchctl_domain()

    run_cmd(["launchctl", "bootout", domain, str(plist_path)])
    run_cmd(["launchctl", "disable", service_name(label)])
    if plist_path.exists():
        plist_path.unlink()
        print(f"Removed: {plist_path}")
    else:
        print(f"Not found: {plist_path}")
    return 0


def status_schedule(args: argparse.Namespace) -> int:
    ensure_macos()
    label = args.label
    plist_path = plist_path_for_label(label)
    print(f"Plist: {plist_path} ({'exists' if plist_path.exists() else 'missing'})")

    result = run_cmd(["launchctl", "print", service_name(label)])
    if result.returncode == 0:
        print(f"Loaded: yes ({service_name(label)})")
        return 0

    print(f"Loaded: no ({service_name(label)})")
    msg = (result.stderr or result.stdout).strip()
    if msg:
        print(msg)
    return 1


def run_now(args: argparse.Namespace) -> int:
    ensure_macos()
    label = args.label
    result = run_cmd(["launchctl", "kickstart", "-k", service_name(label)])
    if result.returncode != 0:
        msg = (result.stderr or result.stdout).strip() or "launchctl kickstart failed"
        raise RuntimeError(msg)
    print(f"Triggered: {service_name(label)}")
    return 0


def print_example(args: argparse.Namespace) -> int:
    root = project_root()
    hour, minute = parse_hhmm(args.time)
    python_bin = default_python_bin(root)
    job_script = default_job_script(root)
    plist = build_plist_content(
        label=args.label,
        hour=hour,
        minute=minute,
        python_bin=python_bin,
        job_script=job_script,
        working_dir=root,
        stdout_path=root / "data" / "logs" / "schedule_stdout.log",
        stderr_path=root / "data" / "logs" / "schedule_stderr.log",
        success_popup=not args.no_success_popup,
    )
    print(plistlib.dumps(plist, sort_keys=False).decode("utf-8"))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage macOS launchd schedule for PersonalFund daily job."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    install = sub.add_parser("install", help="Install or update daily schedule")
    install.add_argument("--time", default=DEFAULT_TIME, help="Target time HH:MM (24h), default 19:00")
    install.add_argument("--label", default=DEFAULT_LABEL, help="launchd label")
    install.add_argument("--python-bin", default="", help="Python executable path (default uses .venv/bin/python)")
    install.add_argument("--script", default="", help="Job script path (default scripts/generate_daily_data.py)")
    install.add_argument(
        "--no-success-popup",
        action="store_true",
        help="Disable success popup for scheduled run",
    )
    install.set_defaults(func=install_schedule)

    uninstall = sub.add_parser("uninstall", help="Uninstall schedule")
    uninstall.add_argument("--label", default=DEFAULT_LABEL, help="launchd label")
    uninstall.set_defaults(func=uninstall_schedule)

    status = sub.add_parser("status", help="Check schedule status")
    status.add_argument("--label", default=DEFAULT_LABEL, help="launchd label")
    status.set_defaults(func=status_schedule)

    run = sub.add_parser("run-now", help="Trigger scheduled job immediately")
    run.add_argument("--label", default=DEFAULT_LABEL, help="launchd label")
    run.set_defaults(func=run_now)

    example = sub.add_parser("print-plist", help="Print example plist XML")
    example.add_argument("--time", default=DEFAULT_TIME, help="Target time HH:MM (24h), default 19:00")
    example.add_argument("--label", default=DEFAULT_LABEL, help="launchd label")
    example.add_argument(
        "--no-success-popup",
        action="store_true",
        help="Disable success popup in printed example",
    )
    example.set_defaults(func=print_example)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
