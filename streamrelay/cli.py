"""
streamrelay.cli — Lifecycle management CLI for the WebSocket relay server.

Commands:

  streamrelay serve      Start the relay server in the foreground
  streamrelay install    Install as a systemd (Linux) or launchd (macOS) service
  streamrelay start      Start the installed service
  streamrelay stop       Stop the running service
  streamrelay restart    Restart the service
  streamrelay status     Show service status + WebSocket health check
  streamrelay uninstall  Remove the installed service

With no subcommand, ``serve`` is used (backward compatible with v0.2.x).

Environment variables respected by the relay server:
  RELAY_SECRET          Shared auth secret (all /produce and /consume connections)
  RELAY_PORT            Port to bind (default: 8765)
  RELAY_HOST            Bind address (default: 0.0.0.0)
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import textwrap

SERVICE_NAME = "streamrelay"
SYSTEMD_SERVICE_PATH = f"/etc/systemd/system/{SERVICE_NAME}.service"
LAUNCHD_PLIST_PATH = os.path.expanduser(
    f"~/Library/LaunchAgents/com.uicacer.{SERVICE_NAME}.plist"
)


def _python_bin() -> str:
    return sys.executable


def _is_linux() -> bool:
    return platform.system() == "Linux"


def _is_macos() -> bool:
    return platform.system() == "Darwin"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check)


def _require_root_or_sudo(action: str):
    if os.geteuid() != 0:
        print(f"[streamrelay] {action} requires root. Try: sudo streamrelay {action}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------

def cmd_serve(args):
    """Start the relay server in the foreground."""
    from streamrelay.server import main as server_main

    # Patch sys.argv so server's argparse sees the right args
    argv = ["streamrelay"]
    if hasattr(args, "host") and args.host:
        argv += ["--host", args.host]
    if hasattr(args, "port") and args.port:
        argv += ["--port", str(args.port)]
    if hasattr(args, "secret") and args.secret:
        argv += ["--secret", args.secret]
    if hasattr(args, "max_buffer") and args.max_buffer:
        argv += ["--max-buffer", str(args.max_buffer)]
    if hasattr(args, "channel_timeout") and args.channel_timeout:
        argv += ["--channel-timeout", str(args.channel_timeout)]
    if hasattr(args, "log_level") and args.log_level:
        argv += ["--log-level", args.log_level]

    old_argv = sys.argv
    sys.argv = argv
    try:
        server_main()
    finally:
        sys.argv = old_argv


# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------

def cmd_install(args):
    if _is_linux():
        _install_systemd(args)
    elif _is_macos():
        _install_launchd(args)
    else:
        print(f"[streamrelay] Unsupported platform: {platform.system()}")
        print("  Manual start: python -m streamrelay.server --host 0.0.0.0 --port 8765")
        sys.exit(1)


def _install_systemd(args):
    _require_root_or_sudo("install")
    python = _python_bin()
    secret = getattr(args, "secret", "") or ""
    port = getattr(args, "port", 8765) or 8765
    max_buffer = getattr(args, "max_buffer", 500) or 500
    channel_timeout = getattr(args, "channel_timeout", 300) or 300
    env_file = getattr(args, "env_file", "") or ""

    env_section = (
        f"EnvironmentFile={env_file}"
        if env_file
        else "# No EnvironmentFile — add with: EnvironmentFile=/path/to/relay-env"
    )
    secret_env = f"\nEnvironment=RELAY_SECRET={secret}" if secret else ""

    unit = textwrap.dedent(f"""\
        [Unit]
        Description=streamrelay WebSocket Relay Server
        After=network.target

        [Service]
        User={os.getenv('SUDO_USER', 'ubuntu')}
        {env_section}{secret_env}
        ExecStart={python} -m streamrelay.server --host 0.0.0.0 --port {port} --max-buffer {max_buffer} --channel-timeout {channel_timeout} --log-level INFO
        Restart=always
        RestartSec=5
        StandardOutput=journal
        StandardError=journal

        [Install]
        WantedBy=multi-user.target
    """)

    with open(SYSTEMD_SERVICE_PATH, "w") as f:
        f.write(unit)
    print(f"[streamrelay] Service file written: {SYSTEMD_SERVICE_PATH}")
    _run(["systemctl", "daemon-reload"])
    _run(["systemctl", "enable", SERVICE_NAME])
    print("[streamrelay] Installed. Run: sudo streamrelay start")
    if not secret and not env_file:
        print("  NOTE: Set RELAY_SECRET for production deployments:")
        print(f"    Add 'Environment=RELAY_SECRET=your-secret' to {SYSTEMD_SERVICE_PATH}")
        print("    Or: sudo streamrelay install --secret your-secret")


def _install_launchd(args):
    python = _python_bin()
    secret = getattr(args, "secret", "") or ""
    port = getattr(args, "port", 8765) or 8765
    env_file = getattr(args, "env_file", "") or ""

    env_dict: dict[str, str] = {}
    if env_file and os.path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_dict[k.strip()] = v.strip()
    if secret:
        env_dict["RELAY_SECRET"] = secret

    env_xml = "\n".join(
        f"            <key>{k}</key><string>{v}</string>"
        for k, v in env_dict.items()
    )
    env_block = (
        f"<key>EnvironmentVariables</key>\n        <dict>\n{env_xml}\n        </dict>"
        if env_dict
        else ""
    )

    plist = textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
          "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key><string>com.uicacer.{SERVICE_NAME}</string>
            <key>ProgramArguments</key>
            <array>
                <string>{python}</string>
                <string>-m</string><string>streamrelay.server</string>
                <string>--host</string><string>0.0.0.0</string>
                <string>--port</string><string>{port}</string>
            </array>
            {env_block}
            <key>RunAtLoad</key><true/>
            <key>KeepAlive</key><true/>
            <key>StandardOutPath</key><string>/tmp/streamrelay.log</string>
            <key>StandardErrorPath</key><string>/tmp/streamrelay.error.log</string>
        </dict>
        </plist>
    """)

    os.makedirs(os.path.dirname(LAUNCHD_PLIST_PATH), exist_ok=True)
    with open(LAUNCHD_PLIST_PATH, "w") as f:
        f.write(plist)
    print(f"[streamrelay] LaunchAgent written: {LAUNCHD_PLIST_PATH}")
    _run(["launchctl", "load", LAUNCHD_PLIST_PATH], check=False)
    print("[streamrelay] Installed and loaded.")


# ---------------------------------------------------------------------------
# start / stop / restart / status / uninstall
# ---------------------------------------------------------------------------

def cmd_start(args):
    if _is_linux():
        _require_root_or_sudo("start")
        _run(["systemctl", "start", SERVICE_NAME])
        print("[streamrelay] Started.")
    elif _is_macos():
        _run(["launchctl", "load", "-w", LAUNCHD_PLIST_PATH], check=False)
    else:
        print("[streamrelay] Use: python -m streamrelay.server --port 8765")


def cmd_stop(args):
    if _is_linux():
        _require_root_or_sudo("stop")
        _run(["systemctl", "stop", SERVICE_NAME])
        print("[streamrelay] Stopped.")
    elif _is_macos():
        _run(["launchctl", "unload", LAUNCHD_PLIST_PATH], check=False)


def cmd_restart(args):
    if _is_linux():
        _require_root_or_sudo("restart")
        _run(["systemctl", "restart", SERVICE_NAME])
        print("[streamrelay] Restarted.")
    elif _is_macos():
        _run(["launchctl", "unload", LAUNCHD_PLIST_PATH], check=False)
        _run(["launchctl", "load", LAUNCHD_PLIST_PATH], check=False)


def cmd_status(args):
    print("[streamrelay] Service status:")
    if _is_linux():
        _run(["systemctl", "status", SERVICE_NAME, "--no-pager"], check=False)
    elif _is_macos():
        _run(["launchctl", "list", f"com.uicacer.{SERVICE_NAME}"], check=False)

    # WebSocket health check
    host = getattr(args, "host", "127.0.0.1") or "127.0.0.1"
    port = getattr(args, "port", 8765) or 8765
    print(f"\n[streamrelay] WebSocket health check → ws://{host}:{port}/health")
    try:
        from websockets.sync.client import connect as ws_connect
        with ws_connect(f"ws://{host}:{port}/health", open_timeout=5) as ws:
            raw = ws.recv()
            data = json.loads(raw)
            print(f"  status: {data.get('status')}")
            print(f"  active_channels: {data.get('active_channels', 0)}")
            print(f"  timestamp: {data.get('timestamp')}")
    except Exception as e:
        print(f"  Health check failed: {e}")
        print(f"  (Is streamrelay running on port {port}?)")


def cmd_uninstall(args):
    if _is_linux():
        _require_root_or_sudo("uninstall")
        _run(["systemctl", "stop", SERVICE_NAME], check=False)
        _run(["systemctl", "disable", SERVICE_NAME], check=False)
        if os.path.exists(SYSTEMD_SERVICE_PATH):
            os.remove(SYSTEMD_SERVICE_PATH)
            print(f"[streamrelay] Removed: {SYSTEMD_SERVICE_PATH}")
        _run(["systemctl", "daemon-reload"])
        print("[streamrelay] Uninstalled.")
    elif _is_macos():
        _run(["launchctl", "unload", LAUNCHD_PLIST_PATH], check=False)
        if os.path.exists(LAUNCHD_PLIST_PATH):
            os.remove(LAUNCHD_PLIST_PATH)
            print(f"[streamrelay] Removed: {LAUNCHD_PLIST_PATH}")
        print("[streamrelay] Uninstalled.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """
    streamrelay CLI entry point.

    No subcommand → ``serve`` (backward compatible with v0.2.x).
    """
    parser = argparse.ArgumentParser(
        prog="streamrelay",
        description="streamrelay — WebSocket relay for real-time HPC output streaming",
    )
    subparsers = parser.add_subparsers(dest="command")

    # serve (mirrors server.py's argparse)
    p_serve = subparsers.add_parser("serve", help="Start the relay server (foreground)")
    p_serve.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    p_serve.add_argument("--port", type=int, default=8765, help="Port (default: 8765)")
    p_serve.add_argument("--secret", default="", help="Shared auth secret (reads RELAY_SECRET env var)")
    p_serve.add_argument("--max-buffer", type=int, default=1000, dest="max_buffer")
    p_serve.add_argument("--channel-timeout", type=int, default=300, dest="channel_timeout")
    p_serve.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
    )

    # install
    p_install = subparsers.add_parser("install", help="Install as a systemd/launchd service")
    p_install.add_argument("--port", type=int, default=8765, help="Port to listen on (default: 8765)")
    p_install.add_argument("--secret", default="", help="RELAY_SECRET value to embed in the service")
    p_install.add_argument(
        "--env-file",
        default="",
        dest="env_file",
        help="Path to an env file with RELAY_SECRET and other vars",
    )
    p_install.add_argument("--max-buffer", type=int, default=500, dest="max_buffer")
    p_install.add_argument("--channel-timeout", type=int, default=300, dest="channel_timeout")

    # start / stop / restart
    subparsers.add_parser("start", help="Start the installed service")
    subparsers.add_parser("stop", help="Stop the running service")
    subparsers.add_parser("restart", help="Restart the service")

    # status
    p_status = subparsers.add_parser("status", help="Show service status and WebSocket health check")
    p_status.add_argument("--host", default="127.0.0.1")
    p_status.add_argument("--port", type=int, default=8765)

    # uninstall
    subparsers.add_parser("uninstall", help="Remove the installed service")

    # For backward compat: if first arg looks like a serve flag (--host, --port etc.),
    # treat it as `serve` with those args
    argv = sys.argv[1:]
    if argv and not argv[0].startswith("-") and argv[0] not in (
        "serve", "install", "start", "stop", "restart", "status", "uninstall"
    ):
        # Unknown subcommand — print help
        parser.print_help()
        sys.exit(1)

    # If first arg starts with "--" (old-style: streamrelay --port 8765), prepend "serve"
    if argv and argv[0].startswith("--"):
        sys.argv = [sys.argv[0], "serve"] + argv

    args = parser.parse_args()

    dispatch = {
        "serve": cmd_serve,
        "install": cmd_install,
        "start": cmd_start,
        "stop": cmd_stop,
        "restart": cmd_restart,
        "status": cmd_status,
        "uninstall": cmd_uninstall,
        None: cmd_serve,
    }

    handler = dispatch.get(args.command, cmd_serve)
    handler(args)


if __name__ == "__main__":
    main()
