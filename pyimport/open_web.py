#!/usr/bin/env python
"""
PyImport Web Frontend Opener

Opens the PyImport REST API web interface in the default browser.
Optionally checks if the server is running and starts it if needed.

Usage:
    pyimport-web                    # Open web interface (default port 8000)
    pyimport-web --port 8080        # Open web interface on custom port
    pyimport-web --check            # Check if server is running first
    pyimport-web --start            # Start server if not running

@author: Claude Code
"""

import sys
import os
import argparse
import webbrowser
import subprocess
from time import sleep


def is_server_running(host: str = "localhost", port: int = 8000, timeout: int = 2) -> bool:
    """
    Check if the PyImport server is running

    Args:
        host: Server host
        port: Server port
        timeout: Connection timeout in seconds

    Returns:
        True if server is running, False otherwise
    """
    try:
        import requests
        url = f"http://{host}:{port}/health"
        response = requests.get(url, timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def start_server_background(host: str = "0.0.0.0", port: int = 8000):
    """
    Start the server in the background

    Args:
        host: Server host
        port: Server port
    """
    print(f"Starting PyImport server on {host}:{port}...")

    # Start server as a background process
    subprocess.Popen(
        [sys.executable, "-m", "pyimport.start_server", "--host", host, "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait for server to start
    print("Waiting for server to start...", end="", flush=True)
    for _ in range(10):
        sleep(1)
        print(".", end="", flush=True)
        if is_server_running("localhost", port):
            print(" ✓")
            return True

    print(" ✗")
    print("⚠️  Server failed to start within 10 seconds")
    return False


def open_web_interface(host: str = "localhost", port: int = 8000, check: bool = False, start: bool = False):
    """
    Open the PyImport web interface in browser

    Args:
        host: Server host
        port: Server port
        check: Check if server is running first
        start: Start server if not running
    """
    url = f"http://{host}:{port}/"

    # Check if server is running
    if check or start:
        if not is_server_running(host, port):
            if start:
                print(f"Server is not running on {host}:{port}")
                if not start_server_background("0.0.0.0", port):
                    print("❌ Failed to start server")
                    print("\nTo start the server manually, run:")
                    print(f"  pyimport-server --port {port}")
                    sys.exit(1)
            else:
                print(f"❌ Server is not running on {host}:{port}")
                print("\nPlease start the server first:")
                print(f"  pyimport-server --port {port}")
                print("\nOr use --start to automatically start the server:")
                print(f"  pyimport-web --port {port} --start")
                sys.exit(1)

    # Open browser
    print("=" * 70)
    print("🌐 Opening PyImport Web Interface")
    print("=" * 70)
    print(f"URL: {url}")
    print()
    print("The web interface allows you to:")
    print("  • Register new users")
    print("  • Login and get authentication tokens")
    print("  • View user profile information")
    print("  • Test authenticated import endpoints")
    print("=" * 70)

    try:
        webbrowser.open(url)
        print("✓ Browser opened successfully")
    except Exception as e:
        print(f"❌ Failed to open browser: {e}")
        print(f"\nPlease open this URL manually: {url}")
        sys.exit(1)


def main():
    """Main entry point for the web opener script"""
    parser = argparse.ArgumentParser(
        description="Open the PyImport REST API web interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pyimport-web                    # Open web interface on port 8000
  pyimport-web --port 8080        # Open on custom port
  pyimport-web --check            # Check if server is running first
  pyimport-web --start            # Start server if not running

The web interface provides:
  - User registration and authentication
  - Profile management
  - Testing of authenticated API endpoints
  - Interactive API documentation at /docs
        """
    )

    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Server host (default: localhost)"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Server port (default: $PYIMPORT_PORT or 8000)"
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if server is running before opening browser"
    )

    parser.add_argument(
        "--start",
        action="store_true",
        help="Start server automatically if not running"
    )

    args = parser.parse_args()

    # Get port from environment or default
    port = args.port or int(os.getenv("PYIMPORT_PORT", "8000"))

    open_web_interface(
        host=args.host,
        port=port,
        check=args.check,
        start=args.start
    )


if __name__ == "__main__":
    main()
