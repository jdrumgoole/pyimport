#!/usr/bin/env python
"""
PyImport REST API Server Starter

Simple script to start the PyImport REST API server with sensible defaults.

Usage:
    pyimport-server                    # Start on default port 8000
    pyimport-server --port 8080        # Start on custom port
    pyimport-server --reload           # Start with auto-reload for development
    pyimport-server --host 0.0.0.0     # Listen on all interfaces

Environment Variables:
    PYIMPORT_HOST         - Server host (default: 0.0.0.0)
    PYIMPORT_PORT         - Server port (default: 8000)
    SECRET_KEY            - JWT secret key (REQUIRED for production)
    ACCESS_TOKEN_EXPIRE_MINUTES - Token expiration in minutes (default: 30)
    MONGODB_URI           - MongoDB connection string

@author: Claude Code
"""

import sys
import os
import argparse
import webbrowser
from time import sleep
from threading import Thread


def start_server(host: str = None, port: int = None, reload: bool = False, open_browser: bool = False):
    """
    Start the PyImport REST API server

    Args:
        host: Server host (default: from env or 0.0.0.0)
        port: Server port (default: from env or 8000)
        reload: Enable auto-reload for development
        open_browser: Open web browser after server starts
    """
    import uvicorn

    # Get defaults from environment or use hardcoded defaults
    host = host or os.getenv("PYIMPORT_HOST", "0.0.0.0")
    port = port or int(os.getenv("PYIMPORT_PORT", "8000"))

    # Check for production security warning
    secret_key = os.getenv("SECRET_KEY")
    if not secret_key or secret_key == "pyimport-secret-key-change-in-production":
        print("=" * 70)
        print("⚠️  WARNING: Using default SECRET_KEY")
        print("=" * 70)
        print("For production use, set a secure SECRET_KEY environment variable:")
        print("  export SECRET_KEY=\"$(python -c 'import secrets; print(secrets.token_urlsafe(32))')\"")
        print("=" * 70)
        print()

    print("=" * 70)
    print("🚀 Starting PyImport REST API Server")
    print("=" * 70)
    print(f"Server URL:        http://{host}:{port}")
    print(f"Web Interface:     http://localhost:{port}/")
    print(f"API Documentation: http://localhost:{port}/docs")
    print(f"Health Check:      http://localhost:{port}/health")
    print(f"Auto-reload:       {'Enabled' if reload else 'Disabled'}")
    print("=" * 70)
    print()
    print("Press CTRL+C to stop the server")
    print()

    # Open browser in background after server starts
    if open_browser:
        def open_browser_delayed():
            sleep(2)  # Wait for server to start
            url = f"http://localhost:{port}/"
            print(f"🌐 Opening browser at {url}")
            webbrowser.open(url)

        Thread(target=open_browser_delayed, daemon=True).start()

    # Start server
    try:
        uvicorn.run(
            "pyimport.rest_api:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n\n✓ Server stopped")
        sys.exit(0)


def main():
    """Main entry point for the server starter script"""
    parser = argparse.ArgumentParser(
        description="Start the PyImport REST API Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pyimport-server                          # Start with defaults
  pyimport-server --port 8080              # Custom port
  pyimport-server --reload                 # Development mode with auto-reload
  pyimport-server --open                   # Auto-open browser
  pyimport-server --host 0.0.0.0 --port 8000 --reload --open

Environment Variables:
  SECRET_KEY                  JWT secret key (required for production)
  PYIMPORT_HOST              Server host (default: 0.0.0.0)
  PYIMPORT_PORT              Server port (default: 8000)
  ACCESS_TOKEN_EXPIRE_MINUTES Token expiration (default: 30)
  MONGODB_URI                MongoDB connection string
        """
    )

    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Server host (default: $PYIMPORT_HOST or 0.0.0.0)"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Server port (default: $PYIMPORT_PORT or 8000)"
    )

    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development"
    )

    parser.add_argument(
        "--open",
        action="store_true",
        help="Open web browser after server starts"
    )

    args = parser.parse_args()

    start_server(
        host=args.host,
        port=args.port,
        reload=args.reload,
        open_browser=args.open
    )


if __name__ == "__main__":
    main()
