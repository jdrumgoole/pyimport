#!/usr/bin/env python
"""
PyImport Web Frontend Closer

Close browser tabs/windows showing the PyImport web interface.

Note: This script attempts to close browser tabs, but due to browser security
restrictions, it may not work on all platforms. On macOS, it uses AppleScript
to close Safari/Chrome tabs. On other platforms, it provides instructions.

Usage:
    pyimport-close                     # Close web interface (port 8000)
    pyimport-close --port 8080         # Close web interface on port 8080
    pyimport-close --browser safari    # Close specific browser
    pyimport-close --stop-server       # Also stop the server

@author: Claude Code
"""

import sys
import os
import argparse
import platform
import subprocess


def close_browser_tab_macos(url: str, browser: str = None):
    """
    Close browser tab on macOS using AppleScript

    Args:
        url: URL to close
        browser: Browser name (safari, chrome, firefox, None for all)

    Returns:
        True if successful, False otherwise
    """
    browsers = []

    if browser:
        browsers = [browser.lower()]
    else:
        # Try all common browsers
        browsers = ['safari', 'chrome', 'firefox']

    success = False

    for browser_name in browsers:
        if browser_name == 'safari':
            applescript = f'''
            tell application "Safari"
                set windowCount to number of windows
                repeat with x from 1 to windowCount
                    set tabCount to number of tabs in window x
                    repeat with y from 1 to tabCount
                        set tabURL to URL of tab y of window x
                        if tabURL contains "{url}" then
                            close tab y of window x
                            set success to true
                            return
                        end if
                    end repeat
                end repeat
            end tell
            '''
        elif browser_name == 'chrome':
            applescript = f'''
            tell application "Google Chrome"
                set windowCount to number of windows
                repeat with x from 1 to windowCount
                    set tabCount to number of tabs in window x
                    repeat with y from 1 to tabCount
                        set tabURL to URL of tab y of window x
                        if tabURL contains "{url}" then
                            close tab y of window x
                            return
                        end if
                    end repeat
                end repeat
            end tell
            '''
        elif browser_name == 'firefox':
            print(f"  ℹ️  Firefox tabs cannot be closed via script")
            print(f"     Please manually close tabs with URL: {url}")
            continue
        else:
            continue

        try:
            result = subprocess.run(
                ['osascript', '-e', applescript],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                print(f"  ✓ Closed tab in {browser_name.title()}")
                success = True
            else:
                # Silently continue if browser not running or tab not found
                pass
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
            pass

    return success


def close_browser_tab_windows(url: str, browser: str = None):
    """
    Close browser tab on Windows

    Note: Windows doesn't provide easy programmatic tab closing.
    This function prints instructions instead.

    Args:
        url: URL to close
        browser: Browser name (ignored)
    """
    print(f"  ℹ️  On Windows, browser tabs cannot be closed programmatically")
    print(f"     Please manually close browser tabs with URL: {url}")
    print(f"     Or close your browser and restart it")
    return False


def close_browser_tab_linux(url: str, browser: str = None):
    """
    Close browser tab on Linux

    Note: Linux doesn't provide easy programmatic tab closing.
    This function prints instructions instead.

    Args:
        url: URL to close
        browser: Browser name (ignored)
    """
    print(f"  ℹ️  On Linux, browser tabs cannot be closed programmatically")
    print(f"     Please manually close browser tabs with URL: {url}")
    print(f"")
    print(f"     You can also try these commands:")
    print(f"     - pkill -f '{url}'  (may close entire browser)")
    return False


def close_web_interface(port: int = 8000, browser: str = None, stop_server: bool = False):
    """
    Close PyImport web interface

    Args:
        port: Server port
        browser: Specific browser to close (safari, chrome, firefox)
        stop_server: Also stop the server
    """
    url = f"localhost:{port}"

    print("=" * 70)
    print("🔒 Closing PyImport Web Interface")
    print("=" * 70)
    print(f"URL: http://{url}/")
    print()

    # Detect platform and close tabs
    system = platform.system()

    if system == "Darwin":  # macOS
        success = close_browser_tab_macos(url, browser)
        if success:
            print()
            print("✓ Browser tab(s) closed successfully")
        else:
            print()
            print("⚠️  Could not find or close browser tabs automatically")
            print(f"   Please manually close tabs with URL: http://{url}/")
    elif system == "Windows":
        close_browser_tab_windows(url, browser)
    elif system == "Linux":
        close_browser_tab_linux(url, browser)
    else:
        print(f"  ℹ️  Unsupported platform: {system}")
        print(f"     Please manually close browser tabs with URL: http://{url}/")

    # Stop server if requested
    if stop_server:
        print()
        print("=" * 70)
        print("Stopping server...")
        print("=" * 70)
        try:
            from pyimport.stop_server import stop_servers
            stop_servers(port=port, all_servers=False, force=False)
        except Exception as e:
            print(f"❌ Error stopping server: {e}")
            print(f"   Try running: pyimport-stop --port {port}")

    print("=" * 70)


def main():
    """Main entry point for the web closer script"""
    parser = argparse.ArgumentParser(
        description="Close PyImport web interface browser tabs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pyimport-close                      # Close web interface on port 8000
  pyimport-close --port 8080          # Close on custom port
  pyimport-close --browser safari     # Close specific browser
  pyimport-close --stop-server        # Also stop the server

Supported Browsers (macOS only):
  - Safari (automatically closed)
  - Chrome (automatically closed)
  - Firefox (manual close required)

Note:
  On macOS, this script uses AppleScript to close browser tabs.
  On Windows/Linux, manual closing is required due to security restrictions.
        """
    )

    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Server port (default: $PYIMPORT_PORT or 8000)"
    )

    parser.add_argument(
        "--browser",
        type=str,
        choices=['safari', 'chrome', 'firefox'],
        default=None,
        help="Specific browser to close (macOS only)"
    )

    parser.add_argument(
        "--stop-server",
        action="store_true",
        help="Also stop the server after closing browser"
    )

    args = parser.parse_args()

    # Get port from environment or default
    port = args.port or int(os.getenv("PYIMPORT_PORT", "8000"))

    try:
        close_web_interface(
            port=port,
            browser=args.browser,
            stop_server=args.stop_server
        )
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()
