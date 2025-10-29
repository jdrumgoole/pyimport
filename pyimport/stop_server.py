#!/usr/bin/env python
"""
PyImport REST API Server Stopper

Stop running PyImport REST API server processes.

Usage:
    pyimport-stop                      # Stop servers on default port 8000
    pyimport-stop --port 8080          # Stop servers on specific port
    pyimport-stop --all                # Stop all PyImport servers
    pyimport-stop --force              # Force kill (SIGKILL instead of SIGTERM)

@author: Claude Code
"""

import sys
import os
import argparse
import signal
import psutil
from time import sleep


def find_server_processes(port: int = None, all_servers: bool = False):
    """
    Find PyImport server processes

    Args:
        port: Specific port to look for (None for all)
        all_servers: Find all PyImport servers regardless of port

    Returns:
        List of (pid, port, cmdline) tuples
    """
    processes = []

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if not cmdline:
                continue

            # Check if this is a PyImport server process
            cmdline_str = ' '.join(cmdline)
            if 'pyimport' in cmdline_str and ('rest_api' in cmdline_str or 'pyimport-api' in cmdline_str or 'pyimport-server' in cmdline_str or 'uvicorn' in cmdline_str):

                # Try to get the port from connections
                proc_port = None
                try:
                    connections = proc.connections()
                    for conn in connections:
                        if conn.status == 'LISTEN':
                            proc_port = conn.laddr.port
                            break
                except (psutil.AccessDenied, psutil.NoSuchProcess, AttributeError):
                    pass

                # If we couldn't get port from connections, try to parse from cmdline
                if proc_port is None:
                    for i, arg in enumerate(cmdline):
                        if arg in ['--port', '-p'] and i + 1 < len(cmdline):
                            try:
                                proc_port = int(cmdline[i + 1])
                                break
                            except ValueError:
                                pass

                # Default to 8000 if we still don't have a port
                if proc_port is None:
                    proc_port = int(os.getenv('PYIMPORT_PORT', '8000'))

                # Check if this process matches our criteria
                if all_servers or port is None or proc_port == port:
                    processes.append((proc.info['pid'], proc_port, cmdline_str))

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    return processes


def stop_process(pid: int, force: bool = False, timeout: int = 5) -> bool:
    """
    Stop a process by PID

    Args:
        pid: Process ID
        force: Use SIGKILL instead of SIGTERM
        timeout: Seconds to wait for graceful shutdown

    Returns:
        True if process was stopped, False otherwise
    """
    try:
        proc = psutil.Process(pid)

        if force:
            # Force kill immediately
            proc.kill()
            print(f"  ⚡ Force killed process {pid}")
            return True
        else:
            # Try graceful shutdown first
            proc.terminate()

            # Wait for process to terminate
            try:
                proc.wait(timeout=timeout)
                print(f"  ✓ Gracefully stopped process {pid}")
                return True
            except psutil.TimeoutExpired:
                # If graceful shutdown failed, force kill
                print(f"  ⚠️  Process {pid} didn't stop gracefully, force killing...")
                proc.kill()
                proc.wait(timeout=2)
                print(f"  ⚡ Force killed process {pid}")
                return True

    except psutil.NoSuchProcess:
        print(f"  ℹ️  Process {pid} already stopped")
        return True
    except psutil.AccessDenied:
        print(f"  ❌ Permission denied to stop process {pid}")
        return False
    except Exception as e:
        print(f"  ❌ Error stopping process {pid}: {e}")
        return False


def stop_servers(port: int = None, all_servers: bool = False, force: bool = False):
    """
    Stop PyImport server processes

    Args:
        port: Specific port to stop (None for default 8000)
        all_servers: Stop all PyImport servers
        force: Force kill processes
    """
    # Find processes
    processes = find_server_processes(port=port, all_servers=all_servers)

    if not processes:
        if all_servers:
            print("✓ No PyImport server processes found")
        else:
            target_port = port or int(os.getenv('PYIMPORT_PORT', '8000'))
            print(f"✓ No PyImport server found on port {target_port}")
        return

    # Print header
    print("=" * 70)
    print("🛑 Stopping PyImport Server(s)")
    print("=" * 70)

    # Stop each process
    stopped = 0
    failed = 0

    for pid, proc_port, cmdline in processes:
        print(f"\nFound server process:")
        print(f"  PID:  {pid}")
        print(f"  Port: {proc_port}")
        print(f"  Command: {cmdline[:80]}...")

        if stop_process(pid, force=force):
            stopped += 1
        else:
            failed += 1

    # Print summary
    print("\n" + "=" * 70)
    if failed == 0:
        print(f"✓ Successfully stopped {stopped} server(s)")
    else:
        print(f"⚠️  Stopped {stopped} server(s), failed to stop {failed}")
    print("=" * 70)


def main():
    """Main entry point for the server stopper script"""
    parser = argparse.ArgumentParser(
        description="Stop PyImport REST API Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pyimport-stop                 # Stop server on default port 8000
  pyimport-stop --port 8080     # Stop server on port 8080
  pyimport-stop --all           # Stop all PyImport servers
  pyimport-stop --force         # Force kill (SIGKILL)
  pyimport-stop --all --force   # Force kill all servers

Notes:
  - By default, attempts graceful shutdown (SIGTERM) first
  - Use --force to immediately kill processes (SIGKILL)
  - Requires appropriate permissions to stop processes
        """
    )

    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Stop server on specific port (default: $PYIMPORT_PORT or 8000)"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Stop all PyImport servers regardless of port"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force kill processes (SIGKILL) instead of graceful shutdown"
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List running servers without stopping them"
    )

    args = parser.parse_args()

    # If --list, just show running servers
    if args.list:
        processes = find_server_processes(port=args.port, all_servers=args.all)
        if not processes:
            print("No PyImport server processes found")
        else:
            print("=" * 70)
            print("Running PyImport Server(s)")
            print("=" * 70)
            for pid, port, cmdline in processes:
                print(f"\nPID:  {pid}")
                print(f"Port: {port}")
                print(f"Command: {cmdline}")
            print("=" * 70)
        return

    # Get port
    port = args.port
    if port is None and not args.all:
        port = int(os.getenv('PYIMPORT_PORT', '8000'))

    # Stop servers
    try:
        stop_servers(port=port, all_servers=args.all, force=args.force)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()
