"""
Invoke tasks file - replacement for Makefile
Run with: invoke <task-name>
Example: invoke test-all
"""

import os
from pathlib import Path
from invoke import task

# Configuration
PYPIUSERNAME = "jdrumgoole"
ROOT = Path.home() / "GIT" / "pyimport"
PYTHONPATH = str(ROOT)

# Load environment variables from .env file
env_file = ROOT / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value


@task
def testenv(c):
    """Test environment variables"""
    audithost = os.environ.get('AUDITHOST', 'Not set')
    print(f'"AUDITHOST" is: "{audithost}"')


@task
def path(c):
    """Show AUDITHOST path"""
    audithost = os.environ.get('AUDITHOST', 'Not set')
    print(f"AUDITHOST={audithost}")


@task
def pythonpath(c):
    """Show Python path"""
    print(f"PYTHONPATH={PYTHONPATH}")


@task
def pgconfig(c):
    """Show PostgreSQL configuration from environment variables"""
    pghost = os.environ.get('PGHOST', 'Not set')
    pgport = os.environ.get('PGPORT', 'Not set')
    pgdatabase = os.environ.get('PGDATABASE', 'Not set')
    pguser = os.environ.get('PGUSER', 'Not set')
    print(f"PGHOST={pghost}")
    print(f"PGPORT={pgport}")
    print(f"PGDATABASE={pgdatabase}")
    print(f"PGUSER={pguser}")
    print("Credentials should be in ~/.pgpass")


@task
def root(c):
    """Show project root"""
    print(f"The project ROOT is '{ROOT}'")


@task
def python_bin(c):
    """Show Python binary info"""
    c.run('python -c "import os;print(os.environ.get(\'USERNAME\'))"')
    c.run('which python')


# Test tasks
@task
def std_quicktest(c):
    """Standard quick test"""
    with c.cd(ROOT):
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='stdout')


@task
def audit_quicktest(c):
    """Audit quick test"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --audit --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported')


@task
def async_quicktest(c):
    """Async quick test"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --audit --asyncpro --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt')
        c.run('poetry run python pyimport/pyimport_main.py --asyncpro --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='stdout')


@task
def thread_quicktest(c):
    """Thread quick test"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --audit --thread --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --asyncpro --thread --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='stdout')


@task
def multi_quicktest(c):
    """Multi-processing quick test"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --splitfile --multi --poolsize 2 --delimiter \'|\' --fieldfile ./test/test_mot/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --splitfile --multi --poolsize 2 --audit --delimiter \'|\' --fieldfile ./test/test_mot/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='stdout')


@task
def quick_test(c):
    """Run all quick tests"""
    std_quicktest(c)
    async_quicktest(c)
    thread_quicktest(c)
    multi_quicktest(c)


@task
def test_audit(c):
    """Test audit functionality"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --audit --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --asyncpro --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --multi --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --asyncpro --multi --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --threads --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --threads --asyncpro --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='stdout')


@task
def test_scripts(c):
    """Test basic script functionality"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py -h', hide='both')
        c.run('poetry run python pyimport/pyimport_main.py --delimiter \'|\' ./test/test_mot/10k.txt', hide='both')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python pyimport/pyimport_main.py --asyncpro --delimiter \'|\' ./test/test_mot/10k.txt', hide='both')
        c.run('poetry run python mdbutils/dbop.py --count PYIM.imported')
        c.run('poetry run python pyimport/pwc.py -h', hide='both')
        c.run('poetry run python pyimport/splitfile.py -h', hide='both')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported', hide='both')


@task
def test_data(c):
    """Test with data files"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --drop --multi --splitfile --autosplit 4 --fieldfile test/data/100k.tff --delimiter "|" --poolsize 2 test/data/100k.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported')


@task
def split_file(c):
    """Test file splitting"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --splitfile --fieldfile test/data/100k.tff --delimiter "|" test/data/100k.txt', hide='stdout')


@task
def test_yellowtrip(c):
    """Test with yellow trip data"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --audit --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv')
        c.run('poetry run python pyimport/pyimport_main.py --audit --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv')
        c.run('poetry run python pyimport/pyimport_main.py --audit --asyncpro --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --async ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv')
        c.run('poetry run python pyimport/pyimport_main.py --audit --asyncpro --splitfile --multi --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --async ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv')
        c.run('poetry run python pyimport/pyimport_main.py --audit --asyncpro --splitfile --threads --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported')
        c.run('rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff')


@task
def test_multi(c):
    """Test multi-processing"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --multi --splitfile --autosplit 10 --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff --poolsize 4 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv', hide='stdout')
        c.run('rm ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported')


@task
def test_threads(c):
    """Test threading"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --asyncpro --threads --poolsize 8 --splitfile --autosplit 8 --fieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.tff ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported')


@task
def test_small_multi(c):
    """Test with small multi-processing dataset"""
    with c.cd(ROOT):
        c.run('head -n 5000 ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv > yellow_tripdata_2015-01-06-5k.csv')
        c.run('poetry run python pyimport/splitfile.py --autosplit 2 yellow_tripdata_2015-01-06-5k.csv')
        c.run('poetry run python pyimport/pyimport_main.py --genfieldfile yellow_tripdata_2015-01-06-5k.csv')
        c.run('poetry run python pyimport/pyimport_main.py --database SMALL --collection yellowcab --splitfile --autosplit 2 --fieldfile yellow_tripdata_2015-01-06-5k.tff --poolsize 2 yellow_tripdata_2015-01-06-5k.csv', hide='stdout')
        c.run('rm yellow_tripdata_2015-01-06-5k.tff yellow_tripdata_2015-01-06-5k.csv')
        c.run('poetry run python mdbutils/dbop.py --drop SMALL.yellowcab')


@task
def genfieldfile(c):
    """Generate field file"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --genfieldfile ./test/test_splitfile/yellow_tripdata_2015-01-06-200k.csv', hide='stdout')


@task
def mongoimport(c):
    """Test MongoDB import functionality"""
    with c.cd(ROOT):
        c.run('mongoimport --db test --collection yellowcab --type csv --columnsHaveTypes --numInsertionWorkers=8 --fieldFile test/test_mongoimport/yellow_trip_data_10.mff --file test/test_mongoimport/yellow_tripdata_200_noheader.csv')
        c.run('poetry run python pyimport/pyimport_main.py --hasheader --forkmethod spawn --asyncpro --multi --splitfile --autosplit 10 --poolsize 8 --fieldfile ./test/test_command/yellow_trip.tff ./test/test_command/yellow_tripdata_2015-01-06-200k.csv')
        c.run('poetry run python mdbutils/dbop.py --drop PYIM.imported')
        c.run('poetry run python mdbutils/dbop.py --drop test.yellowcab')


@task
def missing_records(c):
    """Test missing records handling"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --keepsplits --splitfile --autosplit 10 --hasheader --fieldfile ./test/test_command/yellow_trip.tff ./test/test_command/yellow_tripdata_2015-01-06-200k.csv')


@task
def test_all_scripts(c):
    """Run all script tests"""
    test_scripts(c)
    test_audit(c)
    test_multi(c)
    test_small_multi(c)
    test_yellowtrip(c)
    test_data(c)


@task
def run_pytest(c):
    """Run pytest in all test directories"""
    # Run from within each test directory so tests can find their data files
    test_dirs = [
        'test/test_args',
        'test/test_command',
        'test/test_config',
        'test/test_e2e',
        'test/test_fieldfile',
        'test/test_file_processor',
        'test/test_filesplitter',
        'test/test_http_import',
        'test/test_linecounter',
        'test/test_linereader',
        'test/test_mot',
        'test/test_splitfile',
        'test/test_general',
        'test/test_formats',
        'test/test_db',
    ]
    # PostgreSQL config loaded from .env file (PGHOST, PGPORT, PGDATABASE, PGUSER)
    # Credentials should be in ~/.pgpass
    for test_dir in test_dirs:
        print(f"Running pytest in {test_dir}...")
        with c.cd(ROOT / test_dir):
            c.run('poetry run pytest', warn=True)


@task
def test_top(c):
    """Run pytest from test directory"""
    with c.cd(ROOT / 'test'):
        c.run('poetry run pytest')


@task
def test_all(c):
    """Run all tests"""
    full_pytest_parallel(c)
    test_all_scripts(c)


# Optimized test tasks
@task
def run_pytest_parallel(c):
    """Run pytest with parallel execution in all test directories"""
    # Run from within each test directory so tests can find their data files
    test_dirs = [
        'test/test_args',
        'test/test_command',
        'test/test_config',
        'test/test_e2e',
        'test/test_fieldfile',
        'test/test_file_processor',
        'test/test_filesplitter',
        'test/test_http_import',
        'test/test_linecounter',
        'test/test_linereader',
        'test/test_mot',
        'test/test_splitfile',
        'test/test_general',
        'test/test_formats',
        'test/test_db',
    ]
    # PostgreSQL config loaded from .env file (PGHOST, PGPORT, PGDATABASE, PGUSER)
    # Credentials should be in ~/.pgpass
    for test_dir in test_dirs:
        print(f"Running pytest (parallel) in {test_dir}...")
        with c.cd(ROOT / test_dir):
            c.run('poetry run pytest -n auto', warn=True)


@task
def quick_pytest(c):
    """Run pytest quickly (parallel execution, essential tests only)"""
    # Run only the fastest/most important test directories
    essential_dirs = [
        'test/test_command',
        'test/test_general',
        'test/test_e2e',
    ]

    print("Running essential pytest tests (parallel)...")
    # PostgreSQL config loaded from .env file (PGHOST, PGPORT, PGDATABASE, PGUSER)
    # Credentials should be in ~/.pgpass
    for test_dir in essential_dirs:
        print(f"Running pytest (parallel) in {test_dir}...")
        with c.cd(ROOT / test_dir):
            c.run('poetry run pytest -n auto -q', warn=True)


@task
def quick_test_scripts(c):
    """Run only fast integration scripts"""
    print("Running fast integration tests...")
    test_scripts(c)
    test_data(c)


@task
def quick_dev(c):
    """Quick development test cycle (essential tests only)"""
    print("=" * 60)
    print("QUICK DEVELOPMENT TEST CYCLE")
    print("=" * 60)
    quick_pytest(c)
    quick_test_scripts(c)
    print("\n✓ Quick development tests completed!")


@task
def full_pytest_parallel(c):
    """Run full pytest suite with parallel execution"""
    print("Running full pytest suite (parallel)...")
    run_pytest_parallel(c)


@task
def test_timing(c):
    """Show timing for slowest tests"""
    with c.cd(ROOT / 'test' / 'test_general'):
        print("Running timing analysis on test_general...")
        c.run('poetry run pytest --durations=20')


# GUI tests with Playwright
@task
def test_gui(c):
    """Run GUI tests with Playwright"""
    print("Running GUI tests...")
    with c.cd(ROOT):
        c.run('poetry run pytest test/test_gui -v -m gui')


@task
def test_gui_auth(c):
    """Run GUI authentication tests only"""
    print("Running GUI authentication tests...")
    with c.cd(ROOT):
        c.run('poetry run pytest test/test_gui/test_authentication.py -v')


@task
def test_gui_import(c):
    """Run GUI import tests only"""
    print("Running GUI import tests...")
    with c.cd(ROOT):
        c.run('poetry run pytest test/test_gui/test_import.py -v')


@task
def test_gui_fieldfile(c):
    """Run GUI field file tests only"""
    print("Running GUI field file tests...")
    with c.cd(ROOT):
        c.run('poetry run pytest test/test_gui/test_field_file.py -v')


@task
def test_gui_progress(c):
    """Run GUI progress monitoring tests only"""
    print("Running GUI progress monitoring tests...")
    with c.cd(ROOT):
        c.run('poetry run pytest test/test_gui/test_progress.py -v')


@task
def test_gui_headed(c):
    """Run GUI tests in headed mode (visible browser)"""
    print("Running GUI tests in headed mode...")
    with c.cd(ROOT):
        c.run('poetry run pytest test/test_gui -v -m gui --headed')


@task
def test_gui_install(c):
    """Install Playwright browsers for GUI testing"""
    print("Installing Playwright browsers...")
    with c.cd(ROOT):
        c.run('poetry run playwright install')


@task
def test_gui_install_chromium(c):
    """Install only Chromium browser for GUI testing"""
    print("Installing Chromium browser...")
    with c.cd(ROOT):
        c.run('poetry run playwright install chromium')


@task
def clean(c):
    """Clean build artifacts"""
    with c.cd(ROOT):
        c.run('rm -rf build dist')


@task
def build(c):
    """Build the package with full cross-version testing"""
    print("\n" + "=" * 60)
    print("BUILDING PACKAGE WITH FULL TEST SUITE")
    print("=" * 60)

    print("\n1. Running parallel pytest suite...")
    full_pytest_parallel(c)

    print("\n2. Running integration tests...")
    test_all_scripts(c)

    print("\n3. Running tox tests across Python 3.10-3.13...")
    tox_run(c)

    print("\n4. Building package with poetry...")
    with c.cd(ROOT):
        c.run('poetry build')

    print("\n" + "=" * 60)
    print("✓ Build completed successfully!")
    print("=" * 60)


@task
def poetry_build(c):
    """Build with poetry"""
    with c.cd(ROOT):
        c.run('poetry build')


@task
def poetry_publish(c):
    """Publish with poetry"""
    with c.cd(ROOT):
        c.run('poetry publish')


@task
def trigger_rtd_build(c):
    """Trigger Read the Docs build via webhook"""
    import requests

    # Read the Docs webhook token from environment
    rtd_token = os.environ.get('RTD_WEBHOOK_TOKEN')

    if not rtd_token:
        print("⚠️  RTD_WEBHOOK_TOKEN not found in environment")
        print("⚠️  To enable automatic docs rebuilds:")
        print("   1. Go to https://readthedocs.org/dashboard/pyimport/integrations/")
        print("   2. Create a Generic webhook")
        print("   3. Add RTD_WEBHOOK_TOKEN=<token> to .env file")
        print("⚠️  Skipping Read the Docs rebuild trigger")
        return

    # Generic webhook URL format: https://app.readthedocs.org/api/v2/webhook/{project-slug}/{integration-id}/
    # Using the generic API webhook integration ID 311422
    webhook_url = "https://app.readthedocs.org/api/v2/webhook/pyimport/311422/"

    try:
        print("Triggering Read the Docs rebuild...")
        # Generic webhooks require authentication via token parameter
        data = {
            'token': rtd_token
        }
        response = requests.post(webhook_url, data=data)

        if response.status_code in [200, 202]:
            print("✓ Read the Docs build triggered successfully!")
            try:
                data = response.json()
                build_id = data.get('build', {}).get('id', 'unknown')
                print(f"  Build ID: {build_id}")
                print(f"  View build: https://app.readthedocs.org/projects/pyimport/builds/{build_id}/")
            except:
                print("  Build queued (details unavailable)")
        else:
            print(f"✗ Failed to trigger RTD build: HTTP {response.status_code}")
            if response.text:
                print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"✗ Error triggering RTD build: {e}")


@task
def publish(c):
    """Build, tag, publish to PyPI, push git tag, and trigger Read the Docs rebuild"""
    # Get version from version.py
    import sys
    sys.path.insert(0, str(ROOT / 'pyimport'))
    from version import __VERSION__

    version_tag = f"v{__VERSION__}"

    # Build and test
    build(c)

    # Check if git tag already exists
    with c.cd(ROOT):
        result = c.run(f'git tag -l {version_tag}', hide=True, warn=True)
        tag_exists = bool(result.stdout.strip())

        if tag_exists:
            print(f"⚠️  Git tag {version_tag} already exists")
            response = input(f"Delete existing tag {version_tag} and create new one? (y/N): ")
            if response.lower() == 'y':
                c.run(f'git tag -d {version_tag}')
                c.run(f'git push origin :refs/tags/{version_tag}', warn=True)
                print(f"✓ Deleted existing tag {version_tag}")
            else:
                print("⚠️  Skipping git tag creation")
                tag_exists = False  # Don't try to push later

        # Create git tag
        if not tag_exists:
            print(f"Creating git tag {version_tag}...")
            c.run(f'git tag -a {version_tag} -m "Release {version_tag}"')
            print(f"✓ Created git tag {version_tag}")

        # Publish to PyPI
        print("Publishing to PyPI...")
        c.run('poetry publish')
        print("✓ Published to PyPI")

        # Push git tag to remote
        if not tag_exists:
            print(f"Pushing tag {version_tag} to remote...")
            c.run(f'git push origin {version_tag}')
            print(f"✓ Pushed tag {version_tag} to GitHub")

    # Trigger Read the Docs rebuild after successful publish
    trigger_rtd_build(c)

    print("\n" + "=" * 60)
    print(f"✓ Release {version_tag} published successfully!")
    print("=" * 60)
    print(f"  PyPI: https://pypi.org/project/pyimport/{__VERSION__}/")
    print(f"  GitHub: https://github.com/jdrumgoole/pyimport/releases/tag/{version_tag}")
    print(f"  Docs: https://pyimport.readthedocs.io/en/latest/")
    print("=" * 60)


# Documentation tasks
@task
def docs_clean(c):
    """Clean documentation build artifacts"""
    with c.cd(ROOT / 'docs'):
        c.run('rm -rf _build')
        print("✓ Documentation build artifacts cleaned")


@task
def docs_build(c):
    """Build documentation with Sphinx"""
    with c.cd(ROOT / 'docs'):
        c.run('poetry run sphinx-build -b html . _build/html')
        print("✓ Documentation built successfully")
        print(f"  Open: file://{ROOT}/docs/_build/html/index.html")


@task
def docs_serve(c):
    """Build and serve documentation locally"""
    docs_build(c)
    with c.cd(ROOT / 'docs' / '_build' / 'html'):
        print("Starting local web server on http://localhost:8000")
        print("Press Ctrl+C to stop")
        c.run('python -m http.server 8000')


# Tox tasks
@task
def tox_list(c):
    """List tox test environments"""
    with c.cd(ROOT):
        c.run('poetry run tox -l')


@task
def tox_run(c, env=None):
    """Run tox tests across Python versions

    Args:
        env: Optional specific environment to test (e.g., py310, py311, py312, py313)
    """
    with c.cd(ROOT):
        if env:
            print(f"Running tox for {env}...")
            c.run(f'poetry run tox -e {env}')
        else:
            print("Running tox for all environments (py310, py311, py312, py313)...")
            c.run('poetry run tox')


@task
def check_python_versions(c):
    """Check availability of Python versions required for tox"""
    required_versions = {
        'py310': 'python3.10',
        'py311': 'python3.11',
        'py312': 'python3.12',
        'py313': 'python3.13',
    }

    print("Checking required Python versions for tox:\n")
    all_found = True

    for env, python_cmd in required_versions.items():
        try:
            result = c.run(f'{python_cmd} --version', hide=True, warn=True)
            if result.ok:
                print(f"✓ {env}: {result.stdout.strip()}")
            else:
                print(f"✗ {env}: Not found")
                all_found = False
        except:
            print(f"✗ {env}: Not found")
            all_found = False

    print()
    if all_found:
        print("✓ All required Python versions are available!")
    else:
        print("⚠️  Some Python versions are missing.")
        print("   See PYENV_SETUP.md for installation instructions.")
        print("   Run: pyenv install 3.10.18 3.11.9 3.12.11 3.13.5")


@task
def all(c):
    """Full build process"""
    test_all(c)
    build(c)
    # Note: Removed test_build as it wasn't defined in the original
    print("Ace King, Check it out! A full build")