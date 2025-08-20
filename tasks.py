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
def pguri(c):
    """Show PostgreSQL URI"""
    pguri = os.environ.get('PGURI', 'Not set')
    print(f"PGURI={pguri}")


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
        c.run('poetry run python mdbutils/dbopy.py--drop PYIM.imported', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --audit --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbopy.py--count PYIM.imported')
        c.run('poetry run python mdbutils/dbopy.py--drop PYIM.imported', hide='stdout')


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
        c.run('poetry run python mdbutils/dbopy.py--drop PYIM.imported', hide='stdout')


@task
def thread_quicktest(c):
    """Thread quick test"""
    with c.cd(ROOT):
        c.run('poetry run python pyimport/pyimport_main.py --audit --thread --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python pyimport/pyimport_main.py --asyncpro --thread --delimiter \'|\' --fieldfile ./test/test_command/10k.tff ./test/test_command/120lines.txt', hide='stdout')
        c.run('poetry run python mdbutils/dbop.py -count PYIM.imported')
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
    test_dirs = [
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
    ]

    with c.cd(ROOT):
        for test_dir in test_dirs:
            print(f"Running tests in {test_dir}")
            with c.cd(test_dir):
                c.run('poetry run pytest')

        # Special case for test_db with PGURI environment variable
        print("Running tests in test/test_db")
        with c.cd('test/test_db'):
            pguri = os.environ.get('PGURI', '')
            c.run(f'PGURI={pguri} poetry run pytest')


@task
def test_top(c):
    """Run pytest from test directory"""
    with c.cd(ROOT / 'test'):
        c.run('poetry run pytest')


@task
def test_all(c):
    """Run all tests"""
    run_pytest(c)
    test_all_scripts(c)


@task
def clean(c):
    """Clean build artifacts"""
    with c.cd(ROOT):
        c.run('rm -rf build dist')


@task
def build(c):
    """Build the package"""
    test_all(c)
    with c.cd(ROOT):
        c.run('poetry build')


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
def publish(c):
    """Build and publish"""
    build(c)
    with c.cd(ROOT):
        c.run('poetry publish')


@task
def all(c):
    """Full build process"""
    test_all(c)
    build(c)
    # Note: Removed test_build as it wasn't defined in the original
    print("Ace King, Check it out! A full build")