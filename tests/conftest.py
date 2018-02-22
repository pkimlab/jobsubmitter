import os.path as op

PATH = ':'.join([
    op.expanduser('~/anaconda/bin'),
    '/usr/local/sbin',
    '/usr/local/bin',
    '/usr/sbin',
    '/usr/bin',
    '/sbin',
    '/bin',
])


def pytest_addoption(parser):
    parser.addoption("--quick", action="store_true", help="Run only quick tests.")
