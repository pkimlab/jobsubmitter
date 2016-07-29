def pytest_addoption(parser):
    parser.addoption("--quick", action="store_true", help="Run only quick tests.")
