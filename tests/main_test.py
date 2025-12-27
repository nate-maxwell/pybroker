from pathlib import Path

import pytest


if __name__ == "__main__":
    test_dir = Path(__file__).parent
    pytest.main(["-v", test_dir.as_posix()])
