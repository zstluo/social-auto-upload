from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.files_times import get_absolute_path
from conf import BASE_DIR


def test_get_absolute_path_relative():
    result = get_absolute_path("test.json", "ks_uploader")
    expected = str(Path(BASE_DIR) / "cookies" / "ks_uploader" / "test.json")
    assert result == expected


def test_get_absolute_path_absolute(tmp_path):
    absolute = tmp_path / "my.json"
    result = get_absolute_path(str(absolute), "ks_uploader")
    assert result == str(absolute)
