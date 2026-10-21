"""The timeline index the Data tab pages by: boundaries cut server-side, rows never sent."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from routers.runs import _parse_instant, _timeline_pages  # noqa: E402


def _windows(data: bytes, size: int):
    def read(offset):
        return data[offset:offset + size], len(data)
    return read


def test_parse_instant_reads_epoch_units_and_iso():
    assert _parse_instant("1700000000") == 1700000000 * 1000
    assert _parse_instant("1700000000000") == 1700000000000
    assert _parse_instant("1700000000000000") == 1700000000000
    assert _parse_instant("42") is None
    assert _parse_instant("2024-01-01T00:00:00Z") == 1704067200000
    assert _parse_instant("nan") is None
    assert _parse_instant("abc") is None


def test_pages_hold_page_size_rows_in_time_order_across_windows():
    rows = "".join(f"{1700000000 + i},{i}\n" for i in range(10))
    data = ("time,v\n" + rows).encode()
    # A window smaller than a row boundary forces carried partial lines.
    out = _timeline_pages(_windows(data, 17), ["time"], 4)
    assert out["time_column"] == "time"
    assert out["rows"] == 10
    assert out["pages"] == [
        {"min": "1700000000", "max": "1700000003"},
        {"min": "1700000004", "max": "1700000007"},
        {"min": "1700000008", "max": "1700000009"},
    ]


def test_unsorted_rows_are_paged_in_time_order():
    data = b"time,v\n1700000002,1\n1700000000,2\n1700000001,3\n"
    out = _timeline_pages(_windows(data, 1024), ["time"], 2)
    assert out["pages"] == [{"min": "1700000000", "max": "1700000001"},
                            {"min": "1700000002", "max": "1700000002"}]


def test_a_file_without_a_timestamp_column_has_no_pages():
    out = _timeline_pages(_windows(b"a,b\n1,2\n3,4\n", 1024), [], 10)
    assert out == {"time_column": None, "rows": 2, "pages": []}


def test_preferred_name_wins_over_other_time_like_columns():
    data = b"created,time\n1600000000,1700000000\n"
    assert _timeline_pages(_windows(data, 1024), ["time"], 5)["time_column"] == "time"
