from __future__ import annotations

import io
import sys
import zipfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import main as main_module  # noqa: E402


def build_zip(entries: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def test_parse_ohlc_csv_payload_basic():
    csv_content = (
        "symbol,date,open,high,low,close,volume\n"
        "CDR,2024-01-02,10,11,9,10.5,12345\n"
        "CDR,2024-01-03,10.5,11.5,10,11,15000\n"
    ).encode("utf-8")

    payload, skipped, errors, total_errors = main_module._parse_ohlc_csv_payload(csv_content)

    assert skipped == 0
    assert errors == []
    assert total_errors == 0
    assert payload == [
        ["CDR", date(2024, 1, 2), 10.0, 11.0, 9.0, 10.5, 12345.0],
        ["CDR", date(2024, 1, 3), 10.5, 11.5, 10.0, 11.0, 15000.0],
    ]


def test_parse_mst_archive_payload_merges_files():
    archive_bytes = build_zip(
        {
            "CDR_d.MST": (
                "DATA;OTWARCIE;NAJWYZSZY;NAJNIZSZY;ZAMKNIECIE;WOLUMEN\n"
                "2024-01-02;10,5;11,0;10,1;10,8;12345\n"
            ),
            "PKN.MST": "PKN;20240103;100;110;95;105;123\n",
            "AUTOPARTN.MST": "20240104;1;2;3;4;5\n",
        }
    )

    payload, skipped, errors, total_errors = main_module._parse_mst_archive_payload(archive_bytes)

    assert skipped == 0
    assert errors == []
    assert total_errors == 0

    expected = {
        ("CDR", date(2024, 1, 2)): [10.5, 11.0, 10.1, 10.8, 12345.0],
        ("PKN", date(2024, 1, 3)): [100.0, 110.0, 95.0, 105.0, 123.0],
        ("AUTOPARTN", date(2024, 1, 4)): [1.0, 2.0, 3.0, 4.0, 5.0],
    }
    assert { (row[0], row[1]): row[2:] for row in payload } == expected


def test_parse_mst_archive_payload_handles_placeholder_headers():
    archive_bytes = build_zip(
        {
            "PATENTUS.MST": (
                "<DTYYYYMMDD>,<HISOPEN>,<HISHIGH>,<HISLOW>,<HISCLOSE>,<VOL>\n"
                "20241028,1.1,2.2,0.9,1.5,1234\n"
            )
        }
    )

    payload, skipped, errors, total_errors = main_module._parse_mst_archive_payload(
        archive_bytes
    )

    assert skipped == 0
    assert errors == []
    assert total_errors == 0
    assert payload == [
        ["PATENTUS", date(2024, 10, 28), 1.1, 2.2, 0.9, 1.5, 1234.0]
    ]


def test_parse_mst_archive_payload_reports_missing_symbol():
    archive_bytes = build_zip(
        {
            "@@@.mst": (
                "DATA;OTWARCIE;NAJWYZSZY;NAJNIZSZY;ZAMKNIECIE\n"
                "2024-01-02;10;11;9;10\n"
            )
        }
    )

    payload, skipped, errors, total_errors = main_module._parse_mst_archive_payload(archive_bytes)

    assert payload == []
    assert skipped == 1
    assert total_errors == 1
    assert any("@@@.mst" in message for message in errors)
