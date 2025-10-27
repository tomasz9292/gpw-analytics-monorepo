from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from api.symbols import normalize_input_symbol


def test_normalize_keeps_known_raw_symbol():
    assert normalize_input_symbol("KGHM") == "KGHM"


def test_normalize_maps_short_symbol_to_canonical():
    assert normalize_input_symbol("CDR") == "CDPROJEKT"


def test_normalize_maps_wa_alias_to_canonical():
    assert normalize_input_symbol("KGH.WA") == "KGHM"


def test_normalize_handles_canonical_with_suffix():
    assert normalize_input_symbol("CDPROJEKT.WA") == "CDPROJEKT"
