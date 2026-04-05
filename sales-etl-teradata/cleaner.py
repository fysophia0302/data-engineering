"""
cleaner.py
==========
Special-character scrubbing for Teradata LATIN character set compatibility.
"""

import re
import pandas as pd

CHAR_REPLACE_MAP = [
    # Smart / curly quotes
    ("\u2018", "'"), ("\u2019", "'"), ("\u201A", "'"), ("\u201B", "'"),
    ("\u201C", '"'), ("\u201D", '"'), ("\u201E", '"'), ("\u201F", '"'),
    # Dashes
    ("\u2013", "-"), ("\u2014", "-"), ("\u2015", "-"),
    # Spaces & invisible characters
    ("\u00A0", " "), ("\u200B", ""), ("\u200C", ""), ("\u200D", ""), ("\uFEFF", ""),
    # Punctuation
    ("\u2026", "..."), ("\u2022", "*"), ("\u2023", "*"), ("\u00B7", "*"),
    # Symbols
    ("\u00AE", "(R)"), ("\u00A9", "(C)"), ("\u2122", "(TM)"),
    # Fractions & math
    ("\u00BC", "1/4"), ("\u00BD", "1/2"), ("\u00BE", "3/4"),
    ("\u00B0", " degrees"), ("\u00B1", "+/-"), ("\u00D7", "x"), ("\u00F7", "/"),
    # Accented lowercase
    ("\u00E9", "e"), ("\u00E8", "e"), ("\u00EA", "e"), ("\u00EB", "e"),
    ("\u00E0", "a"), ("\u00E2", "a"), ("\u00E4", "a"), ("\u00E6", "ae"),
    ("\u00F9", "u"), ("\u00FB", "u"), ("\u00FC", "u"),
    ("\u00EE", "i"), ("\u00EF", "i"), ("\u00F4", "o"), ("\u00F6", "o"),
    ("\u00E7", "c"), ("\u00F1", "n"),
    # Accented uppercase
    ("\u00C9", "E"), ("\u00C0", "A"), ("\u00C2", "A"), ("\u00C4", "A"),
    ("\u00C6", "AE"), ("\u00D6", "O"), ("\u00DC", "U"),
    ("\u00C7", "C"), ("\u00D1", "N"),
]


def clean_cell(val) -> str:
    """Replace known special characters with ASCII equivalents.

    Non-printable characters that survive the map are stripped entirely.
    NaN values are returned unchanged.
    """
    if pd.isna(val):
        return val
    s = str(val)
    for src, tgt in CHAR_REPLACE_MAP:
        if src in s:
            s = s.replace(src, tgt)
    return re.sub(r'[^\x20-\x7E]', '', s)


def scan_special_chars(df: pd.DataFrame) -> dict:
    """Return a dict of special characters found and their occurrence counts."""
    hits = {}
    for src, _ in CHAR_REPLACE_MAP:
        mask  = df.apply(lambda col: col.astype(str).str.contains(re.escape(src), na=False))
        count = int(mask.values.sum())
        if count:
            hits[repr(src)] = count
    return hits


def apply_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Apply clean_cell to every cell in the dataframe."""
    return df.applymap(clean_cell)
