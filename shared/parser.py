import re
import logging
from datetime import datetime
from typing import Tuple, List, Optional

# Supported date formats for SQL procedure parsing
_DATE_FORMATS = [
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%Y",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S"
]

class SqlParseError(Exception):
    """Custom exception for errors parsing SQL parameters."""
    pass

def extract_dates(sql_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Extracts @FromDate and @ToDate values from a SQL string.

    Returns:
        (from_date, to_date) as datetime if both found and parsed; otherwise (None, None).
    Raises:
        SqlParseError if dates are missing or invalid.
    """
    logging.info("Extracting @FromDate and @ToDate from SQL template.")
    from_match = re.search(r"@FromDate\s*=\s*\'([^\']+)\'", sql_str, re.IGNORECASE)
    to_match   = re.search(r"@ToDate\s*=\s*\'([^\']+)\'", sql_str, re.IGNORECASE)

    if not from_match or not to_match:
        msg = "Failed to find @FromDate or @ToDate in SQL string."
        logging.error(msg)
        raise SqlParseError(msg)

    def _parse(ds: str) -> Optional[datetime]:
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(ds, fmt)
            except ValueError:
                continue
        logging.warning(f"Unable to parse date string '{ds}' with supported formats.")
        return None

    from_date = _parse(from_match.group(1))
    to_date   = _parse(to_match.group(1))

    if from_date is None or to_date is None:
        msg = f"Parsed dates contain None: from_date={from_date}, to_date={to_date}"
        logging.error(msg)
        raise SqlParseError(msg)

    if from_date > to_date:
        msg = f"@FromDate ({from_date}) is after @ToDate ({to_date})."
        logging.error(msg)
        raise SqlParseError(msg)

    logging.debug("Extracted date range: %s to %s", from_date, to_date)
    return from_date, to_date

def extract_ledgers(sql_str: str, strict: bool = True) -> List[str]:
    """
    Extracts a comma-separated list of ledger IDs from @StrLedgers in the SQL string.

    Args:
        sql_str (str): The SQL string.
        strict (bool): If True, raise error on non-numeric IDs. If False, filter/warn.

    Returns:
        A list of ledger IDs (as strings).
    Raises:
        SqlParseError if strict and invalid ledger IDs found.
    """
    logging.info("Extracting @StrLedgers from SQL template.")
    match = re.search(r"@StrLedgers\s*=\s*\'([^\']+)\'", sql_str, re.IGNORECASE)
    if not match:
        msg = "@StrLedgers parameter not found in SQL string."
        logging.warning(msg)
        return []

    raw = match.group(1)
    ledgers = [entry.strip() for entry in raw.split(',') if entry.strip()]
    invalid = [l for l in ledgers if not re.fullmatch(r"\d+", l)]

    if invalid:
        msg = f"Found non-numeric ledger IDs in @StrLedgers: {invalid}"
        if strict:
            logging.error(msg)
            raise SqlParseError(msg)
        else:
            logging.warning(msg)
            ledgers = [l for l in ledgers if l not in invalid]

    logging.debug("Extracted ledgers: %s", ledgers)
    return ledgers
