import logging
import re
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

import pandas as pd
import pytds

from .connection import parse_conn_str, ConnectionStringError

def fetch_per_ledger_chunked(
    conn_str: str,
    sql_template: str,
    ledgers: List[str],
    from_date,
    to_date,
    max_workers: int = 5,
    retry_attempts: int = 1
) -> Dict[str, pd.DataFrame]:
    """
    Fetch data for each ledger in calendar-month chunks, in parallel.
    Returns a dict: {ledger_id: DataFrame (possibly empty if error)}
    """
    logging.info("Starting fetch_per_ledger_chunked for ledgers: %s", ledgers)
    results = {}

    def proc(lid: str) -> tuple[str, pd.DataFrame]:
        """
        Process a single ledger, fetch data chunked by month.
        Returns (ledger_id, DataFrame).
        Logs and returns empty DataFrame on any error.
        """
        logging.info("=== Processing ledger %s ===", lid)
        for attempt in range(1, retry_attempts+1):
            try:
                (server, port), database, user, password = parse_conn_str(conn_str)
                df_all = pd.DataFrame()
                with pytds.connect(server=server, database=database, user=user, password=password, port=port, as_dict=True) as conn:
                    cursor = conn.cursor()
                    current = from_date

                    while current <= to_date:
                        chunk_end = min(
                            current + relativedelta(months=1) - timedelta(seconds=1),
                            to_date
                        )
                        chunk_sql = "SET NOCOUNT ON;\n" + sql_template
                        chunk_sql = re.sub(
                            r"@StrLedgers\s*=\s*\'[^\']+\'",
                            f"@StrLedgers='{lid}'",
                            chunk_sql,
                            flags=re.IGNORECASE
                        )
                        chunk_sql = re.sub(
                            r"@FromDate\s*=\s*\'[^\']+\'",
                            f"@FromDate='{current:%d-%b-%Y %H:%M:%S}'",
                            chunk_sql,
                            flags=re.IGNORECASE
                        )
                        chunk_sql = re.sub(
                            r"@ToDate\s*=\s*\'[^\']+\'",
                            f"@ToDate='{chunk_end:%d-%b-%Y %H:%M:%S}'",
                            chunk_sql,
                            flags=re.IGNORECASE
                        )
                        logging.debug(
                            "Ledger %s: chunk %s → %s\nSQL: %s",
                            lid, current, chunk_end, chunk_sql
                        )

                        # Execute and collect all result-sets
                        cursor.execute(chunk_sql)
                        all_sets = []
                        while True:
                            if cursor.description:
                                rows = cursor.fetchall() or []
                                all_sets.append(rows)
                                logging.debug(
                                    "Ledger %s chunk %s→%s returned %d rows in this set",
                                    lid, current, chunk_end, len(rows)
                                )
                            if not cursor.nextset():
                                break

                        # Select the first non-empty set
                        for result_set in all_sets:
                            if result_set:
                                chunk_df = pd.DataFrame(result_set)
                                df_all = pd.concat([df_all, chunk_df], ignore_index=True)
                                break  # Only the first non-empty set

                        current = chunk_end + timedelta(seconds=1)

                logging.info("Ledger %s fetch complete. Rows: %d", lid, len(df_all))
                return lid, df_all

            except Exception as e:
                logging.error("Error fetching ledger %s (attempt %d): %s", lid, attempt, e)
                if attempt == retry_attempts:
                    return lid, pd.DataFrame()  # Return empty DataFrame on failure

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_lid = {executor.submit(proc, lid): lid for lid in ledgers}
        for future in as_completed(future_to_lid):
            lid, df = future.result()
            results[lid] = df

    # Fill missing entries for any ledger not processed (shouldn't happen, but safe)
    for lid in ledgers:
        if lid not in results:
            results[lid] = pd.DataFrame()

    logging.info("Completed fetch for all ledgers. Success: %d/%d", sum(len(df) > 0 for df in results.values()), len(ledgers))
    return results
