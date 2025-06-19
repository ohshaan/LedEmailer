import logging
from typing import List, Dict
import pytds

from .connection import parse_conn_str, ConnectionStringError

def get_ledger_metadata(conn_str: str, ledger_ids: List[str]) -> Dict[str, Dict[str, str]]:
    """
    Fetch ledger code/name and company details for each ledger ID.
    Returns {ledger_id: {code, name, company_name, company_address}, ...}
    If a requested ledger ID is not found, the result contains an empty dict for that ID.
    """
    logging.info(f"Fetching metadata for ledgers: {ledger_ids}")
    meta: Dict[str, Dict[str, str]] = {}
    if not ledger_ids:
        logging.warning("No ledger IDs provided for metadata fetch.")
        return meta

    # --- Parameterized query to avoid SQL injection risks ---
    placeholders = ','.join(['%s'] * len(ledger_ids))
    query = f"""
    SELECT DISTINCT
      d.Alm_ID_N      AS LedgerID,
      d.Ald_Code_V    AS code,
      d.Ald_Name_V    AS name,
      c.Cmp_Name_V    AS company_name,
      c.Cmp_Address_V AS company_address
    FROM dbo.Fin_AccountLedger_Dtl AS d
    INNER JOIN dbo.Fin_AccountLedger_Mst AS m ON d.Alm_ID_N = m.Alm_ID_N
    LEFT JOIN dbo.Adm_Company_Mst AS c ON m.Cmp_ID_N = c.Cmp_ID_N
    WHERE d.Alm_ID_N IN ({placeholders})
    """

    (svr, prt), db, usr, pwd = parse_conn_str(conn_str)
    try:
        with pytds.connect(server=svr, database=db, user=usr, password=pwd, port=prt) as conn:
            cur = conn.cursor()
            logging.debug("Executing metadata query: %s", query)
            cur.execute(query, tuple(int(l) for l in ledger_ids))

            cols = [col[0] for col in cur.description]
            found_ids = set()

            for row in cur.fetchall():
                rec = dict(zip(cols, row))
                lid = str(rec['LedgerID'])
                meta[lid] = {
                    'code':            rec['code'],
                    'name':            rec['name'],
                    'company_name':    rec.get('company_name') or '',
                    'company_address': rec.get('company_address') or ''
                }
                found_ids.add(lid)

            # Fill in any missing ledger IDs with empty dicts
            missing = set(str(l) for l in ledger_ids) - found_ids
            if missing:
                logging.warning(f"Metadata not found for ledgers: {missing}")
                for mid in missing:
                    meta[mid] = {}

        logging.info(f"Fetched metadata for {len(meta)} ledgers (requested: {len(ledger_ids)}).")
        return meta

    except ConnectionStringError as ce:
        logging.error(f"Connection string error: {ce}")
        raise
    except Exception as e:
        logging.error(f"Metadata DB fetch error: {e}")
        # Optionally, re-raise or return what we got so far
        raise
