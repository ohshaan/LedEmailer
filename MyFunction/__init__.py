import logging
import os
import json
import tempfile
import re
from datetime import datetime

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from shared.parser import extract_dates, extract_ledgers, SqlParseError
from shared.metadata import get_ledger_metadata
from shared.fetcher import fetch_per_ledger_chunked
from shared.excel_export import save_to_excel
from shared.emailer import send_email_with_excel, EmailSendError
from shared.connection import ConnectionStringError

# --- Key Vault / Secret setup at module level (for cold start cache) ---
_credential = DefaultAzureCredential()
_kv_url = os.getenv("KEYVAULT_URL", "https://ledgervaultdev.vault.azure.net/")
_kv_client = SecretClient(vault_url=_kv_url, credential=_credential)

def safe_get_secret(client, name, required=True, default=None):
    try:
        return client.get_secret(name).value
    except Exception as e:
        logging.error(f"Key Vault error for secret '{name}': {e}")
        if required:
            raise
        return default

_smtp_server   = safe_get_secret(_kv_client, "email-smtp-server", required=False)
_smtp_port     = int(safe_get_secret(_kv_client, "email-smtp-port", required=False, default=587))
_smtp_username = safe_get_secret(_kv_client, "email-username", required=False)
_smtp_password = safe_get_secret(_kv_client, "email-password", required=False)
_sql_template  = safe_get_secret(_kv_client, "sql-connection-template")  # Required!

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Ledger Report triggered.")

    try:
        body = req.get_json()
        logging.info("Incoming payload: %s", json.dumps(body, indent=2))
    except Exception as e:
        logging.error(f"Failed to parse JSON body: {e}")
        return func.HttpResponse("Invalid JSON body", status_code=400)

    # Validate required request fields
    for fld in ("sql_proc", "email_to"):
        if fld not in body or not body[fld]:
            return func.HttpResponse(f"Missing required field: {fld}", status_code=400)

    sql_proc = body["sql_proc"]
    email_to = body["email_to"].strip()
    currency = body.get("currency", "QAR")
    auth_code = req.params.get("code")
    if not auth_code:
        return func.HttpResponse("Missing function key in URL (?code=...)", status_code=400)

    # Parse parameters from SQL string (dates, ledgers)
    try:
        from_date, to_date = extract_dates(sql_proc)
        ledger_ids = extract_ledgers(sql_proc)
        if not ledger_ids:
            return func.HttpResponse("No ledgers specified in @StrLedgers parameter.", status_code=400)
    except SqlParseError as e:
        logging.error(f"SQL parameter parse error: {e}")
        return func.HttpResponse(f"SQL parameter parse error: {e}", status_code=400)

    # --- Per-request secrets (DB name per user) ---
    try:
        tpl = _sql_template
        dbnm = _kv_client.get_secret(f"db-map-{auth_code}").value
        conn_str = tpl.replace("{db}", dbnm)
    except Exception as e:
        logging.error(f"Key Vault (per-request) error: {e}")
        return func.HttpResponse(f"Key Vault error: {e}", status_code=500)

    # --- Fetch metadata for ledgers ---
    try:
        metadata = get_ledger_metadata(conn_str, ledger_ids)
    except Exception as e:
        logging.error(f"Ledger metadata fetch error: {e}")
        return func.HttpResponse(f"Ledger metadata fetch error: {e}", status_code=500)

    # --- Fetch data for each ledger (parallel, chunked) ---
    try:
        data_dict = fetch_per_ledger_chunked(
            conn_str=conn_str,
            sql_template=sql_proc,
            ledgers=ledger_ids,
            from_date=from_date,
            to_date=to_date,
            max_workers=8,  # Tune as needed
            retry_attempts=2
        )
    except (ConnectionStringError, Exception) as e:
        logging.error(f"Ledger data fetch error: {e}")
        return func.HttpResponse(f"Ledger data fetch error: {e}", status_code=500)

    # --- Build file name and subject using company name ---
    company_name = metadata[ledger_ids[0]].get("company_name", "Ledger")
    safe_company = re.sub(r'[\\/*?:"<>|]', "_", company_name)[:30]
    date_str = datetime.now().strftime("%d-%m-%Y")
    time_str = datetime.now().strftime("%H:%M")
    excel_filename = f"{safe_company}_LedgerReport_{date_str}.xlsx"
    excel_path = os.path.join(tempfile.gettempdir(), excel_filename)
    requested_at = datetime.now()

    # --- Export to Excel ---
    try:
        save_to_excel(
            data_dict=data_dict,
            out_path=excel_path,
            metadata=metadata,
            requested_by=email_to,
            from_date=from_date,
            to_date=to_date,
            currency=currency,
            requested_at=requested_at
        )
    except Exception as e:
        logging.error(f"Excel export error: {e}")
        if os.path.exists(excel_path):
            os.remove(excel_path)
        return func.HttpResponse(f"Excel export error: {e}", status_code=500)

    # --- Compose subject ---
    email_subject = (
        f"Ledger Report – {company_name} (Requested by: {email_to} on {date_str} {time_str})"
    )

    # --- Send email with Excel attachment ---
    try:
        send_email_with_excel(
            recipient=email_to,
            file_path=excel_path,
            metadata=metadata,
            requested_ledgers=ledger_ids,
            smtp_server=_smtp_server,
            smtp_port=_smtp_port,
            smtp_username=_smtp_username,
            smtp_password=_smtp_password,
            subject=email_subject,
            body=None,
            cleanup=True,  # Deletes the file after sending
            retries=2
        )
    except EmailSendError as e:
        logging.error(f"Email send error: {e}")
        return func.HttpResponse(f"Failed to send email: {e}", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected email error: {e}")
        return func.HttpResponse(f"Unexpected email error: {e}", status_code=500)
    finally:
        # Just in case cleanup did not occur
        if os.path.exists(excel_path):
            try:
                os.remove(excel_path)
            except Exception:
                pass

    logging.info("Ledger report generated and emailed successfully.")
    return func.HttpResponse(f"Report generated and sent to {email_to}.", status_code=200)
