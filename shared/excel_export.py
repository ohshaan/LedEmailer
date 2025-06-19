import logging
import re
import pandas as pd
from datetime import datetime

def filter_opening_closing(out, opening_re, closing_re):
    """
    Only keep the *first* 'Opening Balance' and the *last* 'Closing Balance' row.
    All other such rows are dropped.
    """
    is_opening = out['Voucher Number'].str.contains(opening_re, na=False)
    is_closing = out['Voucher Number'].str.contains(closing_re, na=False)
    open_idx = out[is_opening].index
    close_idx = out[is_closing].index

    first_open = out.loc[[open_idx[0]]] if len(open_idx) else pd.DataFrame(columns=out.columns)
    last_close = out.loc[[close_idx[-1]]] if len(close_idx) else pd.DataFrame(columns=out.columns)
    # Everything else is not opening or closing
    middle = out[~(is_opening | is_closing)]
    # Combine (reset_index to keep order clean)
    cleaned = pd.concat([first_open, middle, last_close], ignore_index=True)
    return cleaned

def save_to_excel(
    data_dict,
    out_path,
    metadata,
    requested_by,
    from_date,
    to_date,
    currency,
    requested_at  # <-- add this
):

    logging.info(f"Saving Excel to {out_path} for ledgers: {list(data_dict.keys())}")

    opening_re = re.compile(r'opening\s*balance', re.IGNORECASE)
    closing_re = re.compile(r'closing\s*balance', re.IGNORECASE)
    ledger_re  = re.compile(r'(?i)^(ledger[\s._]*code|ledger[\s._]*name)$')
    slno_re    = re.compile(r'(?i)^\s*sl[\s._]*no\s*\.?$', re.IGNORECASE)

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        workbook = writer.book

        # Formatting
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center'})
        info_fmt  = workbook.add_format({'italic': True, 'align': 'center'})
        hdr_fmt   = workbook.add_format({
            'bold': True,
            'bg_color': '#D7E4BC',
            'border': 1,
            'align': 'center'
        })

        used_sheet_names = set()
        for lid, df in data_dict.items():
            m = metadata[lid]
            name = re.sub(r'[\\/*?:"<>|]', "", m.get("name", ""))[:31]
            orig_name = name
            i = 1
            while name in used_sheet_names:
                # Excel sheet names must be <=31 characters
                name = (orig_name[:28] + f"_{i}") if len(orig_name) > 28 else f"{orig_name}_{i}"
                i += 1
            used_sheet_names.add(name)
            sheet_name = name

            company = m.get("company_name", "")
            address = m.get("company_address", "")
            code = m.get("code", lid)

            out = df.copy()
            if "Voucher Date" in out.columns:
                out["Voucher Date"] = pd.to_datetime(out["Voucher Date"], errors="coerce").dt.strftime("%d-%m-%Y")

            # Remove duplicate opening/closing
            out = filter_opening_closing(out, opening_re, closing_re)

            # Drop unwanted columns
            out = out.drop(columns=[c for c in out.columns if ledger_re.match(c)], errors='ignore')
            out = out.drop(columns=[c for c in out.columns if slno_re.match(c)], errors='ignore')

            # Add Sl.No for real vouchers only
            sl_no = [''] * len(out)
            is_valid = ~(out['Voucher Number'].str.contains(opening_re, na=False)) & ~(out['Voucher Number'].str.contains(closing_re, na=False))
            seq = 1
            for idx, v in enumerate(is_valid):
                if v:
                    sl_no[idx] = seq
                    seq += 1
            out.insert(0, 'Sl.No', sl_no)

            out.to_excel(writer, sheet_name=sheet_name, startrow=7, index=False, header=False)
            ws = writer.sheets[sheet_name]

            num_cols = len(out.columns)
            # Header
            ws.merge_range(0, 0, 0, num_cols-1, company, title_fmt)
            ws.merge_range(1, 0, 1, num_cols-1, address, info_fmt)
            ws.merge_range(2, 0, 2, num_cols-1, f"Ledger: {code} - {name}", info_fmt)
            ws.merge_range(3, 0, 3, num_cols-1, f"Period: {from_date:%d-%b-%Y} to {to_date:%d-%b-%Y}   Currency: {currency}", info_fmt)
            request_dt = datetime.now().strftime("%d-%b-%Y %H:%M")
            ws.merge_range(4, 0, 4, num_cols-1, f"Requested by: {requested_by} at {request_dt}", info_fmt)

            # Column headers
            for col_num, value in enumerate(out.columns.values):
                ws.write(6, col_num, value, hdr_fmt)
            ws.freeze_panes(7, 0)

            for i, col in enumerate(out.columns):
                col_data = out[col].astype(str)
                max_len = max(col_data.map(len).max(), len(col)) + 2
                ws.set_column(i, i, min(max_len, 40))

            # --- Summary Block ---
            opening_row = out.loc[out['Voucher Number'].str.contains(opening_re, na=False)]
            closing_row = out.loc[out['Voucher Number'].str.contains(closing_re, na=False)]
            middle_rows = out[
                ~(out['Voucher Number'].str.contains(opening_re, na=False)) &
                ~(out['Voucher Number'].str.contains(closing_re, na=False))
            ]

            def safe_sum(series): return float(series.sum()) if not series.empty else 0.0
            opening_bal = safe_sum(opening_row['Debit']) if 'Debit' in opening_row else 0.0
            total_debit = safe_sum(middle_rows['Debit']) if 'Debit' in middle_rows else 0.0
            total_credit = safe_sum(middle_rows['Credit']) if 'Credit' in middle_rows else 0.0
            closing_bal = safe_sum(closing_row['Debit']) if 'Debit' in closing_row else opening_bal
            entries = len(middle_rows)

            lbl_fmt = workbook.add_format({'bold': True, 'border': 1})
            val_fmt = workbook.add_format({'border': 1, 'num_format': '#,##0.00', 'align': 'right'})

            # Summary block (bottom right)
            summary_row = 8 + len(out)
            col0 = max(0, num_cols - 2)
            col1 = max(0, num_cols - 1)

            summary_labels = [
                "Opening Balance :",
                "Total Debit      :",
                "Total Credit     :",
                "Number of Entries:",
                "Closing Balance  :"
            ]
            summary_values = [opening_bal, total_debit, total_credit, entries, closing_bal]

            for idx, (lbl, val) in enumerate(zip(summary_labels, summary_values)):
                ws.write(summary_row + idx, col0, lbl, lbl_fmt)
                ws.write_number(summary_row + idx, col1, val, val_fmt)

            # Footer
            footer_ts = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
            ws.set_footer(f"&CPage &P of &N&R{footer_ts}  {requested_by}")

    logging.info(f"Excel file saved: {out_path}")
