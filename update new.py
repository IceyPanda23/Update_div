# --------------------------------------------------------------
# update_div.py
# --------------------------------------------------------------
def update_div(localhost=None):
    import importlib
    import subprocess
    import sys
    from pathlib import Path

    def ensure_module(pkg, import_name=None):
        import_name = import_name or pkg
        try:
            importlib.import_module(import_name)
        except ImportError:
            print(f"Installing '{pkg}' …")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

    for pkg in ("pandas", "sqlalchemy", "pyodbc", "openpyxl"):
        ensure_module(pkg)

    import pandas as pd
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import ProgrammingError

    def paginate_list(df, page_size=10, prompt="Select item"):
        full, filt = df.copy(), df.copy()
        page = 0
        while True:
            total = len(filt)
            start, end = page * page_size, min((page + 1) * page_size, total)
            print(f"\n{prompt} {start}–{end - 1} of {total - 1}")
            for rel, (abs_i, row) in enumerate(filt.iloc[start:end].iterrows()):
                print(f"{rel:>3} │ {row.iloc[0]}")
            print("\n[Enter]=next | p=prev | n=index | s:term | r=reset | q=quit")
            cmd = input("Select: ").strip().lower()
            if cmd == "":
                page = page + 1 if end < total else page
            elif cmd == "p":
                page = max(page - 1, 0)
            elif cmd == "r":
                filt, page = full.copy(), 0
            elif cmd.startswith("s:"):
                term = cmd[2:].strip()
                filt, page = full[full.iloc[:, 0].str.lower().str.contains(term)], 0
            elif cmd == "q":
                return None
            elif cmd.isdigit():
                abs_idx = page * page_size + int(cmd)
                if 0 <= abs_idx < total:
                    return filt.index[abs_idx]
                print("Invalid index.")
            else:
                print("Unrecognized input.")

    def run_updates(engine, db_name, table_name, div_list):
        with engine.begin() as connection:
            existing_tables = pd.read_sql(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", connection
            )["TABLE_NAME"]

            union_parts = []
            for div_no in div_list:
                table = "Dividend" if div_no == 1 else f"Dividend{div_no}"
                if table in existing_tables.values:
                    union_parts.append(f"SELECT ShareholderNo, DividendNo FROM {table}")
                else:
                    print(f"Skipping missing table: {table}")

            union_sql = "\nUNION ALL\n".join(union_parts)

            matched_q = f"""
                SELECT src.*, 'Matched' AS __match__
                FROM [{db_name}].[dbo].[{table_name}] src
                INNER JOIN ({union_sql}) tgt
                    ON src.sno = tgt.ShareholderNo
                   AND src.divno = tgt.DividendNo
            """
            unmatched_q = f"""
                SELECT src.*, 'Unmatched' AS __match__
                FROM [{db_name}].[dbo].[{table_name}] src
                LEFT JOIN ({union_sql}) tgt
                    ON src.sno = tgt.ShareholderNo
                   AND src.divno = tgt.DividendNo
                WHERE tgt.ShareholderNo IS NULL
            """

            df_match = pd.read_sql(matched_q, connection)
            df_no = pd.read_sql(unmatched_q, connection)

            print(f"\n--- Matched Rows ({len(df_match)}) ---")
            print(df_match.to_string(index=False))
            print(f"\n--- Unmatched Rows ({len(df_no)}) ---")
            print(df_no.to_string(index=False))

            df_preview = pd.concat([df_match, df_no], ignore_index=True)
            prev_path = f"./DIVS_PREVIEW_{db_name}_{table_name}.xlsx"
            with pd.ExcelWriter(prev_path, engine="openpyxl") as writer:
                df_match.to_excel(writer, sheet_name="Matched", index=False)
                df_no.to_excel(writer, sheet_name="Unmatched", index=False)
                df_preview.to_excel(writer, sheet_name="All", index=False)
            print(f"\n📝 Preview saved to {prev_path}")

            go = input("Proceed with updates? (y/n): ").strip().lower()
            if go != "y":
                print("Aborted by user.")
                return

            pay_df = pd.read_sql(
                f"""SELECT Code, Description
                    FROM [{db_name}].[dbo].[DividendPaymentMethods]
                    ORDER BY Description""",
                connection,
            )
            print("Payment methods:")
            pay_idx = paginate_list(pay_df, prompt="Select payment method")
            if pay_idx is None:
                code_in = input("Enter Code manually or 'q' to quit: ").strip()
                if code_in.lower() == "q":
                    print("Exiting…")
                    return
                pay_code = int(code_in)
                pay_desc = str(pay_code)
            else:
                pay_code = int(pay_df.at[pay_idx, "Code"])
                pay_desc = pay_df.at[pay_idx, "Description"]

        succ, fail, update_stats = [], [], []

        with engine.begin() as connection:
            for div_no in div_list:
                div_tbl = "Dividend" if div_no == 1 else f"Dividend{div_no}"
                set_clause = (
                    "DividendPaymentDate = src.Date, DividendPaymentMethodCode = :pc, DividendPaid = 1"
                    if db_name != "EABLDatabaseRegister"
                    else "DividendPaymentDate = src.Date, DividendPaid = 1"
                )
                upd = f"""
                    UPDATE {div_tbl}
                    SET {set_clause}
                    FROM {div_tbl} tgt
                    INNER JOIN {table_name} src
                        ON tgt.ShareholderNo = src.sno
                       AND tgt.DividendNo = src.divno
                """
                mark = f"""
                    UPDATE {table_name}
                    SET matched = 1
                    FROM {table_name} src
                    INNER JOIN {div_tbl} tgt
                        ON src.sno = tgt.ShareholderNo
                       AND src.divno = tgt.DividendNo
                """
                try:
                    res_upd = connection.execute(text(upd), {"pc": pay_code})
                    connection.execute(text(mark))
                    count = res_upd.rowcount or 0
                    print(f"✅ {div_tbl}: {count} rows updated.")
                    succ.append(div_no)
                    update_stats.append((div_no, count))
                except ProgrammingError as e:
                    print(f"❌ {div_tbl} failed: {e.orig.args[1]}")
                    fail.append(div_no)

        print("\n✅ Updated:", succ or "None")
        print("❌ Failed :", fail or "None")

        if update_stats:
            print("\n📊 Rows affected per dividend:")
            width = max(len(str(d)) for d, _ in update_stats)
            for dn, rc in update_stats:
                print(f" Dividend{str(dn).ljust(width)} : {rc:>7} rows")
            print(" ────────────────────────────────")
            print(f" Total updated : {sum(rc for _, rc in update_stats)} rows")

        with engine.begin() as conn:
            df_out = pd.read_sql(
                f"""SELECT * FROM [{db_name}].[dbo].[{table_name}]
                    WHERE matched IS NULL""",
                conn,
            )
        out_path = f"./DIVS_NOT_UPDATED_{db_name}_{pay_desc}.xlsx"
        df_out.to_excel(out_path, engine="openpyxl")
        print(f"📄 Post-update unmatched rows → {out_path}")

    try:
        if localhost is None:
            localhost = input("Use localhost SQL Server? (y/n): ").lower() == "y"

        if localhost:
            engine = create_engine(
                "mssql+pyodbc://localhost/master?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )
            db_df = pd.read_sql(
                "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')", engine
            )
            idx = paginate_list(db_df, prompt="Select database")
            db_name = input("DB name (or 'q'): ").strip() if idx is None else db_df.iat[idx, 0]
            if db_name.lower() == "q":
                return
            conn_str = f"mssql+pyodbc://localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        else:
            creds_path = Path(__file__).resolve().parent / "server_credentials.csv"
            saved = pd.read_csv(creds_path)
            print("Saved servers:")
            print(saved["Server_ip"])
            sel = input("Pick number or 'new': ").strip().lower()
            if sel == "new":
                ip, user, pw = input("IP: "), input("Username: "), input("Password: ")
            else:
                rec = saved.iloc[int(sel)]
                ip, user, pw = rec["Server_ip"], rec["Username"], rec["Password"]
            engine = create_engine(
                f"mssql+pyodbc://{user}:{pw}@{ip}/master?Driver=ODBC+Driver+17+for+SQL+Server"
            )
            db_df = pd.read_sql(
                "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')", engine
            )
            idx = paginate_list(db_df, prompt="Select database")
            db_name = input("DB name (or 'q'): ").strip() if idx is None else db_df.iat[idx, 0]
            if db_name.lower() == "q":
                return
            conn_str = f"mssql+pyodbc://{user}:{pw}@{ip}/{db_name}?Driver=ODBC+Driver+17+for+SQL+Server"

        engine = create_engine(conn_str)
        tbl_df = pd.read_sql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", engine
        )
        tbl_idx = paginate_list(tbl_df, prompt="Connect tables")
        table_name = input("Table name (or 'q'): ").strip() if tbl_idx is None else tbl_df.iat[tbl_idx, 0]
        if table_name.lower() == "q":
            return

        with engine.begin() as conn:
            raw_divs = pd.read_sql(
                f"SELECT DISTINCT DIVNO FROM [{db_name}].[dbo].[{table_name}] WHERE DIVNO IS NOT NULL",
                conn,
            )["DIVNO"].tolist()

        print("Dividend list:", raw_divs)
        run_updates(engine, db_name, table_name, raw_divs)

    except Exception as exc:
        print(f"\n❌ Error: {exc}")
        input("\nPress any key to exit…")
    else:
        input("\n✔️ Done – press any key to exit…")


if __name__ == "__main__":
    update_div()
