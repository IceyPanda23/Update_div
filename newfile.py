def update_div(localhost=None):
    """
    Update dividend tables and mark matched rows.
    """

    # ------------------------------------------------------------------ #
    # 1. Ensure required modules exist
    # ------------------------------------------------------------------ #
    import importlib
    import subprocess
    import sys
    from pathlib import Path

    def ensure_module(package_name, import_name=None):
        import_name = import_name or package_name
        try:
            importlib.import_module(import_name)
        except ImportError:
            print(f"🔍 '{import_name}' not found – installing '{package_name}'…")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

    for pkg in ("pandas", "sqlalchemy", "pyodbc", "openpyxl"):
        ensure_module(pkg)

    import pandas as pd
    from sqlalchemy import create_engine, text

    # ------------------------------------------------------------------ #
    # 2. Universal, friend-proof pager
    # ------------------------------------------------------------------ #
    def paginate_list(df, page_size=10, prompt="Select item"):
        """
        Generic console pager:
        ● df          : DataFrame – first column must contain the 'name' values
        ● page_size   : rows per page
        ● prompt      : heading shown above each page
        Returns the absolute DataFrame index (int) chosen, or None if user quits.
        """
        original_df = df.copy()
        filtered_df = df.copy()
        page = 0

        while True:
            total = len(filtered_df)
            if total == 0:
                print("❌ Nothing matches that search.")
                return None

            start = page * page_size
            end = min(start + page_size, total)
            print(f"\n▶ {prompt}  {start}–{end - 1} of {total - 1}")

            # Plain numbered list (no pandas dots!)
            for rel_idx, (abs_idx, row) in enumerate(filtered_df.iloc[start:end].iterrows()):
                print(f"{rel_idx:>3} │ {row.iloc[0]}")

            print("\n[Enter]=next | p=prev | n=index | s:term | r=reset | q=quit")
            cmd = input("Select: ").strip().lower()

            if cmd == "":
                if end >= total:
                    print("🔚 End of list.")
                else:
                    page += 1

            elif cmd == "p":
                if page == 0:
                    print("🔝 Already at first page.")
                else:
                    page -= 1

            elif cmd == "r":
                filtered_df = original_df.copy()
                page = 0
                print("🔄 Search reset.")

            elif cmd.startswith("s:"):
                term = cmd[2:].strip()
                if not term:
                    print("⚠️  Provide a search term after 's:'.")
                    continue
                filtered_df = original_df[
                    original_df.iloc[:, 0].str.lower().str.contains(term)
                ]
                page = 0
                print(f"🔍 Found {len(filtered_df)} match(es).")

            elif cmd == "q":
                return None

            elif cmd.isdigit():
                rel = int(cmd)
                abs_idx = page * page_size + rel
                if 0 <= abs_idx < total:
                    return filtered_df.index[abs_idx]
                print("❌ Invalid index.")

            else:
                print("❌ Unrecognised input.")

    # ------------------------------------------------------------------ #
    # 3. Main worker to apply updates
    # ------------------------------------------------------------------ #
    def run_updates(engine, db_name, table_name, div_list):
        """
        Execute the UPDATE statements on the chosen dividend tables.
        """
        success_list, failed_list = [], []

        with engine.begin() as connection:
            # 3.1  Pick payment method
            df_payment = pd.read_sql(
                f"""SELECT [Code],[Description]
                    FROM [{db_name}].[dbo].[DividendPaymentMethods]
                    ORDER BY Description;""",
                connection,
            )
            print("List of Payment Methods:")
            payment_idx = paginate_list(df_payment, prompt="Select payment method")
            if payment_idx is None:
                code_in = input("Enter Code manually or 'q' to quit: ").strip()
                if code_in.lower() == "q":
                    print("Exiting…")
                    return
                payment_code = int(code_in)
            else:
                payment_code = int(df_payment["Code"].iloc[payment_idx])

            # 3.2  Loop through dividend numbers
            for div_no in div_list:
                try:
                    div_no = int(div_no)
                except ValueError:
                    print(f"⚠️  Skipping non-numeric DividendNo '{div_no}'")
                    failed_list.append(div_no)
                    continue

                # --- build the two UPDATE statements dynamically ----------
                if div_no == 1:
                    div_table = "Dividend"
                else:
                    div_table = f"Dividend{div_no}"

                # Use different columns if in EABLDatabaseRegister
                if db_name != "EABLDatabaseRegister":
                    set_clause = (
                        "DividendPaymentDate = src.Date, "
                        "DividendPaymentMethodCode = :pc, "
                        "DividendPaid = 1"
                    )
                else:
                    set_clause = "DividendPaymentDate = src.Date, DividendPaid = 1"

                update_query = f"""
                    UPDATE {div_table}
                    SET {set_clause}
                    FROM {div_table} tgt
                    INNER JOIN {table_name} src
                        ON tgt.ShareholderNo = src.sno
                       AND tgt.DividendNo   = src.divno
                """

                update_mark = f"""
                    UPDATE {table_name}
                    SET matched = 1
                    FROM {table_name} src
                    INNER JOIN {div_table} tgt
                        ON src.sno   = tgt.ShareholderNo
                       AND src.divno = tgt.DividendNo
                """

                try:
                    # connection.execute(text(update_query), {"pc": payment_code})  # ← if you need rowcount
                    connection.execute(text(update_query), {"pc": payment_code})
                    connection.execute(text(update_mark))
                    success_list.append(div_no)
                except Exception as e:
                    print(f"❌ Failed on Dividend{div_no}: {e}")
                    failed_list.append(div_no)

        # 3.3  Summary
        print("\n✅ Successful Updates:", success_list or "None")
        print("❌ Failed Updates    :", failed_list or "None")

        # 3.4  Dump unmatched rows to XLSX
        df_out = pd.read_sql(
            f"""
            SELECT *
            FROM [{db_name}].[dbo].[{table_name}]
            WHERE matched IS NULL
            """,
            engine,
        )
        pay_name = (
            df_payment["Description"].iloc[payment_idx]
            if payment_idx is not None
            else str(payment_code)
        )
        out_path = f"./DIVS_NOT_UPDATED_{db_name}_{pay_name}.xlsx"
        df_out.to_excel(out_path, engine="openpyxl")
        print(f"📝 Unmatched rows exported to {out_path}")

    # ------------------------------------------------------------------ #
    # 4. Top-level flow: choose server → DB → table
    # ------------------------------------------------------------------ #
    try:
        # 4.1  Decide local vs remote
        if localhost is None:
            localhost = input("Use localhost SQL Server? (y/n): ").strip().lower() == "y"

        if localhost:
            engine = create_engine(
                "mssql+pyodbc://localhost/master?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )
            db_df = pd.read_sql(
                "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')",
                engine,
            )
            db_idx = paginate_list(db_df, prompt="Select database")
            if db_idx is None:
                db_name = input("Type database name (or 'q' to quit): ").strip()
                if db_name.lower() == "q":
                    return
            else:
                db_name = db_df.iloc[db_idx, 0]

            conn_str = (
                f"mssql+pyodbc://localhost/{db_name}"
                "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )

        else:
            creds_path = Path(__file__).resolve().parent / "server_credentials.csv"
            server_credentials = pd.read_csv(creds_path)

            print("Saved server IPs:")
            print(server_credentials["Server_ip"])
            pick = input("Pick number or type 'new': ").strip().lower()

            if pick == "new":
                ip = input("IP: ")
                user = input("Username: ")
                password = input("Password: ")
            else:
                idx = int(pick)
                ip = server_credentials["Server_ip"][idx]
                user = server_credentials["Username"][idx]
                password = server_credentials["Password"][idx]

            conn_str = (
                f"mssql+pyodbc://{user}:{password}@{ip}/master?"
                "Driver=ODBC+Driver+17+for+SQL+Server"
            )

            engine = create_engine(conn_str)
            db_df = pd.read_sql(
                "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')",
                engine,
            )
            db_idx = paginate_list(db_df, prompt="Select database")
            if db_idx is None:
                db_name = input("Type database name (or 'q' to quit): ").strip()
                if db_name.lower() == "q":
                    return
            else:
                db_name = db_df.iloc[db_idx, 0]

            conn_str = (
                f"mssql+pyodbc://{user}:{password}@{ip}/{db_name}?"
                "Driver=ODBC+Driver+17+for+SQL+Server"
            )

        # 4.2  Pick the working table inside that DB
        engine = create_engine(conn_str)
        df_tables = pd.read_sql(
            "SELECT TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE'",
            engine,
        )

        tbl_idx = paginate_list(df_tables, prompt="Connect tables")
        if tbl_idx is None:
            table_name = input("Type table name (or 'q' to quit): ").strip()
            if table_name.lower() == "q":
                return
        else:
            table_name = df_tables.iloc[tbl_idx, 0]

        # 4.3  Pull dividend numbers present in that table
        with engine.begin() as conn:
            div_list = [
                row[0]
                for row in conn.execute(
                    text(
                        f"SELECT DIVNO FROM [{db_name}].[dbo].[{table_name}] "
                        "GROUP BY DIVNO"
                    )
                )
                if row[0] is not None
            ]

        print("Dividend list found:", div_list)
        run_updates(engine, db_name, table_name, div_list)

    except Exception as exc:
        print(f"\n❌ Error: {exc}")
        input("\nPress any key to exit…")
    else:
        input("\n✔️  Completed – press any key to exit…")


# ---------------------------------------------------------------------- #
# Execute
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    update_div()