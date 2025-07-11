def update_div(localhost=None):
    import importlib
    import subprocess
    import sys
    from pathlib import Path


    def ensure_module(package_name, import_name=None):
        import_name = import_name or package_name
        try:
            importlib.import_module(import_name)
        except ImportError:
            print(f"\U0001F50D '{import_name}' not found. Installing '{package_name}'...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

    # Ensure required packages are installed
    ensure_module('pandas')
    ensure_module('sqlalchemy')
    ensure_module('pyodbc')

    import pandas as pd
    from sqlalchemy import create_engine, text

    def paginate_table_list(table_df, page_size=10):
        full_df = table_df.copy()
        filtered_df = table_df.copy()
        current_page = 0

        while True:
            total = len(filtered_df)
            start = current_page * page_size
            end = min(start + page_size, total)

            print(f" \nPLEASE PICK TABLE NAME(SNO,DIVNO,DATE)\nShowing tables {start} to {end - 1} of {total - 1}:")
            print(filtered_df.iloc[start:end].reset_index(drop=True))

            print("\nOptions:")
            print(" - Press [Enter] for next page")
            print(" - Type 'p' for previous page")
            print(" - Type index (e.g. 3) to select a table")
            print(" - Type 's:<search_term>' to filter table names")
            print(" - Type 'r' to reset search")
            print(" - Type 'q' to quit/new table")

            user_input = input("Select: ").strip().lower()

            if user_input == '':
                if end >= total:
                    print("\U0001F51A You're at the end.")
                else:
                    current_page += 1

            elif user_input == 'p':
                if current_page == 0:
                    print("\U0001F51D You're already at the first page.")
                else:
                    current_page -= 1

            elif user_input == 'r':
                filtered_df = full_df.copy()
                current_page = 0
                print("\U0001F504 Search reset.")

            elif user_input.startswith('s:'):
                term = user_input[2:].strip()
                if not term:
                    print("⚠️ Please provide a search term after 's:'.")
                    continue
                filtered_df = full_df[full_df['TABLE_NAME'].str.lower().str.contains(term)]
                current_page = 0
                print(f"\U0001F50D Found {len(filtered_df)} matching tables.")

            elif user_input == 'q':
                return None

            elif user_input.isdigit():
                index = int(user_input)
                absolute_index = current_page * page_size + index
                if 0 <= absolute_index < total:
                    return filtered_df.index[absolute_index]
                else:
                    print("❌ Invalid index selected.")
            else:
                print("❌ Unrecognized input.")

    def run_updates(engine, db_name, table_name, div_list):
        success_list = []
        failed_list = []

        with engine.begin() as connection:
            # payment_path = Path(__file__).resolve().parent / "PaymentMethodCodes.xlsx"
            query_pc = f"""  SELECT [Code],[Description]
                            FROM [{db_name}].[dbo].[DividendPaymentMethods]
                            ORDER BY Description;
                        """
            df_payment = pd.read_sql(query_pc,connection)
            print("List of Payment Method Code")
            Payment_code_index =  paginate_table_list(df_payment)
            Payment_code = None
            if Payment_code_index is not None:
                Payment_code = int(df_payment['Code'].iloc[Payment_code_index])
            else:
                Payment_code = input("Please type the code manually or press q to quit")
                if Payment_code == 'q':
                    print("Exiting...")
                    return
                else:
                    Payment_code = int(Payment_code)
            for div_no in div_list:
                div_no = int(div_no)
                try:
                    if not str(div_no).isdigit():
                        print(f"⚠️ Skipping invalid DividendNo '{div_no}' (non-numeric)")
                        failed_list.append(div_no)
                        continue
                    if div_no == 1:
                        if db_name != "EABLDatabaseRegister":
                            update_query = f"""
                                            UPDATE Dividend
                                            SET DividendPaymentDate = {table_name}.Date, DividendPaymentMethodCode = {Payment_code}, DividendPaid = 1
                                            FROM Dividend INNER JOIN
                                            {table_name} ON Dividend.ShareholderNo = {table_name}.sno AND Dividend.DividendNo = {table_name}.divno
                                            """

                            update_query_2 = f"""
                                UPDATE {table_name}
                                SET matched = 1
                                FROM {table_name} INNER JOIN
                                Dividend ON {table_name}.sno = Dividend.Shareholderno AND {table_name}.divno = Dividend.DividendNo
                            """
                        else:
                            update_query = f"""
                                UPDATE Dividend
                                SET DividendPaymentDate = {table_name}.Date, DividendPaid = 1
                                FROM Dividend INNER JOIN
                                {table_name} ON Dividend.ShareholderNo = {table_name}.sno AND Dividend.DividendNo = {table_name}.divno
                                """

                            update_query_2 = f"""
                                UPDATE {table_name}
                                SET matched = 1
                                FROM {table_name} INNER JOIN
                                Dividend ON {table_name}.sno = Dividend.Shareholderno AND {table_name}.divno = Dividend.DividendNo
                            """
#                         update_que
                    else:
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# JOSHUA EDIT HEREEEEE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                        if db_name != "EABLDatabaseRegister":
                            update_query = f"""
                                                UPDATE Dividend{div_no}
                                                SET DividendPaymentDate = {table_name}.Date, DividendPaymentMethodCode = {Payment_code}, DividendPaid = 1
                                                FROM Dividend{div_no} INNER JOIN
                                                {table_name} ON Dividend{div_no}.ShareholderNo = {table_name}.sno AND Dividend{div_no}.DividendNo = {table_name}.divno
                                            """

                            update_query_2 = f"""
                                UPDATE {table_name}
                                SET matched = 1
                                FROM {table_name} INNER JOIN
                                Dividend{div_no} ON {table_name}.sno = Dividend{div_no}.Shareholderno AND {table_name}.divno = Dividend{div_no}.DividendNo
                            """
                        else:
                            update_query = f"""
                                UPDATE Dividend{div_no}
                                SET DividendPaymentDate = {table_name}.Date, DividendPaid = 1
                                FROM Dividend{div_no} INNER JOIN
                                {table_name} ON Dividend{div_no}.ShareholderNo = {table_name}.sno AND Dividend{div_no}.DividendNo = {table_name}.divno
                            """

                            update_query_2 = f"""
                                UPDATE {table_name}
                                SET matched = 1
                                FROM {table_name} INNER JOIN
                                Dividend{div_no} ON {table_name}.sno = Dividend{div_no}.Shareholderno AND {table_name}.divno = Dividend{div_no}.DividendNo
                            """
                    result = connection.execute(text(update_query))
                    print(f"✅ Dividend{div_no}: {result.rowcount} rows updated")
                    result2 = connection.execute(text(update_query_2))
                    success_list.append(div_no)

                except Exception as e:
                    print(f"❌ Failed on Dividend{div_no}: {e}")
                    failed_list.append(div_no)

        print("\n✅ Successful Updates:")
        print(success_list if success_list else "None")

        print("\n❌ Failed Updates:")
        print(failed_list if failed_list else "None")
        queryyy = f"""
                    SELECT *
                    FROM    [{db_name}].[dbo].[{table_name}]
                    WHERE [{table_name}].[matched] IS NULL
                """
        df_output = pd.read_sql(queryyy,engine)
        payment_name = df_payment['Description'][Payment_code_index]
        df_output.to_csv(path_or_buf=f'./DIVS_NOT_UPDATED_{db_name}_{payment_name}.csv')

    try:
        if localhost is None:
            localhost = input("Use localhost SQL Server? (y/n): ").strip().lower() == 'y'

        if localhost:
            engine = create_engine(
                f"mssql+pyodbc://localhost/master?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )
            db_df = pd.read_sql("SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')", engine)
            selected_index = paginate_table_list(db_df)
            print("Databases on the server:")
            if selected_index is not None:
                db_name = db_df['name'].iloc[selected_index]
            else:
                db_input = input("Or type the table name manually(or 'q' to quit): ").strip()
                if db_input == 'q':
                    print("Exiting...")
                    return
                db_name = db_input
            conn_str = (
                f"mssql+pyodbc://localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )
        else:
            creds_path = Path(__file__).resolve().parent / "server_credentials.csv"
            server_credentials = pd.read_csv(creds_path)
            print("List of previous server_ip:")
            print(server_credentials['Server_ip'])

            pick = input("Pick number or type 'new' ")
            if pick == 'new':
                ip = input("IP: ")
                user = input("Username: ")
                password = input("Password: ")
                new_row = {'Server_ip':ip,
                           'Username':user,
                           'Password':password
                           }
                if ip not in server_credentials['Server_ip'].values:
                    csv_path = Path('./server_credentials.csv')
                    if csv_path.exists():
                        existing = pd.read_csv(csv_path, index_col=0)
                        next_index = existing.index.max() + 1
                    else:
                        next_index = 0
                    pd.DataFrame([new_row], index=[next_index]).to_csv(csv_path, mode='a', header=not csv_path.exists())
                else:
                    print("IP address already saved! Press any key to continue...")
                    input()
                    return update_div()

                conn_str = f"mssql+pyodbc://{user}:{password}@{ip}/master?Driver=ODBC+Driver+17+for+SQL+Server"
                engine = create_engine(conn_str)
                db_df = pd.read_sql("SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')", engine)
                selected_index = paginate_table_list(db_df)
                print("Databases on the server:")
                if selected_index is not None:
                    db_name = db_df['name'].iloc[selected_index]
                else:
                    db_input = input("Or type the table name manually(or 'q' to quit): ").strip()
                    if db_input == 'q':
                        print("Exiting...")
                        return
                    db_name = db_input
                # db_name = db_df.iloc[db_index]['name']

                new_row = {'Server_ip': ip, 'Username': user, 'Password': password}
                if ip not in server_credentials['Server_ip'].values:
                    pd.DataFrame([new_row]).to_csv('./server_credentials.csv', mode='a', header=False, index=False)
            else:
                idx = int(pick)
                ip = server_credentials['Server_ip'][idx]
                user = server_credentials['Username'][idx]
                password = server_credentials['Password'][idx]

                conn_str = f"mssql+pyodbc://{user}:{password}@{ip}/master?Driver=ODBC+Driver+17+for+SQL+Server"
                engine = create_engine(conn_str)
                db_df = pd.read_sql("SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')", engine)
                print("Databases on the server:")
                print(db_df)
                db_index = int(input("Select DB index: "))
                db_name = db_df.iloc[db_index]['name']

            conn_str = f"mssql+pyodbc://{user}:{password}@{ip}/{db_name}?Driver=ODBC+Driver+17+for+SQL+Server"

        engine = create_engine(conn_str)
        queryyyyy =  "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        df_tables = pd.read_sql(queryyyyy,engine)

        selected_index = paginate_table_list(df_tables)
        if selected_index is not None:
            table_name = df_tables['TABLE_NAME'].iloc[selected_index]
        else:
            table_input = input("Or type the table name manually (or 'q' to quit): ").strip()
            if table_input.lower() == 'q':
                print("Exiting...")
                return
            table_name = table_input

        with engine.begin() as conn:
            div_list = [row[0] for row in conn.execute(text(
                f"SELECT [DIVNO] FROM [{db_name}].[dbo].[{table_name}] GROUP BY [DIVNO]"
            )) if row[0] is not None]

        print(f"Dividend list: {div_list}")
        run_updates(engine, db_name, table_name, div_list)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        input("\nPress any key to exit...")

    else:
        input("\nPress any key to exit....")

# Run the function
update_div()