import importlib
import subprocess
import sys


def install_if_missing(package_name, import_name=None):
    import_name = import_name or package_name
    try:
        importlib.import_module(import_name)
    except ImportError:
        print(f"📦 Installing missing package: {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

install_if_missing('pandas')
install_if_missing('sqlalchemy')
install_if_missing('pyodbc')

import pandas as pd
from sqlalchemy import text,create_engine
import pyodbc

# def ensure_dependencies():
#     for pkg, imp in [("pandas", "pandas"), ("sqlalchemy", "sqlalchemy"), ("pyodbc", "pyodbc")]:
#         install_if_missing(pkg, imp)

def paginate_table_list(table_df, page_size=10):
    filtered_df = full_df = table_df.copy()
    current_page = 0

    while True:
        start, end = current_page * page_size, (current_page + 1) * page_size
        end = min(end, len(filtered_df))

        print(f"\nShowing tables {start} to {end - 1} of {len(filtered_df) - 1}")
        print(filtered_df.iloc[start:end].reset_index(drop=True))

        print("\nOptions: [Enter]=Next | p=Prev | s:<term>=Search | r=Reset | q=Quit(or add new) | [Index]=Select")
        choice = input("Select: ").strip().lower()

        if choice == '':
            if end >= len(filtered_df): print("🔚 You're at the end.")
            else: current_page += 1
        elif choice == 'p':
            if current_page == 0: print("🔝 You're at the first page.")
            else: current_page -= 1
        elif choice == 'r':
            filtered_df = full_df.copy()
            current_page = 0
        elif choice.startswith('s:'):
            term = choice[2:]
            if not term:
                print("⚠️ Enter search term after 's:'.")
            else:
                filtered_df = full_df[full_df['TABLE_NAME'].str.lower().str.contains(term)]
                current_page = 0
        elif choice == 'q':
            return None
        elif choice.isdigit():
            idx = int(choice)
            abs_idx = current_page * page_size + idx
            if 0 <= abs_idx < len(filtered_df):
                return filtered_df.index[abs_idx]
            print("❌ Invalid index.")
        else:
            print("❌ Invalid input.")

def get_server_connection():
    creds = pd.read_csv('./server_credentials.csv')
    print("List of previous server IPs:")
    print(creds['Server_ip'])

    selection = input("Pick number or type 'new' to add: ").strip().lower()

    if selection == 'new':
        db_name = input("Database name: ")
        ip = input("IP address: ")
        user = input("Username: ")
        pwd = input("Password: ")

        if ip not in creds['Server_ip'].values:
            new_row = {'Server_ip': ip, 'Username': user, 'Password': pwd}
            pd.DataFrame([new_row]).to_csv('./server_credentials.csv', mode='a', header=False, index=True)
        else:
            print("⚠️ IP already exists!")

    else:
        try:
            i = int(selection)
            ip, user, pwd = creds.loc[i, ['Server_ip', 'Username', 'Password']]
        except Exception:
            print("❌ Invalid selection.")
            return None, None, None

    engine = create_engine(f"mssql+pyodbc://{user}:{pwd}@{ip}/master?Driver=ODBC+Driver+17+for+SQL+Server")
    dbs = pd.read_sql("SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb')", engine)

    print("Available databases:")
    selected_index = paginate_table_list(dbs)
    print("Databases on the server:")
    if selected_index is not None:
        db_name = dbs['name'].iloc[selected_index]
    else:
        db_input = input("Or type the table name manually(or 'q' to quit/Add a new table): ").strip()
        if db_input == 'q':
            print("Exiting...")
            return
        db_name = db_input

    conn_str = f"mssql+pyodbc://{user}:{pwd}@{ip}/{db_name}?Driver=ODBC+Driver+17+for+SQL+Server"
    return conn_str, db_name

def get_table_name(engine):
    replace_new = input("Do you wish to replace an existing table name or add a new one:(y/n)")
    if replace_new == 'n':
        table_name = input("Please input new table name:")
        table_name = str(table_name)
        return table_name
    elif replace_new == 'y':
        query ="SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        df = pd.read_sql(query, engine)

        idx = paginate_table_list(df)
        if idx is not None:
            return df['TABLE_NAME'].iloc[idx]
        manual = input("Enter table name (or 'q' to quit): ").strip()
        return None if manual.lower() == 'q' or not manual else manual
    else:
        input("Wrong input.Press any key to retry...")
        get_table_name(engine)

def import_excel_to_sql(conn_str, table_name):
    file_path = input("Excel file path: ")
    sheet = input("Sheet name (or 0 for default): ").strip()

    df = pd.read_excel(file_path) if sheet == '0' else pd.read_excel(file_path, sheet_name=sheet)
    df.columns = [col.strip().replace(" ", "").lower() for col in df.columns]

    engine = create_engine(conn_str, fast_executemany=True)
    df.to_sql(table_name, con=engine, index=False, if_exists='replace')
    print(f"✅ Data imported into table: {table_name}")

def Import_Thing():
    # ensure_dependencies()
    # Choose connection type
    if input("Localhost? (y/n): ").lower() == 'y':
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
        conn_str = f"mssql+pyodbc://localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    else:
        conn_str, db_name = get_server_connection()
    # if not conn_str:
    #     return

    engine = create_engine(conn_str)
    table_name = get_table_name(engine)
    if not table_name:
        print("❌ No table selected. Exiting.")
        return

    try:
        import_excel_to_sql(conn_str, table_name)
    except Exception as e:
        print(f"❌ Import failed: {e}")

    if input("Continue? (y/n): ").strip().lower() == 'y':
        Import_Thing()
    else:
        print("👋 Done.")

# Run the function
Import_Thing()