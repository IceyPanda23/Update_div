def agent_perf(nmweeks=None, yesterday=False, ytd=False):
    import pandas as pd
    import pyodbc
    import numpy as np
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine,text

    # Connect to SQL Server
    conn_str = (
        r"mssql+pyodbc://sa:skyblue2009*@192.168.11.3/QueueSystem?driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(url=conn_str)
    # with engine.begin() as connection:

        # Load data from SQL Server
    query = "SELECT * FROM [QueueSystem].[dbo].[nkevinDataAnalyticsBI]"
    df = pd.read_sql(query,con=conn_str)

    # Parse datetime column
    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')

    # Clean data: Remove rows with null or negative time values
    df_filtered = df[
        (df['CustomerWaitingTimeInMinutes'].notna()) &
        (df['CustomerBeingServedInMinutes'].notna()) &
        (df['CustomerWaitingTimeInMinutes'] >= 0) &
        (df['CustomerBeingServedInMinutes'] >= 0) &
        (df['Username'].isin(['FAITH.MOGAKA','diana.maundu','rosemary.kariuki','RODAH.TIMBWA','HARON.MAALU','JOSEPHINE.ONDIMU','HILLARY.MWAMBI','KELVIN.MUSYOKA']))
    ]

    # Apply time filters
    if yesterday:
        yesterday_date = datetime.today().date() - timedelta(days=1)
        df_filtered = df_filtered[df_filtered['Datetime'].dt.date == yesterday_date]
    elif ytd:
        start_of_year = datetime(datetime.today().year, 1, 1)
        df_filtered = df_filtered[df_filtered['Datetime'] >= start_of_year]
    elif nmweeks is not None:
        start_date = datetime.today() - timedelta(weeks=nmweeks)
        df_filtered = df_filtered[df_filtered['Datetime'] >= start_date]

    # Group by agent and calculate metrics
    agent_stats = df_filtered.groupby(['Username']).agg({
        'CustomerWaitingTimeInMinutes': 'mean',
        'CustomerBeingServedInMinutes': 'mean',
        'UserID': 'count'
    }).rename(columns={
        'CustomerWaitingTimeInMinutes': 'avg_waiting_time',
        'CustomerBeingServedInMinutes': 'avg_serving_time',
        'UserID': 'resolved_cases'
    }).reset_index()

    # Normalize metrics
    def normalize(series, invert=False):
        if series.max() == series.min():
            return pd.Series([1] * len(series))
        norm = (series - series.min()) / (series.max() - series.min())
        return 1 - norm if invert else norm

    agent_stats['Norm Waiting'] = normalize(agent_stats['avg_waiting_time'], invert=True)
    agent_stats['Norm Serving'] = normalize(agent_stats['avg_serving_time'], invert=True)
    agent_stats['Norm Cases'] = normalize(agent_stats['resolved_cases'])

    # Final performance score
    w1, w2, w3 = 0.2, 0.3, 0.5
    agent_stats['Overall Performance %'] = np.round(
        (w1 * agent_stats['Norm Waiting'] +
         w2 * agent_stats['Norm Serving'] +
         w3 * agent_stats['Norm Cases']) * 100, 2
    )

    # Drop intermediate columns
    agent_stats.drop(columns=['Norm Cases', 'Norm Serving', 'Norm Waiting'], inplace=True)

    # Sort by performance
    agent_stats.sort_values(by='Overall Performance %', ascending=False, inplace=True)

    # Determine file path
    if yesterday:
        file_suffix = "yesterday"
        timestamp = ""
    elif ytd:
        file_suffix = "YTD"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    else:
        file_suffix = f"{nmweeks}" if nmweeks is not None else "all"
        timestamp = "weeks" if nmweeks is not None else ""
    if ytd:
        file_path = f'./agent_performance_YTD.xlsx'
    else:
        file_path = f'./agent_performance_{file_suffix}_{timestamp}.xlsx'

    # Export to Excel
    agent_stats.to_excel(
        file_path,
        index=False,
        engine='openpyxl',
        float_format='%.2f'
    )

    # Upload to SQL Server
    df = pd.read_excel(file_path,engine='openpyxl')

    if yesterday:
        table_name = 'AgentPerformanceAttempt_yesterday'
    elif ytd:
        table_name = f'AgentPerformanceAttempt_YTD'
    else:
        table_name = f'AgentPerformanceAttempt_{file_suffix}_{timestamp}'

    # SQLAlchemy connection string
    conn_str = r"mssql+pyodbc://sa:skyblue2009*@192.168.11.3/QueueSystem?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)

    df.to_sql(table_name, con=engine, index=False, if_exists='replace')
    print(f" Excel data successfully imported into table: {table_name}")

def clean_call_logs():
    from functions import ensure_module
    ensure_module('pandas')
    ensure_module('sqlalchemy')
    ensure_module('datetime')
    ensure_module('re')
    ensure_module('pathlib')



    import pandas as pd
    import numpy as np
    import sqlalchemy
    from sqlalchemy import create_engine
    from datetime import datetime, timedelta
    import re
    from functions import import_emails
    from pathlib import Path



    import_emails()
    yesterdays = datetime.now() - timedelta(days=1)
    yesterday_day = int(yesterdays.strftime('%Y%d%m'))
    file_path = Path(__file__).resolve().parent / "Emails" / "Your 3CX Scheduled Reports are ready" / f"3CX_Report_{yesterday_day}.csv"
    df = pd.read_csv(file_path)
    df['Call Time'] = pd.to_datetime(df['Call Time'],errors='coerce')
    df = df[df['Status'] != 'Waiting']
    df = df[df['Direction'] != 'Internal']
    df = df[(df['Direction'] != 'Inbound Queue') & (df['Call Time'].notna())]
    df = df.drop(columns=['Sentiment','Summary','Transcription','Call ID'])
    df.loc[df['Status'] == 'Unanswered', 'Talking'] = '00:00:00'
    df['Talking'] = pd.to_timedelta(df['Talking'], errors='coerce')
    from_pattern = r'^(.+\s\(\d{4}\)|\d{4})$'
    details_pattern_1 = r'.+\(\d{4}\)\s→\sVia trunk: .+\s→\sEnded by .+\(\d{4}\)'
    details_pattern_2 = r'^Dialed: .+?\(\d{4}\)(\s?\?| →)'
    details_pattern_3 = r'^\d{4}$'
    details_pattern_5 = r'Inbound: .+?\(\d{4}\)(\s?\?| →)'
    regex_mask = (
        df['Call Activity Details'].str.contains(details_pattern_1, na=False) |
        df['Call Activity Details'].str.contains(details_pattern_2, na=False) |
        df['Call Activity Details'].str.contains(details_pattern_3, na=False) |
        df['Call Activity Details'].str.contains(details_pattern_5, na=False)
    )
    def ends_with_same_id(row):
        from_val = row['From']
        details = row['Call Activity Details']
        if pd.isna(from_val) or pd.isna(details):
            return False
        if re.fullmatch(r'\d{4}', str(from_val)):
            return re.search(rf'Ended by .*?\({from_val}\)', details) is not None
        return False
    self_end_mask = df.apply(ends_with_same_id, axis=1)
    df = df[
        ~(
            df['From'].str.contains(from_pattern, na=False) &
            (regex_mask | self_end_mask)
        )
    ]
    filterrr = df.loc[df['From'].str.startswith('AGM',na=False)]
    AGM_phone_list = []
    for idx,val in list(filterrr['From'].items()):
        AGM_phone_list.append(val)
    df = df[~df['From'].isin(AGM_phone_list)]
    df['Talking'] = (df['Talking'].dt.total_seconds() / 60).round(2)
    df['Ringing'] = pd.to_timedelta(df['Ringing'],errors='coerce')
    df['Ringing'] = (df['Ringing'].dt.total_seconds())
    df['Call Date'] = pd.to_datetime(df['Call Time']).dt.normalize()
    df.loc[df['Direction'] == 'Inbound', 'Direction'] = 'Incoming'
    df.loc[df['Direction'] == 'Outbound', 'Direction'] = 'Outgoing'
    df['Incoming_true'] = (df['Direction'] == 'Incoming').astype(int)
    df_incoming = df[df['Direction'] == 'Incoming'].reset_index()
    df_outgoing = df[df['Direction'] == 'Outgoing'].reset_index()
    conn_str = f"mssql+pyodbc://sa:skyblue2009*@192.168.11.3/QueueSystem?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str, fast_executemany=True)
    df_incoming.to_sql('Clean_3cx_incoming', con=engine, index=False, if_exists='replace')
    print(f" Data successfully imported into table: 3cx_Clean_incoming")
    df_outgoing.to_sql('Clean_3cx_outgoing', con=engine, index=False,if_exists='replace')
    print(f" Data successfully imported into table: 3cx_Clean_outgoing")
    df.to_sql('Clean_3cx_All', con=engine, index=False, if_exists='replace')
    print(f' Data successfully imported into table: 3cx_Clean_All')


def sanitize_filename(name, max_length=80):
    from functions import ensure_module
    ensure_module('re')
    import re
    """Sanitize folder/file names for Windows and limit length."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return name[:max_length].strip()

def ensure_module(package_name, import_name=None):
    import importlib
    import subprocess
    import sys
    import_name = import_name or package_name
    try:
        importlib.import_module(import_name)
    except ImportError:
        print(f"\U0001F50D '{import_name}' not found. Installing '{package_name}'...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

# Initialize COM
def import_emails():
    from functions import ensure_module,sanitize_filename
    ensure_module('pythoncom')
    ensure_module('win32com')
    ensure_module('re')
    ensure_module('pathlib')

    from pathlib import Path
    import win32com.client
    import pythoncom
    import re


    pythoncom.CoInitialize()

    try:
        output_dir = Path.cwd() / "Emails"
        output_dir.mkdir(parents=True, exist_ok=True)

        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        inbox = outlook.GetDefaultFolder(6)
        messages = inbox.Items

        # for idx, message in enumerate(messages):
        target_subject = "Your 3CX Scheduled Reports are ready"
        filtered_messages = [msg for msg in messages if msg.Subject and target_subject in msg.Subject]

        for idx, message in enumerate(filtered_messages):
            raw_subject = str(message.Subject) or f"Message_{idx}"
            subject = sanitize_filename(raw_subject, 80)
            body = message.Body
            attachments = message.Attachments

            target_folder = output_dir / subject
            target_folder.mkdir(parents=True, exist_ok=True)

            Path(target_folder / "EMAIL_BODY.txt").write_text(str(body), encoding='utf-8')

            for a_idx in range(1, attachments.Count + 1):
                attachment = attachments.Item(a_idx)
                raw_name = attachment.FileName or f"attachment_{a_idx}.dat"
                match = re.match(r"thismonth_(\d{2})(\d{2})_",raw_name)
                if match:
                    day = match.group(1)
                    month = match.group(2)
                    safe_name = f"3CX_Report_2025{day}{month}.csv"
                else:

                    safe_name = f"attachment_{a_idx}{Path(raw_name).suffix}"

                save_path = target_folder / safe_name

                # Absolute path length fallback if needed
                if len(str(save_path)) >= 240:
                    safe_name = f"attachment_{a_idx}.dat"
                    save_path = target_folder / safe_name

                try:
                    attachment.SaveAsFile(str(save_path))
                except Exception as e:
                    print(f"❌ Failed to save attachment '{raw_name}' in '{subject}': {e}")

    finally:
        pythoncom.CoUninitialize()


def Imports(file_path=None, file_name=None):
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
    install_if_missing('pathlib')

    from pathlib import Path
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
        creds_path = Path(__file__).resolve().parent / "server_credentials.csv"
        creds = pd.read_csv(creds_path)
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
        from functions import ensure_module
        ensure_module('pandas')
        ensure_module('sqlalchemy')
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