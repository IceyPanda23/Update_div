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
    print(f"✅ Excel data successfully imported into table: {table_name}")

def clean_call_logs():
    import pandas as pd
    import numpy as np
    import sqlalchemy
    from sqlalchemy import create_engine
    from datetime import datetime, timedelta
    import re

    file_path = rf'C:\Users\Ronald.Kipngetich\Downloads\call_reports (2).csv'
    df = pd.read_csv(file_path)
    df['Call Time'] = pd.to_datetime(df['Call Time'],errors='coerce')
    df = df[df['Status'] != 'Waiting']
    df = df[df['Direction'] != 'Internal']
    df = df[df['Direction'] != 'Inbound Queue']
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
    df.loc[df['Direction'] == 'Inbound', 'Direction'] = 'Incoming'
    df.loc[df['Direction'] == 'Outbound', 'Direction'] = 'Outgoing'
    df['Incoming_true'] = (df['Direction'] == 'Incoming').astype(int)
    df_incoming = df[df['Direction'] == 'Incoming'].reset_index()
    df_outgoing = df[df['Direction'] == 'Outgoing'].reset_index()
    conn_str = f"mssql+pyodbc://sa:skyblue2009*@192.168.11.3/QueueSystem?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str, fast_executemany=True)
    df_incoming.to_sql('Clean_3cx_incoming', con=engine, index=False, if_exists='replace')
    print(f"✅ Data successfully imported into table: 3cx_Clean_incoming")
    df_outgoing.to_sql('Clean_3cx_outgoing', con=engine, index=False,if_exists='replace')
    print(f"✅ Data successfully imported into table: 3cx_Clean_outgoing")
    df.to_sql('Clean_3cx_All', con=engine, index=False, if_exists='replace')
    print(f'✅ Data successfully imported into table: 3cx_Clean_All')


def Import_Thing(file_path=None, file_name=None):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import create_engine

    # Prompt for input if not provided
    if not file_name:
        file_name = input("Please input the file name (without extension or quotes): ")

    if not file_path:
        file_path = input("Please input the full file path to the Excel file: ")

    try:
        # Read Excel file
        df = pd.read_csv(file_path)

        # SQL Server connection string (Windows Authentication)
        conn_str = (
            "mssql+pyodbc://sa:skyblue2009*@192.168.11.3\QueueSystem"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(conn_str, fast_executemany=True)

        # Write to SQL
        df.to_sql(file_name, con=engine, index=False, if_exists='replace')
        print(f"✅ Data successfully imported into table: {file_name}")
        input("Please type anything to exit...")

    except Exception as e:
        print(f"❌ Error: {e}")
