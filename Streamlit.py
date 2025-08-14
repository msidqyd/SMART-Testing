# app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# -----------------------------
# 0) PAGE CONFIG
# -----------------------------
st.set_page_config(page_title="SMI Final Browser", layout="wide")

# -----------------------------
# 1) LOGIN
# -----------------------------
# Simple username/password dictionary
USER_CREDENTIALS = {
    "admin": "password123",
    "user": "user123"
    
}

if "login_success" not in st.session_state:
    st.session_state.login_success = False

def login():
    st.title("User Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
            st.session_state.login_success = True
            st.experimental_rerun()
        else:
            st.error("❌ Invalid username or password")

if not st.session_state.login_success:
    login()
    st.stop() 


st.title("S.M.A.R.T Sales Monitoring And Route Tracking 📊")


def make_engine():
    db_secrets = st.secrets.get("db", {})
    
    server = db_secrets.get("server", "localhost\\SQLEXPRESS")
    database = db_secrets.get("database", "StreamliteDB")
    driver = db_secrets.get("driver", "ODBC Driver 17 for SQL Server")
    username = db_secrets.get("username", "")
    password = db_secrets.get("password", "")
    use_trusted = db_secrets.get("trusted_connection", True)
    
    if use_trusted:
        conn_str = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
    else:
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"

    return create_engine(conn_str, fast_executemany=True)

engine = make_engine()
TABLE = "dbo.SMI_Final"

# -----------------------------
# 4) HELPERS
# -----------------------------
@st.cache_data(ttl=300)
def load_distinct_values():
    sql = f"""
        SELECT DISTINCT
            Division,
            WH_Name,
            Region,
            SPV_Employee_Name AS Supervisor,
            Sales_Office_Name,
            CAST(Sales_Date AS DATE) AS Sales_Date
        FROM {TABLE}
    """
    return pd.read_sql(sql, engine)

distinct_df = load_distinct_values()

@st.cache_data(ttl=300)
def fetch_filtered(where_sql="", params=None, limit=None):
    sql = f"SELECT * FROM {TABLE} WHERE 1=1 {where_sql}"
    if limit:
        sql += f" ORDER BY Sales_Date DESC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
    return pd.read_sql(text(sql), engine, params=params or {})

@st.cache_data(ttl=300)
def count_rows(where_sql="", params=None):
    sql = f"SELECT COUNT(1) AS cnt FROM {TABLE} WHERE 1=1 {where_sql}"
    return int(pd.read_sql(text(sql), engine, params=params or {}).iloc[0,0])

# -----------------------------
# 5) SIDEBAR FILTERS
# -----------------------------
st.sidebar.header("Filter Options")

df_filtered = distinct_df.copy()

# Division
division_opts = sorted(distinct_df["Division"].dropna().unique())
selected_div = st.sidebar.selectbox("Division", ["All"] + division_opts)
if selected_div != "All":
    df_filtered = df_filtered[df_filtered["Division"] == selected_div]

# AMO / Sales Office
amo_opts = sorted(df_filtered["Sales_Office_Name"].dropna().unique())
selected_amo = st.sidebar.selectbox("AMO", ["All"] + amo_opts)
if selected_amo != "All":
    df_filtered = df_filtered[df_filtered["Sales_Office_Name"] == selected_amo]

# WH_Name
wh_opts = sorted(df_filtered["WH_Name"].dropna().unique())
selected_wh = st.sidebar.selectbox("WH_Name", ["All"] + wh_opts)
if selected_wh != "All":
    df_filtered = df_filtered[df_filtered["WH_Name"] == selected_wh]

# Supervisor
spv_opts = sorted(df_filtered["Supervisor"].dropna().unique())
selected_spv = st.sidebar.selectbox("Supervisor", ["All"] + spv_opts)
if selected_spv != "All":
    df_filtered = df_filtered[df_filtered["Supervisor"] == selected_spv]

# Region
region_opts = sorted(df_filtered["Region"].dropna().unique())
selected_region = st.sidebar.selectbox("Region", ["All"] + region_opts)
if selected_region != "All":
    df_filtered = df_filtered[df_filtered["Region"] == selected_region]

# Sales Date
min_date = df_filtered["Sales_Date"].min()
max_date = df_filtered["Sales_Date"].max()
start_date, end_date = st.sidebar.date_input(
    "Sales Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
)

# -----------------------------
# 6) BUILD WHERE CLAUSE
# -----------------------------
where_parts = []
params = {}
if selected_div != "All":
    where_parts.append(" AND Division = :division")
    params["division"] = selected_div
if selected_amo != "All":
    where_parts.append(" AND Sales_Office_Name = :amo")
    params["amo"] = selected_amo
if selected_wh != "All":
    where_parts.append(" AND WH_Name = :wh")
    params["wh"] = selected_wh
if selected_spv != "All":
    where_parts.append(" AND SPV_Employee_Name = :spv")
    params["spv"] = selected_spv
if selected_region != "All":
    where_parts.append(" AND Region = :region")
    params["region"] = selected_region

where_parts.append(" AND CAST(Sales_Date AS DATE) BETWEEN :start AND :end")
params["start"] = start_date
params["end"] = end_date

where_sql = "".join(where_parts)

# -----------------------------
# 7) FETCH & STATS
# -----------------------------
total_all = count_rows()
total_filtered = count_rows(where_sql, params)

c1, c2 = st.columns(2)
c1.metric("Total rows in dbo.SMI_Final", f"{total_all:,}")
c2.metric("Rows after filters", f"{total_filtered:,}")

fetch_limit = 10000 if total_filtered > 10000 else None
if fetch_limit:
    st.warning(f"Showing first {fetch_limit:,} rows. Please refine filters to see all rows.")

df = fetch_filtered(where_sql, params, limit=fetch_limit)

# Select columns to show
front_cols = [
    "Sequence", "Visit_Time","Outlet_Latitude","Outlet_Longitude","Customer_Code","Store_Name","Visit_Div_",
    "Pack_Regular","Pack_NPL","Travel_Transaction_Time_HHMMSS","Distance_Outlet_m", "Radius_MNT_m",
    "Google_Maps","Fake_Indication","Outlet_Address"
]
df = df[[c for c in front_cols if c in df.columns]]
if "Sequence" in df.columns:
    df = df.sort_values("Sequence").reset_index(drop=True)

# -----------------------------
# 8) CONDITIONAL FORMATTING
# -----------------------------
def highlight_cells(val, col_name):
    if col_name == "Pack_NPL" and val == 0:
        return "background-color: red; color: white"
    elif col_name == "Distance_Outlet_m" and val > 2000:
        return "background-color: red; color: white"
    elif col_name == "Radius_MNT_m" and val > 300:
        return "background-color: red; color: white"
    elif col_name == "Fake_Indication" and val > 1:
        return "background-color: red; color: white"
    else:
        return ""

def style_df(df):
    return df.style.applymap(lambda v: highlight_cells(v, "Pack_NPL"), subset=["Pack_NPL"])\
                   .applymap(lambda v: highlight_cells(v, "Distance_Outlet_m"), subset=["Distance_Outlet_m"])\
                   .applymap(lambda v: highlight_cells(v, "Radius_MNT_m"), subset=["Radius_MNT_m"])\
                   .applymap(lambda v: highlight_cells(v, "Fake_Indication"), subset=["Fake_Indication"])

st.write("### Results with Conditional Formatting")
st.dataframe(style_df(df), use_container_width=True, height=600)


@st.cache_data(ttl=300)
def to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")

st.download_button(
    "⬇️⬇️⬇️ Download CSV",
    data=to_csv_bytes(df),
    file_name="SMI_Final_filtered.csv",
    mime="text/csv"
)
