import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from io import BytesIO
import re
from datetime import timedelta
import folium
from folium.plugins import AntPath
from streamlit_folium import st_folium
import urllib
import tempfile

USER_CREDENTIALS = {
    "admin1": "ArsenalJuara2026@",
    "admin2": "Welcomeqwerty123321@@@"
}

def login():
    st.title("Login Page")
    
    username = st.text_input("Username")
    password = st.text_input("Password", type='password')
    
    if st.button("Login"):
        if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
            st.session_state.logged_in = True  
            st.success(f"Welcome {username}!")
            st.query_params["page"] = "SMART"
        else:
            st.error("Invalid username or password")

def SMART():
    st.set_page_config(page_title="SMI Final Browser", layout="wide")

    # ============================ UI & CSS ============================
    st.markdown(
        """
        <style>
        .ktg-title {
        background: #c8e6c9; color:#1b5e20; border:1px solid #2e7d32;
        border-radius: 8px; text-align:center; padding:12px 10px; 
        font-weight: 800; font-size: 28px; margin-bottom: 10px;
        }
        .card { border: 1px solid #2e7d32; border-radius: 8px; margin: 6px 0 16px; }
        .card > h4{
        margin:0; padding:12px;
        background:#c8e6c9; color:#1b5e20;
        text-align:center !important;
        border-bottom:1px solid #2e7d32;
        font-weight:800; font-size:24px; letter-spacing:.3px;
        }
        .section-title {
        background:#c8e6c9; color:#1b5e20; border:1px solid #2e7d32; 
        border-radius:6px; padding:6px 10px; font-weight:800; margin: 8px 0 4px;
        }
        .ktg-table { width:100%; border-collapse:collapse; table-layout:fixed; }
        .ktg-table th, .ktg-table td { border:1px solid #2e7d32; padding:6px; font-size:12px; 
                                    text-align:center; word-break:break-word; }
        .ktg-table thead th { background:#e8f5e9; }
        .ktg-table tfoot td { background:#f1f8e9; font-weight:600; }
        .text-left { text-align:left !important; }

        #sql-table { overflow-x: visible !important; }
        #sql-table table { table-layout: fixed; width: 100% !important; border-collapse: collapse; }
        #sql-table thead th { background:#e8f5e9; border:1px solid #2e7d32; }
        #sql-table td, #sql-table th { 
            font-size: 11px !important; 
            white-space: normal !important; word-break: break-word !important; overflow-wrap: anywhere !important;
            line-height: 1.15; padding: 4px 6px; vertical-align: top; border:1px solid #2e7d32;
        }
        html, body, .block-container { overflow-x: visible !important; }
        .stDataFrame td, .stDataFrame th { font-size: 8px !important; }

        .print-note { display: none; }
        @media print {
        header, footer, [data-testid="stSidebar"] { display:none !important; }
        .block-container { padding:0 !important; margin:0 !important; }
        @page { size: A3 landscape; margin: 10mm; }
        .print-note {
            display:block; position: fixed; top: 0; left:0; right:0; 
            text-align:center; font-weight:700; color:#1b5e20;
            border-bottom:1px solid #2e7d32; padding:4px 0; background:#c8e6c9;
        }
        body { margin-top: 34px; }
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    formatted_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    formatted_html = f'<div class="print-note">SMI Browser — SMART Sales Monitoring & Route Tracking • Printed {formatted_time}</div>'

    st.markdown(formatted_html, unsafe_allow_html=True)
    st.markdown('<div class="ktg-title">S.M.A.R.T Sales Monitoring And Route Tracking (SMART)</div>', unsafe_allow_html=True)

    # ============================ DB ENGINE (cached) ============================
    @st.cache_resource
    def make_engine():
        try:
            s = st.secrets["dbo"]
            
            # Try different ODBC drivers in order of preference
            drivers_to_try = [
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 13 for SQL Server", 
                "ODBC Driver 11 for SQL Server",
                "SQL Server Native Client 11.0",
                "SQL Server"
            ]
            
            engine = None
            last_error = None
            
            for driver in drivers_to_try:
                try:
                    odbc_str = (
                        f"DRIVER={{{driver}}};"
                        f"SERVER={s['host']},{s['port']};DATABASE={s['database']};UID={s['username']};PWD={s['password']};"
                        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
                    )
                    url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)
                    engine = create_engine(url)
                    
                    # Test the connection
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    
                    st.sidebar.success(f"Connected using: {driver}")
                    return engine
                    
                except Exception as e:
                    last_error = e
                    continue
            
            # If all drivers fail, show error
            raise Exception(f"Could not connect with any ODBC driver. Last error: {str(last_error)}")
            
        except Exception as e:
            st.error(f"Database connection failed: {str(e)}")
            st.info("Please check your database configuration in Streamlit secrets.")
            st.stop()

    engine = make_engine()
    TABLE = "dbo.SMI_Final"

    # ============================ Performance toggles ============================
    st.sidebar.markdown("### Performance Adjustment")
    fast_mode = st.sidebar.checkbox("Fast table (no cell coloring)", value=True)
    max_rows = st.sidebar.number_input("Row cap (fetch at most)", min_value=20, max_value=60, value=60, step=10)
    dirty_read = st.sidebar.checkbox("Read-uncommitted (faster, allows dirty reads)", value=False)

    # ============================ Helpers ============================
    def _where(parts):
        return (" WHERE 1=1 " + " ".join(parts)) if parts else " WHERE 1=1 "

    @st.cache_data(ttl=900, show_spinner=False)
    def get_columns():
        try:
            query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='SMI_Final'
            """
            
            with engine.connect() as conn:
                result = pd.read_sql_query(query, conn)
            
            return result["COLUMN_NAME"].tolist()
            
        except Exception as e:
            st.error(f"Error fetching columns: {str(e)}")
            # Return fallback columns
            return ["Sequence", "Visit_Time", "Latitude", "Longitude", "Customer_Code", "Store_Name", 
                   "Visit_Div_", "Pack_Regular", "Pack_NPL", "Google_Maps", "Fake_Indication"]

    def pick_col(candidates, available):
        if not available:
            return None
        avail_lower = {c.lower(): c for c in available}
        for c in candidates:
            if not c: 
                continue
            real = avail_lower.get(c.lower())
            if real: 
                return real
        return None

    def _parse_hhmmss_to_seconds(x):
        if x is None or (isinstance(x, float) and pd.isna(x)): 
            return None
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "nat", "none"}: 
            return None
        m = re.search(r'(\d{1,2}):(\d{1,2}):(\d{1,2})', s)
        if m:
            h, m_, s_ = map(int, m.groups())
            return h*3600 + m_*60 + s_
        digits = re.sub(r'\D', '', s)
        if digits.isdigit() and 1 <= len(digits) <= 6:
            digits = digits.zfill(6)
            h, m_, s_ = int(digits[:2]), int(digits[2:4]), int(digits[4:6])
            return h*3600 + m_*60 + s_
        try:
            return int(float(s))
        except Exception:
            return None

    try:
        ALL_COLS = get_columns()
    except Exception as e:
        st.error(f"Failed to get columns: {str(e)}")
        st.stop()

    # Resolve real column names from your schema (case/variant tolerant)
    DIVISION_COL   = pick_col(["Division","Divisi"], ALL_COLS)
    AMO_COL        = pick_col(["Sales_Office_Name","Sales_Office","AMO","AMO_Name","SalesOffice","Sales_OfficeName"], ALL_COLS)
    WH_COL         = pick_col(["WH_Name","Warehouse","Warehouse_Name","WH"], ALL_COLS)
    SPV_COL        = pick_col(["SPV_Employee_Name","SPV_Name","Supervisor","Supervisor_Name"], ALL_COLS)
    REGION_COL     = pick_col(["Region","Region_Code","Area","Area_Name"], ALL_COLS)
    SALES_DATE_COL = pick_col(["Sales_Date","Visit_Date","Trans_Date","Date"], ALL_COLS)

    # ---------- Distincts & date range with optional NOLOCK ----------
    @st.cache_data(ttl=900, show_spinner=False)
    def distinct_division(nolock=""):
        if not DIVISION_COL:
            return []
        try:
            query = f"SELECT DISTINCT {DIVISION_COL} AS v FROM {TABLE}{nolock} WHERE {DIVISION_COL} IS NOT NULL ORDER BY v"
            with engine.connect() as conn:
                result = pd.read_sql_query(query, conn)
            return result["v"].tolist()
        except Exception as e:
            st.error(f"Error fetching divisions: {str(e)}")
            return []

    @st.cache_data(ttl=900, show_spinner=False)
    def distinct_amo(division, nolock=""):
        if not AMO_COL:
            return []
        try:
            parts = []
            params = {}
            if division and division != "All" and DIVISION_COL:
                parts.append(f" AND {DIVISION_COL}=:d")
                params["d"] = division
            
            query = f"SELECT DISTINCT {AMO_COL} AS v FROM {TABLE}{nolock}" + _where(parts) + f" AND {AMO_COL} IS NOT NULL ORDER BY v"
            with engine.connect() as conn:
                if params:
                    result = pd.read_sql_query(text(query), conn, params=params)
                else:
                    result = pd.read_sql_query(query, conn)
            return result["v"].tolist()
        except Exception as e:
            st.error(f"Error fetching AMO: {str(e)}")
            return []

    @st.cache_data(ttl=900, show_spinner=False)
    def distinct_wh(division, amo, nolock=""):
        if not WH_COL:
            return []
        try:
            parts = []
            params = {}
            if division and division != "All" and DIVISION_COL:
                parts.append(f" AND {DIVISION_COL}=:d")
                params["d"] = division
            if amo and amo != "All" and AMO_COL:
                parts.append(f" AND {AMO_COL}=:a")
                params["a"] = amo
            
            query = f"SELECT DISTINCT {WH_COL} AS v FROM {TABLE}{nolock}" + _where(parts) + f" AND {WH_COL} IS NOT NULL ORDER BY v"
            with engine.connect() as conn:
                if params:
                    result = pd.read_sql_query(text(query), conn, params=params)
                else:
                    result = pd.read_sql_query(query, conn)
            return result["v"].tolist()
        except Exception as e:
            st.error(f"Error fetching WH: {str(e)}")
            return []

    @st.cache_data(ttl=900, show_spinner=False)
    def distinct_spv(division, amo, wh, nolock=""):
        if not SPV_COL:
            return []
        try:
            parts = []
            params = {}
            if division and division != "All" and DIVISION_COL:
                parts.append(f" AND {DIVISION_COL}=:d")
                params["d"] = division
            if amo and amo != "All" and AMO_COL:
                parts.append(f" AND {AMO_COL}=:a")
                params["a"] = amo
            if wh and wh != "All" and WH_COL:
                parts.append(f" AND {WH_COL}=:w")
                params["w"] = wh
                
            query = f"SELECT DISTINCT {SPV_COL} AS v FROM {TABLE}{nolock}" + _where(parts) + f" AND {SPV_COL} IS NOT NULL ORDER BY v"
            with engine.connect() as conn:
                if params:
                    result = pd.read_sql_query(text(query), conn, params=params)
                else:
                    result = pd.read_sql_query(query, conn)
            return result["v"].tolist()
        except Exception as e:
            st.error(f"Error fetching SPV: {str(e)}")
            return []

    @st.cache_data(ttl=900, show_spinner=False)
    def distinct_region(division, amo, wh, spv, nolock=""):
        if not REGION_COL:
            return []
        try:
            parts = []
            params = {}
            if division and division != "All" and DIVISION_COL:
                parts.append(f" AND {DIVISION_COL}=:d")
                params["d"] = division
            if amo and amo != "All" and AMO_COL:
                parts.append(f" AND {AMO_COL}=:a")
                params["a"] = amo
            if wh and wh != "All" and WH_COL:
                parts.append(f" AND {WH_COL}=:w")
                params["w"] = wh
            if spv and spv != "All" and SPV_COL:
                parts.append(f" AND {SPV_COL}=:s")
                params["s"] = spv
                
            query = f"SELECT DISTINCT {REGION_COL} AS v FROM {TABLE}{nolock}" + _where(parts) + f" AND {REGION_COL} IS NOT NULL ORDER BY v"
            with engine.connect() as conn:
                if params:
                    result = pd.read_sql_query(text(query), conn, params=params)
                else:
                    result = pd.read_sql_query(query, conn)
            return result["v"].tolist()
        except Exception as e:
            st.error(f"Error fetching regions: {str(e)}")
            return []

    @st.cache_data(ttl=900, show_spinner=False)
    def date_range(division, amo, wh, spv, region, nolock=""):
        if not SALES_DATE_COL:
            today = pd.Timestamp.today().date()
            return today, today
        try:
            parts = []
            params = {}
            if division and division != "All" and DIVISION_COL:
                parts.append(f" AND {DIVISION_COL}=:d")
                params["d"] = division
            if amo and amo != "All" and AMO_COL:
                parts.append(f" AND {AMO_COL}=:a")
                params["a"] = amo
            if wh and wh != "All" and WH_COL:
                parts.append(f" AND {WH_COL}=:w")
                params["w"] = wh
            if spv and spv != "All" and SPV_COL:
                parts.append(f" AND {SPV_COL}=:s")
                params["s"] = spv
            if region and region != "All" and REGION_COL:
                parts.append(f" AND {REGION_COL}=:r")
                params["r"] = region

            query = f"""
                SELECT CAST(MIN({SALES_DATE_COL}) AS DATE) AS min_d,
                    CAST(MAX({SALES_DATE_COL}) AS DATE) AS max_d
                FROM {TABLE}{nolock} {_where(parts)}
            """
            
            with engine.connect() as conn:
                if params:
                    row = pd.read_sql_query(text(query), conn, params=params)
                else:
                    row = pd.read_sql_query(query, conn)
            
            min_d = row.loc[0, "min_d"]
            max_d = row.loc[0, "max_d"]
            if pd.isna(min_d) or pd.isna(max_d):
                today = pd.Timestamp.today().date()
                return today, today
            return min_d, max_d
        except Exception as e:
            st.error(f"Error fetching date range: {str(e)}")
            today = pd.Timestamp.today().date()
            return today, today

    # ============================ FILTER FORM ============================
    st.sidebar.header("Filters")

    # default selections in session
    for k in ("selected_div","selected_amo","selected_wh","selected_spv","selected_region"):
        st.session_state.setdefault(k, "All")
    st.session_state.setdefault("date_range", None)

    with st.sidebar.form("filters", clear_on_submit=False):
        nol = " WITH (NOLOCK)" if dirty_read else ""

        try:
            div_opts = ["All"] + distinct_division(nolock=nol)
        except Exception as e:
            st.error(f"Database error: {str(e)}")
            st.stop()

        div_val = st.selectbox(
            "Division", div_opts,
            index=div_opts.index(st.session_state.get("selected_div","All")) if st.session_state.get("selected_div","All") in div_opts else 0
        )

        amo_opts = ["All"] + distinct_amo(div_val, nolock=nol)
        amo_default = st.session_state.get("selected_amo","All")
        if amo_default not in amo_opts: 
            amo_default = "All"
        amo_val = st.selectbox("AMO", amo_opts, index=amo_opts.index(amo_default))

        wh_opts = ["All"] + distinct_wh(div_val, amo_val, nolock=nol)
        wh_default = st.session_state.get("selected_wh","All")
        if wh_default not in wh_opts: 
            wh_default = "All"
        wh_val = st.selectbox("WH_Name", wh_opts, index=wh_opts.index(wh_default))

        spv_opts = ["All"] + distinct_spv(div_val, amo_val, wh_val, nolock=nol)
        spv_default = st.session_state.get("selected_spv","All")
        if spv_default not in spv_opts: 
            spv_default = "All"
        spv_val = st.selectbox("Supervisor", spv_opts, index=spv_opts.index(spv_default))

        region_opts = ["All"] + distinct_region(div_val, amo_val, wh_val, spv_val, nolock=nol)
        region_default = st.session_state.get("selected_region","All")
        if region_default not in region_opts: 
            region_default = "All"
        region_val = st.selectbox("Region", region_opts, index=region_opts.index(region_default))

        min_d, max_d = date_range(div_val, amo_val, wh_val, spv_val, region_val, nolock=nol)
        dr_default = st.session_state.get("date_range") or (min_d, max_d)
        ds, de = pd.to_datetime(dr_default[0]).date(), pd.to_datetime(dr_default[1]).date()
        ds = max(min_d, ds)
        de = min(max_d, de)
        date_val = st.date_input("Sales Date Range", value=(ds, de), min_value=min_d, max_value=max_d)

        submitted = st.form_submit_button("Apply filters")

    # safe init for first load
    if "last_df" not in st.session_state:
        st.session_state["last_df"] = None
    if "last_filters" not in st.session_state:
        st.session_state["last_filters"] = None

    # ============================ WHERE builder ============================
    def build_where(div_, amo_, wh_, spv_, region_, start_, end_):
        parts, p = [], {}
        if div_    != "All" and DIVISION_COL: 
            parts.append(f" AND {DIVISION_COL} = :division")
            p["division"] = div_
        if amo_    != "All" and AMO_COL:      
            parts.append(f" AND {AMO_COL} = :amo")
            p["amo"] = amo_
        if wh_     != "All" and WH_COL:       
            parts.append(f" AND {WH_COL} = :wh")
            p["wh"] = wh_
        if spv_    != "All" and SPV_COL:      
            parts.append(f" AND {SPV_COL} = :spv")
            p["spv"] = spv_
        if region_ != "All" and REGION_COL:   
            parts.append(f" AND {REGION_COL} = :region")
            p["region"] = region_
        end_next = pd.to_datetime(end_) + pd.Timedelta(days=1)
        if SALES_DATE_COL:
            p["start"] = pd.to_datetime(start_)
            p["end_next"] = end_next
            parts.append(f" AND {SALES_DATE_COL} >= :start AND {SALES_DATE_COL} < :end_next")
        return "".join(parts), p, end_next

    # ============================ Dynamic Column picking ============================
    TIME_COL   = pick_col(
        ["Travel_Transaction_Time_HHMMSS","Travel_Time_HHMMSS","Travel_Transaction_Time",
        "TravelTime","Travel_Time","TransTime","Travel_Trans_HHMMSS","TT_HHMMSS"],
        ALL_COLS
    )
    DIST_COL   = pick_col(["Distance_Outlet_m","Distance_m","Distance","Outlet_Distance_m"], ALL_COLS)
    RADIUS_COL = pick_col(["Radius_MNT_m","Radius_m","Radius"], ALL_COLS)

    preferred_cols = [
        "Sequence","Visit_Time","Latitude","Longitude","Customer_Code","Store_Name","Visit_Div_",
        "Pack_Regular","Pack_NPL", TIME_COL, DIST_COL, RADIUS_COL,
        "Google_Maps","Fake_Indication","Outlet_Address","Outlet_Latitude","Outlet_Longitude",
        # optional MNT columns for map
        "MNT_Latitude","Latitude_MNT","MNT_Lat","Lat_MNT","MNTLatitude",
        "MNT_Longitude","Longitude_MNT","MNT_Long","Lon_MNT","Long_MNT","MNTLongitude"
    ]
    RAW_COLS = [c for c in preferred_cols if c and (c in ALL_COLS)]
    if not RAW_COLS:
        st.error("No expected columns were found in dbo.SMI_Final. Please verify table schema.")
        st.stop()
    select_list = ", ".join(RAW_COLS)

    # ============================ Data fetchers ============================
    nolock = " WITH (NOLOCK)" if dirty_read else ""

    @st.cache_data(ttl=300, show_spinner=True)
    def fetch_rows(select_list, where_sql, params, cap, nolock=""):
        try:
            order_col = SALES_DATE_COL if SALES_DATE_COL else "Visit_Time"
            query = f"""
                SELECT {select_list}
                FROM {TABLE}{nolock}
                WHERE 1=1 {where_sql}
                ORDER BY {order_col} DESC
                OFFSET 0 ROWS FETCH NEXT :cap ROWS ONLY
            """
            p = dict(params)
            p["cap"] = int(cap)
            
            with engine.connect() as conn:
                if p:
                    return pd.read_sql_query(text(query), conn, params=p)
                else:
                    return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Error fetching data: {str(e)}")
            return pd.DataFrame()

    @st.cache_data(ttl=300, show_spinner=True)
    def fetch_rows_full(select_list, where_sql, params, nolock=""):
        try:
            query = f"SELECT {select_list} FROM {TABLE}{nolock} WHERE 1=1 {where_sql}"
            with engine.connect() as conn:
                if params:
                    return pd.read_sql_query(text(query), conn, params=params)
                else:
                    return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Error fetching full data: {str(e)}")
            return pd.DataFrame()

    @st.cache_data(ttl=300, show_spinner=False)
    def fetch_route_summary_sql(where_sql, params, time_col=None, nolock=""):
        try:
            if time_col:
                avg_part = f"AVG(DATEDIFF(SECOND, 0, TRY_CONVERT(time(0), {time_col}))) AS AvgSecs"
            else:
                avg_part = "CAST(NULL AS float) AS AvgSecs"
            
            radius_col = RADIUS_COL or "'NULL'"
            dist_col = DIST_COL or "'NULL'"
            
            query = f"""
            SELECT
            MIN(Visit_Time) AS FirstVisit,
            MAX(Visit_Time) AS LastVisit,
            SUM(CASE WHEN TRY_CONVERT(float, Fake_Indication) > 1 THEN 1 ELSE 0 END) AS FakeIndCnt,
            SUM(CASE WHEN TRY_CONVERT(float, {radius_col}) > 300 THEN 1 ELSE 0 END)  AS RadiusGT300,
            AVG(TRY_CONVERT(float, {dist_col})) AS AvgDist,
            {avg_part}
            FROM {TABLE}{nolock}
            WHERE 1=1 {where_sql}
            """
            
            with engine.connect() as conn:
                if params:
                    row = pd.read_sql_query(text(query), conn, params=params)
                else:
                    row = pd.read_sql_query(query, conn)
            
            if row.empty:
                return {"FirstVisit":"", "LastVisit":"", "FakeIndCnt":0, "RadiusGT300":0, "AvgDist":None, "AvgSecs":None}
            return row.iloc[0].to_dict()
        except Exception as e:
            st.error(f"Error fetching route summary: {str(e)}")
            return {"FirstVisit":"", "LastVisit":"", "FakeIndCnt":0, "RadiusGT300":0, "AvgDist":None, "AvgSecs":None}

    # ============================ Session flow ============================
    if submitted:
        st.session_state["selected_div"]    = div_val
        st.session_state["selected_amo"]    = amo_val
        st.session_state["selected_wh"]     = wh_val
        st.session_state["selected_spv"]    = spv_val
        st.session_state["selected_region"] = region_val
        st.session_state["date_range"]      = date_val

        start_date, end_date = date_val[0], date_val[1]
        selected_div, selected_amo = div_val, amo_val
        selected_wh, selected_spv, selected_region = wh_val, spv_val, region_val

        where_sql, params, end_next = build_where(selected_div, selected_amo, selected_wh, selected_spv, selected_region, start_date, end_date)
        
        df = fetch_rows(select_list, where_
