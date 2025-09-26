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
        
        df = fetch_rows(select_list, where_sql, params, cap=max_rows, nolock=nolock)
        
        if df.empty:
            st.warning("No data found for the selected filters.")
            st.stop()

        st.session_state["last_df"] = df
        st.session_state["last_filters"] = dict(
            selected_div=selected_div, selected_amo=selected_amo, selected_wh=selected_wh,
            selected_spv=selected_spv, selected_region=selected_region,
            start_date=start_date, end_date=end_date, end_next=end_next, dirty_read=dirty_read
        )
    else:
        if st.session_state["last_df"] is None:
            st.info("Choose filters then click **Apply filters** to load rows.")
            st.stop()
        df = st.session_state["last_df"]
        f = st.session_state["last_filters"] or {}
        selected_div    = f.get("selected_div", st.session_state["selected_div"])
        selected_amo    = f.get("selected_amo", st.session_state["selected_amo"])
        selected_wh     = f.get("selected_wh", st.session_state["selected_wh"])
        selected_spv    = f.get("selected_spv", st.session_state["selected_spv"])
        selected_region = f.get("selected_region", st.session_state["selected_region"])
        start_date      = f.get("start_date", (st.session_state["date_range"] or [None, None])[0])
        end_date        = f.get("end_date", (st.session_state["date_range"] or [None, None])[1])
        end_next        = f.get("end_next", pd.to_datetime(end_date) + pd.Timedelta(days=1))
        dirty_read      = f.get("dirty_read", dirty_read)
        where_sql, params, _ = build_where(selected_div, selected_amo, selected_wh, selected_spv, selected_region, start_date, end_date)

    # ============================ Rename & Clean ============================
    rename_map = {
        "Visit_Time": "Visit Time",
        "Latitude": "Latitude",
        "Longitude": "Longitude",
        "Customer_Code": "Customer Code",
        "Store_Name": "Store Name",
        "Visit_Div_": "Visit Division",
        "Pack_Regular": "Regular Pack",
        "Pack_NPL": "NPL Pack",
        "Google_Maps": "Google Maps",
        "Fake_Indication": "Fake Indication",
        "Outlet_Address": "Outlet Address",
    }
    if TIME_COL:   
        rename_map[TIME_COL] = "Travel Time (HH:MM:SS)"
    if DIST_COL:   
        rename_map[DIST_COL] = "Distance (m)"
    if RADIUS_COL: 
        rename_map[RADIUS_COL] = "Radius MNT (m)"

    df = df.rename(columns=rename_map)

    # sort by Sequence if present
    if "Sequence" in df.columns:
        df["Sequence"] = pd.to_numeric(df["Sequence"], errors="coerce")
        df = df.sort_values("Sequence").reset_index(drop=True)

    # numeric transforms (round to 0; display later with thousand separators)
    if "Distance (m)" in df.columns:
        df["Distance (m)"] = df["Distance (m)"].astype(str).str.replace(r"[^0-9.]", "", regex=True).replace("", None)
        df["Distance (m)"] = pd.to_numeric(df["Distance (m)"], errors="coerce").round(0)

    if "Radius MNT (m)" in df.columns:
        df["Radius MNT (m)"] = pd.to_numeric(df["Radius MNT (m)"], errors="coerce").round(0)

    # display order
    display_order = [
        "Sequence","Visit Time","Latitude","Longitude","Customer Code","Store Name","Visit Division",
        "Regular Pack","NPL Pack","Travel Time (HH:MM:SS)","Distance (m)","Radius MNT (m)",
        "Google Maps","Fake Indication","Outlet Address"
    ]
    df_view = df[[c for c in display_order if c in df.columns]]

    # make a display copy with thousand separators for fast_mode too
    df_view_display = df_view.copy()
    for c in ("Distance (m)", "Radius MNT (m)"):
        if c in df_view_display.columns:
            df_view_display[c] = df_view_display[c].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")

    # ============================ Sales Monitoring ============================
    @st.cache_data(ttl=300, show_spinner=False)
    def load_cmec_summary(start_date, end_next, org_name=None, region_code=None, dirty=False):
        try:
            nolock2 = " WITH (NOLOCK)" if dirty else ""
            base = f"""
                SELECT 
                    SUM(CAST(Tetap_outlet AS float))  AS Target_Call,
                    SUM(CAST(CM_Tetap_1_ AS float))   AS CM_Tetap_1,
                    SUM(CAST(EC_Tetap_2_ AS float))   AS EC_Tetap_2,
                    SUM(CAST(CM_Dummy_3_ AS float))   AS CM_Dummy_3,
                    SUM(CAST(EC_Dummy_4_ AS float))   AS EC_Dummy_4,
                    SUM(CAST(Sales_Qty_  AS float))   AS Sales_Qty
                FROM dbo.cmec{nolock2}
                WHERE Sales_Date >= :start AND Sales_Date < :end_next
            """
            where = []
            p = {"start": pd.to_datetime(start_date), "end_next": pd.to_datetime(end_next)}
            if org_name and org_name != "All": 
                where.append("Org_Name = :org")
                p["org"] = org_name
            if region_code and region_code != "All": 
                where.append("Region_Code = :region")
                p["region"] = region_code
            if where: 
                base += " AND " + " AND ".join(where)
            
            with engine.connect() as conn:
                row = pd.read_sql_query(text(base), conn, params=p)
            
            if row.empty:
                return dict(Target_Call=0, CM_Tetap_1=0, EC_Tetap_2=0, CM_Dummy_3=0, EC_Dummy_4=0, Sales_Qty=0)
            return row.iloc[0].fillna(0).to_dict()
        except Exception:
            return dict(Target_Call=0, CM_Tetap_1=0, EC_Tetap_2=0, CM_Dummy_3=0, EC_Dummy_4=0, Sales_Qty=0)

    cm = load_cmec_summary(start_date, end_next, selected_amo, selected_region, dirty=dirty_read)

    cust = df.get("Customer Code", pd.Series(dtype=str)).astype(str).fillna("")
    is_dummy = cust.str.startswith("9")
    has_npl_gt1 = df.get("NPL Pack", pd.Series(0, index=df.index)).fillna(0).astype(float) > 1

    reg_ec_npl = int(((~is_dummy) & has_npl_gt1).sum())
    dum_ec_npl = int((is_dummy & has_npl_gt1).sum())

    reg_vol_npl = int(df.loc[~is_dummy, "NPL Pack"].fillna(0).sum()) if "NPL Pack" in df.columns else 0
    dum_vol_npl = int(df.loc[ is_dummy, "NPL Pack"].fillna(0).sum()) if "NPL Pack" in df.columns else 0

    reg_vol_total = int(
        df.loc[~is_dummy, "Regular Pack"].fillna(0).sum()
        + df.loc[~is_dummy, "NPL Pack"].fillna(0).sum()
    ) if set(["Regular Pack","NPL Pack"]).issubset(df.columns) else 0

    dum_vol_total = int(
        df.loc[ is_dummy, "Regular Pack"].fillna(0).sum()
        + df.loc[ is_dummy, "NPL Pack"].fillna(0).sum()
    ) if set(["Regular Pack","NPL Pack"]).issubset(df.columns) else 0

    def safe_div(a, b):
        try:
            a = float(a)
            b = float(b)
            return 0.0 if b == 0 else round(a / b, 2)
        except Exception:
            return 0.0

    reg = {
        "Outlet Type": "Registered Outlet",
        "Target Call": int(cm["Target_Call"] or 0),
        "CM":          int(cm["CM_Tetap_1"] or 0),
        "EC Total":    int(cm["EC_Tetap_2"] or 0),
        "EC NPL":      reg_ec_npl,
        "Drop Size":   safe_div(cm["Sales_Qty"], cm["EC_Tetap_2"]),
        "Volume Total": reg_vol_total,
        "Volume NPL":   reg_vol_npl,
    }
    dum = {
        "Outlet Type": "Dummy",
        "Target Call": "",
        "CM":          int(cm["CM_Dummy_3"] or 0),
        "EC Total":    int(cm["EC_Dummy_4"] or 0),
        "EC NPL":      dum_ec_npl,
        "Drop Size":   safe_div(cm["Sales_Qty"], cm["EC_Dummy_4"]),
        "Volume Total": dum_vol_total,
        "Volume NPL":   dum_vol_npl,
    }
    tot = {
        "Outlet Type": "Total",
        "Target Call": int(cm["Target_Call"] or 0),
        "CM":          int((cm["CM_Tetap_1"] or 0) + (cm["CM_Dummy_3"] or 0)),
        "EC Total":    int((cm["EC_Tetap_2"] or 0) + (cm["EC_Dummy_4"] or 0)),
        "EC NPL":      int(reg_ec_npl + dum_ec_npl),
        "Drop Size":   safe_div(cm["Sales_Qty"], ((cm["EC_Tetap_2"] or 0) + (cm["EC_Dummy_4"] or 0))),
        "Volume Total": int(reg_vol_total + dum_vol_total),
        "Volume NPL":   int(reg_vol_npl + dum_vol_npl),
    }

    # ============================ Center cards ============================
    center_col = st.columns([1, 2.4, 1])[1]

    with center_col:
        st.markdown('<div class="card"><h4>Sales Monitoring</h4>', unsafe_allow_html=True)
        sku_active_text = "SOON"
        sales_html = f"""
        <table class="ktg-table">
        <thead>
            <tr>
            <th class="text-left">Outlet Type</th>
            <th>Target Call</th><th>CM</th><th>EC Total</th><th>EC NPL</th>
            <th>Drop Size (Per Outlet)</th><th>Volume Total</th><th>Volume NPL</th><th>SKU Active</th>
            </tr>
        </thead>
        <tbody>
            <tr>
            <td class="text-left">{reg['Outlet Type']}</td>
            <td>{reg['Target Call']:,}</td><td>{reg['CM']:,}</td><td>{reg['EC Total']:,}</td><td>{reg['EC NPL']:,}</td>
            <td>{reg['Drop Size']}</td><td>{reg['Volume Total']:,}</td><td>{reg['Volume NPL']:,}</td>
            <td rowspan="2">{sku_active_text}</td>
            </tr>
            <tr>
            <td class="text-left">{dum['Outlet Type']}</td>
            <td></td><td>{dum['CM']:,}</td><td>{dum['EC Total']:,}</td><td>{dum['EC NPL']:,}</td>
            <td>{dum['Drop Size']}</td><td>{dum['Volume Total']:,}</td><td>{dum['Volume NPL']:,}</td>
            </tr>
        </tbody>
        <tfoot>
            <tr>
            <td class="text-left">{tot['Outlet Type']}</td>
            <td>{tot['Target Call']:,}</td><td>{tot['CM']:,}</td><td>{tot['EC Total']:,}</td><td>{tot['EC NPL']:,}</td>
            <td>{tot['Drop Size']}</td><td>{tot['Volume Total']:,}</td><td>{tot['Volume NPL']:,}</td><td></td>
            </tr>
        </tfoot>
        </table>
        """
        st.markdown(sales_html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ============================ Route Tracking ============================
    rs = fetch_route_summary_sql(where_sql, params, time_col=TIME_COL, nolock=nolock)

    avg_tt = ""
    if rs.get("AvgSecs") is not None and not pd.isna(rs["AvgSecs"]):
        total_sec = int(rs["AvgSecs"])
        h = total_sec//3600
        m = (total_sec%3600)//60
        s = total_sec%60
        avg_tt = f"{h:02d}:{m:02d}:{s:02d}"
    else:
        source_col = None
        if "Travel Time (HH:MM:SS)" in df.columns:
            source_col = "Travel Time (HH:MM:SS)"
        elif TIME_COL and TIME_COL in df.columns:
            source_col = TIME_COL
        if source_col:
            secs = df[source_col].apply(lambda x: None if pd.isna(x) else _parse_hhmmss_to_seconds(x)).dropna()
            if len(secs):
                mean = int(np.mean(secs))
                h=mean//3600
                m=(mean%3600)//60
                s=mean%60
                avg_tt = f"{h:02d}:{m:02d}:{s:02d}"

    avg_dist = ""
    if rs.get("AvgDist") is not None and not pd.isna(rs["AvgDist"]):
        try:
            avg_dist = f"{int(round(float(rs['AvgDist']))):,}"
        except Exception:
            avg_dist = ""

    route_summary = {
        "Working Time": "",
        "First Outlet": str(rs.get("FirstVisit") or ""),
        "Last Outlet":  str(rs.get("LastVisit") or ""),
        "Fake Indication": int(rs.get("FakeIndCnt") or 0),
        "Average Travel & Transaction Time": avg_tt,
        "Radius MNT > 300": int(rs.get("RadiusGT300") or 0),
        "Average Distance per Outlet (m)": avg_dist
    }
    
    try:
        if route_summary["First Outlet"] and route_summary["Last Outlet"]:
            first_time = pd.to_datetime(route_summary["First Outlet"])
            last_time = pd.to_datetime(route_summary["Last Outlet"])
            route_summary["Working Time"] = str(last_time - first_time)
    except Exception:
        pass

    rg300_val = route_summary.get('Radius MNT > 300', '')
    rg300_str = f"{rg300_val:,}" if isinstance(rg300_val, (int, float)) and not pd.isna(rg300_val) else ""

    with center_col:
        st.markdown('<div class="card"><h4>Route Tracking</h4>', unsafe_allow_html=True)
        rt_html = f"""
        <table class="ktg-table">
        <thead>
            <tr>
            <th>Working Time</th><th>First Outlet</th><th>Fake Indication</th>
            <th>Average Travel & Transaction Time</th><th>Radius MNT > 300</th>
            <th>Average Distance per Outlet (m)</th><th>Last Outlet</th><th>Upload Time</th>
            </tr>
        </thead>
        <tbody>
            <tr>
            <td>{route_summary.get('Working Time','')}</td>
            <td>{route_summary.get('First Outlet','')}</td>
            <td>{route_summary.get('Fake Indication','')}</td>
            <td>{route_summary.get('Average Travel & Transaction Time','')}</td>
            <td>{rg300_str}</td>
            <td>{route_summary.get('Average Distance per Outlet (m)','')}</td>
            <td>{route_summary.get('Last Outlet','')}</td>
            <td></td>
            </tr>
        </tbody>
        </table>
        """
        st.markdown(rt_html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Visit Details</div>', unsafe_allow_html=True)

    def highlight_cells(val, col_name):
        try:
            v = float(val)
            if col_name == "NPL Pack" and v == 0:
                return "background-color:#e53935;color:white"
            if col_name == "Radius MNT (m)" and v > 300:
                return "background-color:#e53935;color:white"
            if col_name == "Distance (m)" and v > 3000:
                return "background-color:#e53935;color:white"
        except Exception:
            pass
        return ""

    def style_df(df_):
        sty = df_.style.hide(axis="index")
        if "NPL Pack" in df_.columns:       
            sty = sty.applymap(lambda v: highlight_cells(v, "NPL Pack"), subset=["NPL Pack"])
        if "Radius MNT (m)" in df_.columns: 
            sty = sty.applymap(lambda v: highlight_cells(v, "Radius MNT (m)"), subset=["Radius MNT (m)"])
        if "Distance (m)" in df_.columns:   
            sty = sty.applymap(lambda v: highlight_cells(v, "Distance (m)"), subset=["Distance (m)"])
        fmt = {}
        if "Distance (m)" in df_.columns:   
            fmt["Distance (m)"] = "{:,.0f}"
        if "Radius MNT (m)" in df_.columns: 
            fmt["Radius MNT (m)"] = "{:,.0f}"
        return sty.format(fmt, na_rep="")

    if fast_mode or len(df_view) > 30000:
        st.dataframe(df_view_display, use_container_width=True, height=600)
    else:
        html = style_df(df_view).to_html()
        st.markdown('<div id="sql-table">', unsafe_allow_html=True)
        st.markdown(html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    @st.cache_data(ttl=300)
    def to_csv_bytes(frame: pd.DataFrame) -> bytes:
        df_out = frame.copy()
        for c in ("Distance (m)","Radius MNT (m)"):
            if c in df_out.columns:
                df_out[c] = pd.to_numeric(df_out[c], errors="coerce").round(0).astype("Int64")
        return df_out.to_csv(index=False).encode("utf-8")

    @st.cache_data(ttl=300)
    def to_xlsx_bytes(visit_df: pd.DataFrame, sales_rows: list, route_summary: dict) -> bytes:
        sales_df = pd.DataFrame(sales_rows)
        route_df = pd.DataFrame([route_summary])
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            visit_df_x = visit_df.copy()
            for c in ("Distance (m)","Radius MNT (m)"):
                if c in visit_df_x.columns:
                    visit_df_x[c] = pd.to_numeric(visit_df_x[c], errors="coerce").round(0)
            sales_df.to_excel(writer, index=False, sheet_name="Sales Monitoring")
            route_df.to_excel(writer, index=False, sheet_name="Route Tracking")
            visit_df_x.to_excel(writer, index=False, sheet_name="Visit Details")

            for name, df_ in [("Sales Monitoring", sales_df), ("Route Tracking", route_df), ("Visit Details", visit_df_x)]:
                ws = writer.sheets[name]
                for i, col in enumerate(df_.columns):
                    width = min(45, max(10, int(df_[col].astype(str).str.len().clip(upper=80).mean()) + 4))
                    ws.set_column(i, i, width)
                if name == "Visit Details":
                    fmt = writer.book.add_format({'num_format': '#,##0'})
                    for i, col in enumerate(df_.columns):
                        if col in ("Distance (m)","Radius MNT (m)"):
                            ws.set_column(i, i, None, fmt)

        output.seek(0)
        return output.getvalue()

    sales_rows = [
        {**reg, "SKU Active": "SOON"},
        {**dum, "SKU Active": ""},
        {**tot, "SKU Active": ""},
    ]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("Download CSV (current)", data=to_csv_bytes(df_view), 
                          file_name="SMI_Final_filtered.csv", mime="text/csv")
    with c2:
        st.download_button("Download XLSX (current)", data=to_xlsx_bytes(df_view, sales_rows, route_summary), 
                          file_name="SMART_Report.xlsx", 
                          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with c3:
        if st.button("Download FULL XLSX (no cap)"):
            df_full = fetch_rows_full(select_list, where_sql, params, nolock=nolock).rename(columns=rename_map)
            if "Distance (m)" in df_full.columns:
                df_full["Distance (m)"] = pd.to_numeric(
                    df_full["Distance (m)"].astype(str).str.replace(r"[^0-9.]", "", regex=True).replace("", None), 
                    errors="coerce").round(0)
            if "Radius MNT (m)" in df_full.columns:
                df_full["Radius MNT (m)"] = pd.to_numeric(df_full["Radius MNT (m)"], errors="coerce").round(0)
            df_full_view = df_full[[c for c in display_order if c in df_full.columns]]
            st.download_button(
                "Download prepared file",
                data=to_xlsx_bytes(df_full_view, sales_rows, route_summary),
                file_name="SMART_Report_FULL.xlsx",
                mime="application/vnd.openxmlformsets-officedocument.spreadsheetml.sheet",
                key="dl_full_xlsx"
            )

    st.markdown('<div class="section-title">Salesman Route vs Outlet Map</div>', unsafe_allow_html=True)

    # ---- Sidebar controls for visibility ----
    st.sidebar.subheader("Map Visibility Controls")
    tile_choice = st.sidebar.selectbox("Base map", ["CartoDB Positron", "OpenStreetMap"], index=0)
    route_weight = st.sidebar.slider("Route line weight", 3, 12, 6)
    route_opacity = st.sidebar.slider("Route opacity", 0.3, 1.0, 0.9)
    sales_size = st.sidebar.slider("Salesman marker size", 6, 36, 16)
    outlet_size = st.sidebar.slider("Outlet marker size", 4, 28, 12)
    mnt_size = st.sidebar.slider("MNT marker size", 10, 48, 28)
    label_font = st.sidebar.slider("Sequence label font size (px)", 10, 28, 16)

    st.sidebar.markdown("---")
    st.sidebar.caption("Optional: use a logo instead of dots (PNG/SVG recommended)")
    use_logos = st.sidebar.checkbox("Use logos for markers", value=False)

    sales_logo_file = st.sidebar.file_uploader("Salesman logo (PNG/SVG)", type=["png", "svg"])
    sales_logo_url = st.sidebar.text_input("...or Salesman logo URL")
    outlet_logo_file = st.sidebar.file_uploader("Outlet logo (PNG/SVG)", type=["png", "svg"])
    outlet_logo_url = st.sidebar.text_input("...or Outlet logo URL")
    mnt_logo_file = st.sidebar.file_uploader("MNT logo (PNG/SVG)", type=["png", "svg"])
    mnt_logo_url = st.sidebar.text_input("...or MNT logo URL")

    def _logo_icon(uploaded_file, url_str, size):
        """Return a folium CustomIcon if a file or URL is provided; else None."""
        if uploaded_file is not None:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix="." + uploaded_file.name.split(".")[-1])
            tmp.write(uploaded_file.getbuffer())
            tmp.flush()
            return folium.features.CustomIcon(tmp.name, icon_size=(size, size), icon_anchor=(size // 2, size // 2))
        if url_str:
            return folium.features.CustomIcon(url_str, icon_size=(size, size), icon_anchor=(size // 2, size // 2))
        return None

    sales_icon = _logo_icon(sales_logo_file, sales_logo_url, sales_size)
    outlet_icon = _logo_icon(outlet_logo_file, outlet_logo_url, outlet_size)
    mnt_icon = _logo_icon(mnt_logo_file, mnt_logo_url, mnt_size)

    required = ["Latitude", "Longitude", "Customer Code", "Store Name", "Visit Time", "Visit Division"]
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        st.error(f"Missing columns: {missing}")
    else:
        df_map = df.dropna(subset=["Latitude", "Longitude"], how="any").copy()
        if df_map.empty:
            st.warning("No valid salesman coordinates found.")
        else:
            if "Sequence" in df_map.columns:
                df_map["Sequence"] = pd.to_numeric(df_map["Sequence"], errors="coerce")
                df_map["Visit Time"] = pd.to_datetime(df_map["Visit Time"], errors="coerce")
                df_map = df_map.sort_values(["Sequence", "Visit Time"], na_position="last")
            else:
                df_map["Visit Time"] = pd.to_datetime(df_map["Visit Time"], errors="coerce")
                df_map = df_map.sort_values("Visit Time")

            df_map = df_map.head(100)
            if len(df_map) == 100:
                st.info("Showing first 100 points for performance.")

            mnt_lat_col = next((c for c in ["MNT_Latitude", "Latitude_MNT", "MNT_Lat", "Lat_MNT", "MNTLatitude"] if c in df.columns), None)
            mnt_lon_col = next((c for c in ["MNT_Longitude", "Longitude_MNT", "MNT_Long", "Lon_MNT", "Long_MNT", "MNTLongitude"] if c in df.columns), None)

            start_coords = [df_map.iloc[0]["Latitude"], df_map.iloc[0]["Longitude"]]
            tiles = {"CartoDB Positron": "CartoDB positron", "OpenStreetMap": "OpenStreetMap"}[tile_choice]
            m = folium.Map(location=start_coords, zoom_start=12, tiles=tiles, control_scale=True)

            # animated path
            salesman_route = df_map[["Latitude", "Longitude"]].dropna().values.tolist()
            if len(salesman_route) > 1:
                AntPath(
                    locations=salesman_route,
                    delay=700, dash_array=[6, 12],
                    color="#d32f2f", pulse_color="#ff8a80",
                    weight=route_weight, opacity=route_opacity
                ).add_to(m)

            # helper to add a label just above a point
            def add_number_label(lat, lon, text):
                if not text:
                    return
                y_offset = max(18, int(sales_size * 1.2))
                folium.map.Marker(
                    [lat, lon],
                    icon=folium.DivIcon(
                        html=f'''
                            <div style="
                                transform: translate(-6px, -{y_offset}px);
                                font-weight:900; font-size:{label_font}px; color:#d32f2f;
                                text-shadow:
                                    -1px -1px 0 #fff, 1px -1px 0 #fff,
                                    -1px  1px 0 #fff, 1px  1px 0 #fff,
                                    0 0 6px #fff;">
                            {text}
                            </div>'''
                    )
                ).add_to(m)

            # salesman markers + labels
            seq_vals = df_map["Sequence"].tolist() if "Sequence" in df_map.columns else list(range(1, len(df_map) + 1))
            for (idx, row), num in zip(df_map.iterrows(), seq_vals):
                if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]):
                    latlon = [row["Latitude"], row["Longitude"]]
                    label = f"{int(num)}" if pd.notna(num) else ""

                    # Popup with store name and sequence number
                    popup_html = (
                        f"<b>Seq:</b> {label}<br>"
                        f"<b>Store:</b> {row.get('Store Name', '')}<br>"
                        f"<b>Visit Status:</b> {row.get('Visit Division', '')}<br>"
                        f"<b>Time:</b> {row.get('Visit Time', '')}<br>"
                        f"<b>Customer Code:</b> {row.get('Customer Code', '')}"
                    )

                    # Adding both sequence number and store name in tooltip
                    if use_logos and sales_icon is not None:
                        folium.Marker(
                            location=latlon,
                            tooltip=f"#{label} → {row.get('Store Name', '')}",
                            popup=popup_html,
                            icon=sales_icon
                        ).add_to(m)
                    else:
                        folium.CircleMarker(
                            location=latlon,
                            radius=sales_size,
                            color="#d32f2f",
                            fill=True, fill_opacity=0.95,
                            tooltip=f"#{label} → {row.get('Store Name', '')}",
                            popup=popup_html,
                        ).add_to(m)

                    # sequence number label (text only)
                    add_number_label(latlon[0], latlon[1], f"{label} {row.get('Store Name', '')}")

            # outlet markers (logo or bigger blue dots)
            if {"Outlet_Latitude", "Outlet_Longitude"}.issubset(df.columns):
                for _, row in df_map.iterrows():
                    olat, olon = row.get("Outlet_Latitude"), row.get("Outlet_Longitude")
                    if pd.notna(olat) and pd.notna(olon):
                        # Popup with store name and outlet information
                        popup_html = f"<b>Outlet</b><br>Code: {row.get('Customer Code', '')}<br>Store: {row.get('Store Name', '')}"
                        if use_logos and outlet_icon is not None:
                            folium.Marker(
                                [olat, olon],
                                tooltip=f"Outlet → {row.get('Store Name', '')}",
                                popup=popup_html,
                                icon=outlet_icon
                            ).add_to(m)
                        else:
                            folium.CircleMarker(
                                location=[olat, olon],
                                radius=outlet_size,
                                color="#1976d2",
                                fill=True, fill_opacity=0.9,
                                popup=popup_html,
                                tooltip=f"Outlet → {row.get('Store Name', '')}",
                            ).add_to(m)

            # BIG MNT markers (logo or large green)
            if mnt_lat_col and mnt_lon_col:
                for _, row in df_map.iterrows():
                    mlat, mlon = row.get(mnt_lat_col), row.get(mnt_lon_col)
                    if pd.notna(mlat) and pd.notna(mlon):
                        # Popup with store name and MNT information
                        popup_html = f"<b>MNT</b><br>Code: {row.get('Customer Code', '')}<br>Store: {row.get('Store Name', '')}"
                        if use_logos and mnt_icon is not None:
                            folium.Marker(
                                [mlat, mlon],
                                tooltip=f"MNT → {row.get('Store Name', '')}",
                                popup=popup_html,
                                icon=mnt_icon
                            ).add_to(m)
                        else:
                            folium.CircleMarker(
                                location=[mlat, mlon],
                                radius=mnt_size,
                                color="#2e7d32",
                                fill=True, fill_opacity=0.95,
                                popup=popup_html,
                                tooltip=f"MNT → {row.get('Store Name', '')}",
                            ).add_to(m)

            # Fit to all visible points (salesman + outlets + MNT)
            all_pts = []
            all_pts += [[r["Latitude"], r["Longitude"]] for _, r in df_map.iterrows() 
                       if pd.notna(r["Latitude"]) and pd.notna(r["Longitude"])]
            if {"Outlet_Latitude", "Outlet_Longitude"}.issubset(df.columns):
                all_pts += [[r["Outlet_Latitude"], r["Outlet_Longitude"]] for _, r in df_map.iterrows()
                           if pd.notna(r.get("Outlet_Latitude")) and pd.notna(r.get("Outlet_Longitude"))]
            if mnt_lat_col and mnt_lon_col:
                all_pts += [[r[mnt_lat_col], r[mnt_lon_col]] for _, r in df_map.iterrows()
                           if pd.notna(r.get(mnt_lat_col)) and pd.notna(r.get(mnt_lon_col))]
            if all_pts:
                m.fit_bounds(all_pts, padding=(30, 30))

            st_folium(m, use_container_width=True, height=700)

# Main app logic
def main():
    if 'logged_in' not in st.session_state or not st.session_state.logged_in:
        login()  # If not logged in, show the login page
    else:
        SMART()  # If logged in, show the dashboard

# Run the app
if __name__ == "__main__":
    main()
