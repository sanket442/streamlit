import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta

# ---------------------------------------------------------
# GOOGLE SHEET AUTH (Placeholder for deployment)
# ---------------------------------------------------------
try:
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        st.secrets["google"],
        scope
    )
    client = gspread.authorize(creds)
except (KeyError, FileNotFoundError, AttributeError):
    client = None

# ---------------------------------------------------------
# LOAD SHEET AND DATA CLEANING
# ---------------------------------------------------------
@st.cache_data(ttl=600) # Cache data for 10 minutes to reduce API calls
def load_data():
    """Loads and cleans data from Google Sheet."""
    
    # Define expected columns for cleaning/checks
    EXPECTED_DATE_COLS = ["ODR DATE", "DUE DATE"]
    EXPECTED_NUMERIC_COLS = ["ORD WT", "ON_TIME DEL", "LATE_DEL"]
    
    if client is None:
        # Fallback to dummy data structure if creds are missing for demonstration
        df_data = {
            "CONT.PERSON": ["John", "Jane", "John", "Alice", "Jane", "Alice", "John", "Jane", "Bob", "Alice"],
            "ORD WT": [1000.0, 500.0, 2000.0, 1500.0, 800.0, 1200.0, 1800.0, 900.0, 1100.0, 1300.0],
            "ON_TIME DEL": [800.0, 400.0, 0.0, 1000.0, 800.0, 1000.0, 1500.0, 900.0, 1000.0, 1300.0],
            "LATE_DEL": [50.0, 0.0, 1500.0, 500.0, 0.0, 200.0, 0.0, 0.0, 0.0, 0.0],
            "ODR DATE": ["2025-01-10", "2025-01-15", "2025-03-01", "2025-04-05", "2025-05-20", "2025-06-10", "2025-07-01", "2025-08-01", "2025-08-15", "2025-09-01"], 
            "DUE DATE": ["2025-01-20", "2025-01-25", "2025-03-10", "2025-04-15", "2025-05-30", "2025-06-25", "2025-07-15", "2025-08-05", "2025-08-25", "2025-09-10"],
            "LATE DELIVERY REASON": ["Raw Material Delay", "", "Production Issue", "Logistics", "Raw Material Delay", "", "Production Issue", "Logistics", "", "Production Issue"],
            "ITEM NAME": ["ROPE CHAIN", "M.CHAIN", "ROPE CHAIN", "BALL CHAIN", "M.CHAIN", "ROPE", "COCKTAIL", "ROPE", "MIX", "BALL CHAIN"],
            "ORD NO": ["P1001", "P1002", "P1003", "P1004", "P1005", "P1006", "P1007", "P1008", "P1009", "P1010"],
            "PURITY": ["22KT", "18KT", "22KT", "20KT", "18KT", "22KT", "21KT", "14KT", "22KT", "20KT"]
        }
        df = pd.DataFrame(df_data)
        st.info("Using dummy data. Real-time data loading requires 'st.secrets[\"google\"]'.")
    else:
        try:
            sheet = client.open("PRODUCTION_ORDER_STATUS_REPORT").worksheet("ORDER_SHEET")
            raw = sheet.get_all_values()
            df = pd.DataFrame(raw)
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Spreadsheet 'PRODUCTION_ORDER_STATUS_REPORT' not found. Check the name.")
            return pd.DataFrame()
        except gspread.exceptions.WorksheetNotFound:
            st.error("Worksheet 'ORDER_SHEET' not found. Check the name.")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"An error occurred while loading data: {e}")
            return pd.DataFrame()

        # ----- CLEAN HEADER and Data -----
        if df.empty or df.shape[0] < 2:
            st.warning("Dataframe is empty or has too few rows to determine the header.")
            return pd.DataFrame()
            
        df = df.dropna(axis=1, how="all")
        # Assuming 2nd row (index 1) is the actual header
        df.columns = df.iloc[1]
        df = df[2:].reset_index(drop=True)

    # 1. Deduplicate and strip whitespace from column names (CRITICAL FIX)
    df.columns = df.columns.astype(str).str.strip()
    df = df.loc[:, ~df.columns.duplicated()]

    # Convert numeric columns
    for col in EXPECTED_NUMERIC_COLS:
        if col in df.columns:
            # Attempt to clean up strings (like removing currency symbols or spaces) before conversion
            df[col] = df[col].astype(str).str.replace(r'[^\d\.\-]', '', regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            
    # Convert date columns
    for col in EXPECTED_DATE_COLS:
        if col in df.columns:
            # errors="coerce" turns invalid dates/blanks into NaT
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            st.warning(f"Required date column '{col}' was not found in the sheet.")
            
    
    # --- POST-PROCESSING CHECK AND CALCULATIONS ---
    if all(col in df.columns for col in EXPECTED_NUMERIC_COLS):
        # 1. Pending ORD = Total ORD WT - ON_TIME DEL - LATE_DEL
        df['PENDING ORD'] = df['ORD WT'] - df['ON_TIME DEL'] - df['LATE_DEL']
        # Ensure pending order weight is not negative
        df['PENDING ORD'] = np.where(df['PENDING ORD'] < 0, 0, df['PENDING ORD'])
        
        # 2. Late Delivery % Calculation
        df['LATE_DEL_%'] = np.where(
            df['ORD WT'] > 0, 
            (df['LATE_DEL'] / df['ORD WT']) * 100, 
            0.0
        )
    else:
        missing_cols = [col for col in EXPECTED_NUMERIC_COLS if col not in df.columns]
        if missing_cols:
            st.error(f"🛑 CRITICAL ERROR: The following required numeric column(s) are missing from your sheet: {', '.join(missing_cols)}")
        # If running with dummy data, this is unlikely to trigger unless column names are changed.

    # 3. Lead Time Calculation (DUE DATE - ODR DATE)
    if all(col in df.columns for col in ["DUE DATE", "ODR DATE"]):
        df['LEAD TIME (DAYS)'] = (df["DUE DATE"] - df["ODR DATE"]).dt.days
        
    else:
        st.warning("Cannot calculate 'LEAD TIME (DAYS)': Missing 'ODR DATE' or 'DUE DATE'.")
        # Ensure the column exists and is filled with NaN if calculation fails
        df['LEAD TIME (DAYS)'] = np.nan 

    return df

df_full = load_data()

# Identify which of the expected date columns were successfully loaded
DATE_COLS_LOADED = [col for col in ["ODR DATE", "DUE DATE"] if col in df_full.columns]
REQUIRED_MATRIX_COLS = ["ORD WT", "ON_TIME DEL", "LATE_DEL", "PENDING ORD", "CONT.PERSON"]


if df_full.empty:
    st.error("The dataframe is empty. Cannot continue.")
    st.stop()
    
# ---------------------------------------------------------
# UI TITLE AND SLICERS (Filters)
# ---------------------------------------------------------
st.title("🔥 Production Delivery Dashboard (Google Sheet Linked)")

# Add a sidebar refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

df_filtered = df_full.copy()

st.markdown("### 📅 Date Filters")

# --- Date Filter Logic ---
if not DATE_COLS_LOADED:
    st.info("No valid date columns (ODR DATE, DUE DATE) were found in your spreadsheet. Date filters are disabled.")
else:
    # Safely determine min/max date, handling NaT values
    all_valid_dates = pd.concat([df_full[col].dropna() for col in DATE_COLS_LOADED if col in df_full.columns])
    
    if all_valid_dates.empty:
        min_date = datetime.today().date() - timedelta(days=365)
        max_date = datetime.today().date()
        st.info("No valid date entries found in the loaded date columns. Using a default filter range (last 1 year).")
    else:
        min_date = all_valid_dates.min().date()
        max_date = all_valid_dates.max().date()
        
    filter_col1, filter_col2 = st.columns(2)

    # --- Slicer 1: Order Date Range ---
    if "ODR DATE" in DATE_COLS_LOADED:
        with filter_col1:
            st.markdown("##### Filter by Order Date")
            
            ord_date_min_valid = df_full["ODR DATE"].min()
            ord_date_max_valid = df_full["ODR DATE"].max()
            
            default_start_ord = min_date
            default_end_ord = max_date
            
            if pd.notna(ord_date_min_valid):
                default_start_ord = ord_date_min_valid.date()
            if pd.notna(ord_date_max_valid):
                default_end_ord = ord_date_max_valid.date()

            if default_start_ord > default_end_ord:
                 default_start_ord = default_end_ord - timedelta(days=30)
                 
            ord_date_range = st.date_input(
                "Order Date Range",
                value=(default_start_ord, default_end_ord),
                min_value=min_date,
                max_value=max_date,
                key='ord_date_slicer'
            )
            
            # Apply Filtering
            if len(ord_date_range) == 2:
                start_ord_date = pd.to_datetime(ord_date_range[0])
                end_ord_date = pd.to_datetime(ord_date_range[1]) + timedelta(days=1)
                df_filtered = df_filtered[
                    (df_filtered["ODR DATE"].notna()) & 
                    (df_filtered["ODR DATE"] >= start_ord_date) & 
                    (df_filtered["ODR DATE"] < end_ord_date)
                ]

    # --- Slicer 2: Due Date Range ---
    if "DUE DATE" in DATE_COLS_LOADED:
        with filter_col2:
            st.markdown("##### Filter by Due Date")
            
            due_date_min_valid = df_full["DUE DATE"].min()
            due_date_max_valid = df_full["DUE DATE"].max()
            
            default_start_due = min_date
            default_end_due = max_date
            
            if pd.notna(due_date_min_valid):
                default_start_due = due_date_min_valid.date()
            if pd.notna(due_date_max_valid):
                default_end_due = due_date_max_valid.date()
                
            if default_start_due > default_end_due:
                 default_start_due = default_end_due - timedelta(days=30)

            due_date_range = st.date_input(
                "Due Date Range",
                value=(default_start_due, default_end_due),
                min_value=min_date,
                max_value=max_date,
                key='due_date_slicer'
            )
            
            # Apply Filtering
            if len(due_date_range) == 2:
                start_due_date = pd.to_datetime(due_date_range[0])
                end_due_date = pd.to_datetime(due_date_range[1]) + timedelta(days=1)
                df_filtered = df_filtered[
                    (df_filtered["DUE DATE"].notna()) & 
                    (df_filtered["DUE DATE"] >= start_due_date) & 
                    (df_filtered["DUE DATE"] < end_due_date)
                ]

df = df_filtered.copy()


# ---------------------------------------------------------
# MATRIX TABLE (Person Summary and Drilldown)
# ---------------------------------------------------------

st.write("## 📊 Person Wise Summary") 

PERSON_COL = "CONT.PERSON"
REMARK_COL = "LATE DELIVERY REASON"

# Check if all required columns exist after data loading and calculation attempt
if not all(col in df.columns for col in REQUIRED_MATRIX_COLS):
    st.error("Required calculation columns are missing. Cannot render matrix. Please check the data loading step and column names in your Google Sheet.")
    st.stop()
else:
    # --- CSS for Professional Table Styling (Kept as is for structure) ---
    st.markdown("""
        <style>
        /* General styling for the matrix container */
        .matrix-container {
            border: 1px solid #333333;
            border-radius: 8px;
            margin-bottom: 20px;
            overflow: hidden; 
        }

        /* Styling for the header row */
        .matrix-header {
            background-color: #383838; 
            padding: 8px 5px; 
            font-weight: 700;
            color: #f0f2f6; 
            border-bottom: 2px solid #555555;
            text-transform: uppercase;
            font-size: 0.75em; 
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            text-align: center; 
        }
        .matrix-header.person-header {
             text-align: left; 
        }

        /* Styling for the data rows (Person Summary) */
        .person-summary-row {
            padding: 2px 5px; 
            border-bottom: 1px solid #222222; 
            align-items: center;
            display: flex; 
            font-size: 0.8em; 
            min-height: 25px; 
        }
        
        /* INCREASE FONT SIZE FOR VALUE COLUMNS */
        .summary-value-cell {
            font-size: 1.1em !important; 
            display: flex; 
            align-items: center;
            justify-content: flex-end; 
            min-height: 35px; 
            padding-top: 5px; 
            padding-bottom: 5px;
        }

        /* Grand Total Row Styling */
        .grand-total-row {
            background-color: #1e1e1e; 
            font-weight: 800;
            color: #ffffff; 
            border-top: 2px solid #555555;
            font-size: 0.85em;
        }

        /* Color Coding for Metric Values */
        .on-time-del { color: #5cb85c; font-weight: 600; }
        .late-del { color: #d9534f; font-weight: 600; }
        .ord-wt { color: #6a9ce7; }
        .pending-ord { color: #f0ad4e; font-weight: 600; }
        .late-del-percent { color: #fa5788; font-weight: 600; }

        /* Icon Styling */
        [data-testid="stExpander"] button p { display: none !important; }
        [data-testid="stExpander"] button:before { content: '➕'; font-size: 1.5em; color: #6a9ce7; transition: transform 0.3s; margin: 0; padding: 0; line-height: 1; }
        [data-testid="stExpander"] button[aria-expanded="true"]:before { content: '➖'; color: #d9534f; }
        .stColumns { margin-top: 0px !important; margin-bottom: 0px !important; padding-top: 0px !important; padding-bottom: 0px !important; position: relative; }
        .person-name-cell { padding-left: 5px !important; }
        
        /* Toggle icon styling for visibility */
        /* Hides the default Streamlit toggle label and checkbox */
        [data-testid^="stForm"] + div > div > div:nth-child(1) [data-testid="stForm"] + div label { visibility: hidden; height: 0; margin: 0; padding: 0; }
        [data-testid^="stForm"] + div > div > div:nth-child(1) [data-testid="stForm"] + div input[type="checkbox"] { display: none; }
        
        /* Custom plus icon for toggle */
        [data-testid^="stForm"] + div > div > div:nth-child(1) [data-testid="stForm"] + div label:before {
            visibility: visible; 
            content: '➕'; 
            font-size: 1.5em; 
            color: #6a9ce7; 
            position: absolute; 
            top: 50%; 
            left: 50%; 
            transform: translate(-50%, -50%); 
            margin: 0; 
            cursor: pointer;
        }
        /* Custom minus icon for toggle when checked */
        [data-testid^="stForm"] + div > div > div:nth-child(1) [data-testid="stForm"] + div input[type="checkbox"]:checked + label:before {
            content: '➖'; 
            color: #d9534f;
        }
        </style>
    """, unsafe_allow_html=True)

    # Get unique persons from filtered data
    persons = sorted(df[PERSON_COL].dropna().unique())

    st.markdown('<div class="matrix-container">', unsafe_allow_html=True)
    
    # --- Matrix Header Row ---
    cols_header = st.columns([0.3, 2.2, 1, 1, 1, 1, 1])
    
    cols_header[0].markdown('<div class="matrix-header" style="text-align: center;"></div>', unsafe_allow_html=True)
    cols_header[1].markdown('<div class="matrix-header person-header">CONT.PERSON</div>', unsafe_allow_html=True)
    cols_header[2].markdown('<div class="matrix-header">ORD WT</div>', unsafe_allow_html=True)
    cols_header[3].markdown('<div class="matrix-header">ON_TIME DEL</div>', unsafe_allow_html=True)
    cols_header[4].markdown('<div class="matrix-header">LATE_DEL</div>', unsafe_allow_html=True)
    cols_header[5].markdown('<div class="matrix-header">PENDING ORD</div>', unsafe_allow_html=True)
    cols_header[6].markdown('<div class="matrix-header">LATE_DEL %</div>', unsafe_allow_html=True)
    
    # --- Data Rows (Person Summary) ---
    for p in persons:
        df_p = df[df[PERSON_COL] == p].dropna(subset=[PERSON_COL])

        if df_p.empty:
            continue
            
        ord_wt_sum = df_p["ORD WT"].sum()
        ontime_del_sum = df_p["ON_TIME DEL"].sum()
        late_del_sum = df_p["LATE_DEL"].sum()
        
        pending_ord_sum = max(0, ord_wt_sum - ontime_del_sum - late_del_sum) 
        late_del_percent_agg = (late_del_sum / ord_wt_sum) * 100 if ord_wt_sum > 0 else 0.0

        expander_key = f'expander_{p}'

        # 1. Display the static summary row (always visible)
        cols_summary = st.columns([0.3, 2.2, 1, 1, 1, 1, 1])
        
        # Col 0: Expander Icon (➕)
        with cols_summary[0]:
            is_expanded = st.toggle(
                label=" ", 
                value=st.session_state.get(expander_key, False), 
                key=f'toggle_{expander_key}'
            )
            st.session_state[expander_key] = is_expanded
        
        # Col 1: Person Name 
        cols_summary[1].markdown(f'<div class="person-summary-row person-name-cell">👤 {p}</div>', unsafe_allow_html=True)
        
        # Format numbers
        ord_wt_str = f"{ord_wt_sum:,.2f}"
        ontime_del_str = f"{ontime_del_sum:,.2f}"
        late_del_str = f"{late_del_sum:,.2f}"
        pending_ord_str = f"{pending_ord_sum:,.2f}"
        late_del_percent_str = f"{late_del_percent_agg:,.2f}%"

        # Summary Values (Cols 2, 3, 4, 5, 6)
        cols_summary[2].markdown(f'<div class="person-summary-row summary-value-cell ord-wt">{ord_wt_str}</div>', unsafe_allow_html=True)
        cols_summary[3].markdown(f'<div class="person-summary-row summary-value-cell on-time-del">{ontime_del_str}</div>', unsafe_allow_html=True)
        cols_summary[4].markdown(f'<div class="person-summary-row summary-value-cell late-del">{late_del_str}</div>', unsafe_allow_html=True)
        cols_summary[5].markdown(f'<div class="person-summary-row summary-value-cell pending-ord">{pending_ord_str}</div>', unsafe_allow_html=True)
        cols_summary[6].markdown(f'<div class="person-summary-row summary-value-cell late-del-percent">{late_del_percent_str}</div>', unsafe_allow_html=True)


        # --- Drilldown Detail (Remarks Section) ---
        if st.session_state.get(expander_key, False):
            with st.container(border=True): 
                st.markdown(f"**Details for {p}**", unsafe_allow_html=True)
                
                if REMARK_COL in df_p.columns:
                    # Filter remarks to drop blanks/NaN
                    remarks = sorted(df_p[REMARK_COL].astype(str).str.strip().replace('', np.nan).dropna().unique())

                    if remarks:
                        st.markdown("##### 📝 Late Delivery Details by Reason")

                        for r in remarks:
                            with st.expander(f"➡️ **Reason:** {r}"):
                                df_r = df_p[df_p[REMARK_COL] == r]

                                try:
                                    order_detail_cols = ["ORD NO", "ORD WT", "ON_TIME DEL", "LATE_DEL", "PENDING ORD"]
                                    existing_cols = [col for col in order_detail_cols if col in df_r.columns]
                                    
                                    if existing_cols:
                                        orders = df_r[existing_cols].copy()
                                        st.markdown(f"###### 📦 Orders affected by '{r}' ({len(orders)} orders)")
                                        st.dataframe(orders, use_container_width=True, hide_index=True, height=200) 
                                    else:
                                        st.info("Required order detail columns (ORD NO, ORD WT, etc.) are missing.")
                                except KeyError as e:
                                    st.error(f"Missing column in detail view: {e}")
                    else:
                        st.info("No specific late delivery remarks recorded for this person in the filtered data.")
                else:
                    st.error(f"Column '{REMARK_COL}' not found! Cannot display remark details.")

    # --- Grand Total Row ---
    total_ord_wt = df["ORD WT"].sum()
    total_ontime_del = df["ON_TIME DEL"].sum()
    total_late_del = df["LATE_DEL"].sum()
    total_pending_ord = df["PENDING ORD"].sum()
    
    total_late_del_percent = (total_late_del / total_ord_wt) * 100 if total_ord_wt > 0 else 0.0
    
    # Format totals
    total_ord_wt_str = f"{total_ord_wt:,.2f}"
    total_ontime_del_str = f"{total_ontime_del:,.2f}"
    total_late_del_str = f"{total_late_del:,.2f}"
    total_pending_ord_str = f"{total_pending_ord:,.2f}"
    total_late_del_percent_str = f"{total_late_del_percent:,.2f}%"

    cols_total = st.columns([0.3, 2.2, 1, 1, 1, 1, 1])
    
    cols_total[0].markdown('<div class="person-summary-row grand-total-row person-name-cell"></div>', unsafe_allow_html=True)
    cols_total[1].markdown('<div class="person-summary-row grand-total-row person-name-cell">GRAND TOTAL</div>', unsafe_allow_html=True)
    cols_total[2].markdown(f'<div class="person-summary-row grand-total-row summary-value-cell ord-wt">{total_ord_wt_str}</div>', unsafe_allow_html=True)
    cols_total[3].markdown(f'<div class="person-summary-row grand-total-row summary-value-cell on-time-del">{total_ontime_del_str}</div>', unsafe_allow_html=True)
    cols_total[4].markdown(f'<div class="person-summary-row grand-total-row summary-value-cell late-del">{total_late_del_str}</div>', unsafe_allow_html=True)
    cols_total[5].markdown(f'<div class="person-summary-row grand-total-row summary-value-cell pending-ord">{total_pending_ord_str}</div>', unsafe_allow_html=True)
    cols_total[6].markdown(f'<div class="person-summary-row grand-total-row summary-value-cell late-del-percent">{total_late_del_percent_str}</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
    
# ---------------------------------------------------------
# CHARTS
# ---------------------------------------------------------
if not df.empty and all(col in df.columns for col in REQUIRED_MATRIX_COLS):
    st.write("### 📘 Raw Data Preview (Filtered)")
    st.dataframe(df, use_container_width=True)
    
    st.write("---")
    
    ## 1. Remark Count Bar Chart (Excluding Blanks)
    st.write("## 📊 No of problems Count")

    REMARK_COL = "LATE DELIVERY REASON"
    if REMARK_COL in df.columns:
        # Filter out blanks/empty strings
        remark_counts = df[REMARK_COL].astype(str).str.strip().replace('', np.nan).dropna().value_counts().reset_index()
        remark_counts.columns = [REMARK_COL, "COUNT"]

        if not remark_counts.empty:
            fig_bar = px.bar(
                remark_counts,
                x=REMARK_COL,
                y="COUNT",
                title="Late Delivery Remarks Count (Excluding Blanks)",
                color="COUNT",
                color_continuous_scale=px.colors.sequential.Sunset,
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No late delivery remarks found in the current date filter selection.")
    else:
        st.info(f"Cannot generate Remark Count Chart: Column '{REMARK_COL}' is missing.")

    st.write("---")
    
    ## 2 & 3. Pie Charts (Item Name and Purity)
    ITEM_COL = "ITEM NAME"
    PURITY_COL = "PURITY"
    
    st.write("## 🥧 Item Name & Purity Distribution by Order Weight")
    
    col_pie1, col_pie2 = st.columns(2)

    # Pie Chart 1: Item Name vs Total Order Weight
    with col_pie1:
        if ITEM_COL in df.columns:
            # Group data
            item_wt = df.groupby(ITEM_COL)["ORD WT"].sum().reset_index()

            # Create pie chart
            fig_item_pie = px.pie(
                item_wt,
                names=ITEM_COL,
                values="ORD WT",
                title="Item Name by Total Order Weight",
                hole=.3,
            )

            # Enforce consistent size and centering
            fig_item_pie.update_layout(
                height=700, # <-- INCREASED HEIGHT TO PREVENT CUTOFF
                title_x=0.5,
                uniformtext_minsize=12,
                uniformtext_mode='hide',
                # Set horizontal legend below the chart
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.2, 
                    xanchor="center",
                    x=0.5
                )
            )

            # Show chart
            st.plotly_chart(fig_item_pie, use_container_width=True)

        else:
            st.info(f"Cannot generate Item Weight Pie Chart: Column '{ITEM_COL}' is missing.")

    # Pie Chart 2: Purity vs Total Order Weight
    with col_pie2:
        if PURITY_COL in df.columns:
            purity_wt = df.groupby(PURITY_COL)["ORD WT"].sum().reset_index()

            fig_purity_pie = px.pie(
                purity_wt,
                names=PURITY_COL,
                values="ORD WT",
                title="Purity Distribution by Total Order Weight",
                hole=.3,
            )
            # Enforce consistent size and centering
            fig_purity_pie.update_layout(
                height=700, # <-- INCREASED HEIGHT TO PREVENT CUTOFF
                title_x=0.5, 
                uniformtext_minsize=12, 
                uniformtext_mode='hide',
                # Set horizontal legend below the chart
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.2, 
                    xanchor="center",
                    x=0.5
                )
            )
            st.plotly_chart(fig_purity_pie, use_container_width=True)
        else:
            st.info(f"Cannot generate Purity Pie Chart: Column '{PURITY_COL}' is missing.")


    st.write("---")
    
    ## 4. Item Wise Delivery Time (Min, Max, Avg)
    st.write("## ⏱️ Item Wise Production time Analysis")
    LEAD_TIME_COL = 'LEAD TIME (DAYS)'

    if ITEM_COL in df.columns and LEAD_TIME_COL in df.columns:
        # Group by ITEM NAME and calculate min, max, average lead time
        delivery_time_summary = df.groupby(ITEM_COL)[LEAD_TIME_COL].agg(
            min_lead='min',
            max_lead='max',
            avg_lead='mean'
        ).reset_index()
        
        # Sort data by highest average lead time (Descending)
        delivery_time_summary = delivery_time_summary.sort_values(by='avg_lead', ascending=False)
        
        # Melt the dataframe for plotting with Plotly Express
        df_melted = delivery_time_summary.melt(
            id_vars=ITEM_COL,
            value_vars=['min_lead', 'max_lead', 'avg_lead'],
            var_name='Metric',
            value_name='Delivery Time (Days)'
        )
        
        df_melted['Metric'] = df_melted['Metric'].replace({
            'min_lead': 'Min Lead Time',
            'max_lead': 'Max Lead Time',
            'avg_lead': 'Avg Lead Time'
        })
        
        # Create the bar chart
        fig_lead_time = px.bar(
            df_melted,
            x=ITEM_COL,
            y='Delivery Time (Days)',
            color='Metric',
            barmode='group',
            title='Item Wise Min, Max, and Average Lead Time (DUE DATE - ODR DATE)',
            color_discrete_map={
                'Avg Lead Time': '#6a9ce7',
                'Max Lead Time': '#d9534f',
                'Min Lead Time': '#5cb85c',
            },
            category_orders={ITEM_COL: delivery_time_summary[ITEM_COL].tolist()} 
        )
        
        st.plotly_chart(fig_lead_time, use_container_width=True)
        
        st.markdown("##### Matrix Table (Sorted by Average Lead Time)")
        # Display the matrix table as requested, formatted to 2 decimal places and excluding the index
        st.dataframe(
            delivery_time_summary.style.format(
                {'min_lead': "{:.2f}", 'max_lead': "{:.2f}", 'avg_lead': "{:.2f}"}
            ), 
            use_container_width=True, 
            hide_index=True
        )
        
    else:
        st.info(f"Cannot generate Lead Time Analysis Chart. Check if '{ITEM_COL}', 'ODR DATE', and 'DUE DATE' columns exist and contain valid data.")
        

elif not df.empty:
    st.warning("Cannot display charts or raw data due to missing required numeric columns.")
else:
    st.warning("The dataset is empty after applying filters.")
