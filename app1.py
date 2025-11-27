import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import plotly.express as px

# ---------------------------------------------------------
# GOOGLE SHEET AUTH
# ---------------------------------------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)
client = gspread.authorize(creds)

# ---------------------------------------------------------
# LOAD SHEET
# ---------------------------------------------------------
sheet = client.open("PRODUCTION_ORDER_STATUS_REPORT").worksheet("ORDER_SHEET")
raw = sheet.get_all_values()
df = pd.DataFrame(raw)

# ----- CLEAN HEADER -----
df = df.dropna(axis=1, how="all")
df.columns = df.iloc[1]        # 2nd row is header
df = df[2:].reset_index(drop=True)

# Deduplicate column names
df = df.loc[:, ~df.columns.duplicated()]

# Convert numeric columns
for col in ["ORD WT", "ON_TIME DEL", "LATE_DEL"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ---------------------------------------------------------
# UI TITLE
# ---------------------------------------------------------
st.title("🔥 Production Delivery Dashboard (Google Sheet Linked)")

st.write("### 📘 Raw Data Preview")
st.dataframe(df)

# ---------------------------------------------------------
# MATRIX TABLE (Expandable)
# ---------------------------------------------------------

st.write("## 📊 Person → Remark → Orders Matrix")

if "CONT.PERSON" not in df.columns:
    st.error("CONT.PERSON column not found!")
else:
    persons = sorted(df["CONT.PERSON"].dropna().unique())

    for p in persons:
        with st.expander(f"👨‍🔧 {p}"):
            df_p = df[df["CONT.PERSON"] == p]

            remarks = sorted(df_p["LATE DELIVERY REASON"].dropna().unique())

            for r in remarks:
                with st.expander(f"📝 Remark: {r}"):
                    df_r = df_p[df_p["LATE DELIVERY REASON"] == r]

                    # Show order numbers
                    orders = df_r[["ORD NO", "ORD WT", "ON_TIME DEL", "LATE_DEL"]]

                    st.write("#### 📦 Orders")
                    st.dataframe(orders)

            # Totals for that person
            total_row = {
                "ORD WT": df_p["ORD WT"].sum(),
                "ON_TIME DEL": df_p["ON_TIME DEL"].sum(),
                "LATE_DEL": df_p["LATE_DEL"].sum(),
            }

            st.write("### ✅ Total Summary for", p)
            st.json(total_row)

# ---------------------------------------------------------
# CHART 1 — REMARK COUNT BAR CHART
# ---------------------------------------------------------
st.write("## 📊 Remark Frequency Count")

if "LATE DELIVERY REASON" in df.columns:
    remark_counts = df["LATE DELIVERY REASON"].value_counts().reset_index()
    remark_counts.columns = ["LATE DELIVERY REASON", "COUNT"]

    fig_bar = px.bar(
        remark_counts,
        x="LATE DELIVERY REASON",
        y="COUNT",
        title="Remarks Count",
    )
    st.plotly_chart(fig_bar)

# ---------------------------------------------------------
# CHART 2 — ITEM NAME WISE TOTAL ORDER WT
# ---------------------------------------------------------
st.write("## 🏷️ Item Name Wise Total Order Weight")

if "ITEM NAME" in df.columns:
    item_wt = df.groupby("ITEM NAME")["ORD WT"].sum().reset_index()

    fig_item = px.bar(
        item_wt,
        x="ITEM NAME",
        y="ORD WT",
        title="Item Name vs Total Order Weight",
    )
    st.plotly_chart(fig_item)
