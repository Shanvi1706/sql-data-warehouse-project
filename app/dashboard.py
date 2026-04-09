# streamlit dashboard fr sales data from sql server

import os
from datetime import timedelta
import logging

import streamlit as st
import pandas as pd
import pyodbc
import altair as alt

LOG = logging.getLogger(__name__)

DEFAULT_CONN_STR = os.environ.get(
    "DW_CONN",
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-TRPCHH4\\SQLEXPRESS;"
    "DATABASE=DataWarehouse;"
    "Trusted_Connection=yes;",
)


@st.cache_resource
def get_conn(conn_str: str = DEFAULT_CONN_STR):
    return pyodbc.connect(conn_str, autocommit=True)


@st.cache_data(ttl=300)
def query_sales(_conn, start_date: str, end_date: str) -> pd.DataFrame:
    q = """
    SELECT *
    FROM gold.fact_sales
    WHERE order_date >= ? AND order_date <= ?
    """
    cur = _conn.cursor()
    cur.execute(q, (start_date, end_date))
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame.from_records(rows, columns=cols)
    df.columns = [col.lower() for col in df.columns]
    # ensure datetime parsing if needed
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    return df


st.set_page_config(page_title="Sales Dashboard", layout="wide")
st.title("Sales Dashboard")

# connection info
with st.expander("Connection"):
    st.text_input("Connection string (env DW_CONN overrides)", value=DEFAULT_CONN_STR, key="conn_str_input")

conn = None
try:
    conn = get_conn()
except Exception as exc:
    st.error(f"Failed to connect to database: {exc}")
    st.stop()

# determine date range defaults
today = pd.Timestamp.now().normalize()
default_start = (today - pd.Timedelta(days=90)).date()
default_end = today.date()

col1, col2 = st.sidebar.columns([3, 1])
with st.sidebar:
    st.header("Filters")
    start_date = st.date_input(
        "Start date",
        value=pd.to_datetime("2013-01-01"),
        min_value=pd.to_datetime("2010-01-01"),
        max_value=pd.to_datetime("2030-01-01")
    )

end_date = st.date_input(
    "End date",
    value=pd.to_datetime("2014-01-01"),
    min_value=pd.to_datetime("2010-01-01"),
    max_value=pd.to_datetime("2030-01-01")
)

if start_date > end_date:
    st.warning("Start date is after end date — results may be empty.")
refresh = st.button("Refresh")

# fetch data
df = query_sales(conn, str(start_date), str(end_date))

if df.empty:
    st.info("No sales records found for the selected range.")
else:
    # basic KPIs
    total_sales = df["sales_amount"].sum() if "sales_amount" in df.columns else None
    total_orders = len(df)
    avg_order = (df["sales_amount"].mean() if "sales_amount" in df.columns else None)
    unique_customers = df["customer_key"].nunique() if "customer_key" in df.columns else None

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Sales", f"{total_sales:,.2f}" if total_sales is not None else "N/A")
    k2.metric("Orders", f"{total_orders:,}")
    k3.metric("Avg Order", f"{avg_order:,.2f}" if avg_order is not None else "N/A")
    k4.metric("Unique Customers", f"{unique_customers:,}" if unique_customers is not None else "N/A")

    st.markdown("### Sales Over Time")
    if "order_date" in df.columns and "amount" in df.columns:
        timeseries = (
            df.dropna(subset=["order_date"])
            .groupby(pd.Grouper(key="order_date", freq="D"))["amount"]
            .sum()
            .reset_index()
        )
        chart = alt.Chart(timeseries).mark_line(point=True).encode(
            x="order_date:T", y=alt.Y("sales_amount:Q", title="Sales Amount")
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("order_date or sales_amount column missing for timeseries.")

    st.markdown("### Top Products by Sales")
    if "product_key" in df.columns and "sales_amount" in df.columns:
        top_products = (
            df.groupby("product_key")["sales_amount"].sum().reset_index().sort_values("sales_amount", ascending=False).head(10)
        )
        bar = alt.Chart(top_products).mark_bar().encode(
            x=alt.X("sales_amount:Q", title="Sales Amount"), y=alt.Y("product_key:N", sort="-x", title="Product")
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("product_key or sales_amount column missing for top products.")

    st.markdown("### Sample Rows")
    st.dataframe(df.head(200))

st.sidebar.markdown("---")
st.sidebar.write("ETL datasets present in repo:")
st.sidebar.write("- source_crm/cust_info.csv")
st.sidebar.write("- source_crm/prd_info.csv")
st.sidebar.write("- source_crm/sales_details.csv")
st.sidebar.write("- source_erp/CUST_AZ12.csv")
st.sidebar.write("- source_erp/ITEM_AZ12.csv")
st.sidebar.write("- source_erp/SALES_AZ12.csv")