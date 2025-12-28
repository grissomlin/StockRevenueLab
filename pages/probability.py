import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

st.set_page_config(page_title="機率研究室 | StockRevenueLab", layout="wide")

# ========== 資料庫連線 ==========
@st.cache_resource
def get_engine():
    try:
        DB_PASSWORD = st.secrets["DB_PASSWORD"]
        PROJECT_REF = st.secrets["PROJECT_REF"]
        POOLER_HOST = st.secrets["POOLER_HOST"]
        encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
        connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
        return create_engine(connection_string)
    except Exception as e:
        st.error("❌ 資料庫連線失敗，請檢查 Secrets。")
        st.stop()

@st.cache_data(ttl=3600)
def fetch_probability_data(year, threshold_low, threshold_high):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH revenue_stats AS (
        SELECT stock_id, COUNT(*) FILTER (WHERE yoy_pct >= {threshold_low} AND yoy_pct < {threshold_high}) as hit_count
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND (LENGTH(report_month) = {len(str(minguo_year))}+3))
        GROUP BY stock_id
        HAVING COUNT(*) >= 10
    ),
    performance AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, ((year_close - year_open) / year_open) * 100 as annual_return
        FROM stock_annual_k WHERE year = '{year}'
    )
    SELECT r.hit_count as "營收達標次數", COUNT(*) as "股票檔數",
        ROUND(AVG(p.annual_return)::numeric, 2) as "平均年漲幅%",
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY p.annual_return)::numeric, 2) as "漲幅中位數%",
        ROUND((COUNT(*) FILTER (WHERE p.annual_return > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(漲幅>20%)",
        ROUND((COUNT(*) FILTER (WHERE p.annual_return > 100) * 100.0 / COUNT(*))::numeric, 1) as "大賺率(漲幅>100%)"
    FROM revenue_stats r
    JOIN performance p ON r.stock_id = p.stock_id
    GROUP BY r.hit_count ORDER BY r.hit_count DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

st.title("🎲 跨年度營收勝率研究室")

target_year = st.selectbox("研究年度", [str(y) for y in range(2025, 2019, -1)])
growth_range = st.select_slider("設定營收年增率 (YoY) 爆發區間", options=[0, 20, 50, 100, 500, 1000], value=(100, 1000))

df_prob = fetch_probability_data(target_year, growth_range[0], growth_range[1])

if not df_prob.empty:
    st.subheader(f"📊 {target_year} 年：營收達標次數 vs 期望值對照表")
    st.table(df_prob)
    
    # 修正處：將「營營收達標次數」改為「營收達標次數」
    st.subheader("🎯 實戰勝率視覺化")
    chart_data = df_prob.set_index("營收達標次數")[["勝率(漲幅>20%)", "大賺率(漲幅>100%)"]]
    st.bar_chart(chart_data)
else:
    st.info(f"查無 {target_year} 年數據。請確認資料庫已匯入該年度股價與營收表。")
