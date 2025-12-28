import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

st.set_page_config(page_title="機率研究室 | StockRevenueLab", layout="wide")

# ========== 資料庫連線 ==========
@st.cache_resource
def get_engine():
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    PROJECT_REF = st.secrets["PROJECT_REF"]
    POOLER_HOST = st.secrets["POOLER_HOST"]
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
    return create_engine(connection_string)

@st.cache_data(ttl=3600)
def fetch_prob_data(year, metric_col, low, high):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH revenue_stats AS (
        SELECT stock_id, COUNT(*) FILTER (WHERE {metric_col} >= {low} AND {metric_col} < {high}) as hit_count
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12' OR m.report_month LIKE '{minguo_year}_%'
        GROUP BY stock_id HAVING COUNT(*) >= 10
    ),
    performance AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, ((year_close - year_open) / year_open) * 100 as annual_return
        FROM stock_annual_k WHERE year = '{year}'
    )
    SELECT r.hit_count as "爆發次數", COUNT(*) as "樣本數",
        ROUND(AVG(p.annual_return)::numeric, 1) as "平均漲幅%",
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY p.annual_return)::numeric, 1) as "漲幅中位數%",
        ROUND((COUNT(*) FILTER (WHERE p.annual_return > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(>20%)",
        ROUND((COUNT(*) FILTER (WHERE p.annual_return > 100) * 100.0 / COUNT(*))::numeric, 1) as "翻倍率(>100%)"
    FROM revenue_stats r
    JOIN performance p ON r.stock_id = p.stock_id
    GROUP BY r.hit_count ORDER BY r.hit_count DESC;
    """
    # 修正：因 SQL 內部邏輯較複雜，若報錯改用較簡單的 minguo 判斷
    simple_query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits FROM monthly_revenue 
        WHERE (report_month LIKE '{minguo_year}_%' OR report_month = '{prev_minguo_year}_12')
        AND {metric_col} >= {low} AND {metric_col} < {high}
        GROUP BY stock_id
    ),
    perf_table AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, ((year_close - year_open) / year_open)*100 as ret
        FROM stock_annual_k WHERE year = '{year}'
    )
    SELECT h.hits as "爆發次數", COUNT(*) as "股票檔數",
           ROUND(AVG(p.ret)::numeric, 1) as "平均漲幅%",
           ROUND((COUNT(*) FILTER (WHERE p.ret > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(>20%)",
           ROUND((COUNT(*) FILTER (WHERE p.ret > 100) * 100.0 / COUNT(*))::numeric, 1) as "翻倍率(>100%)"
    FROM hit_table h JOIN perf_table p ON h.stock_id = p.stock_id
    GROUP BY h.hits ORDER BY h.hits DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(simple_query), conn)

st.title("🎲 營收爆發與股價期望值")

with st.sidebar:
    target_year = st.selectbox("研究年度", [str(y) for y in range(2025, 2019, -1)])
    study_metric = st.selectbox("研究指標", ["yoy_pct", "mom_pct"])
    growth_range = st.select_slider("設定爆發區間 (%)", options=[-50, 0, 20, 50, 100, 500, 1000], value=(50, 500))

df_prob = fetch_prob_data(target_year, study_metric, growth_range[0], growth_range[1])

if not df_prob.empty:
    st.subheader(f"📊 {target_year} 年：營收達標次數 vs 勝率對照")
    st.table(df_prob)
    st.bar_chart(df_prob.set_index("爆發次數")[["勝率(>20%)", "翻倍率(>100%)"]])
else:
    st.info("此年度或條件下暫無足夠樣本。")
