import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

st.set_page_config(page_title="機率研究室 | StockRevenueLab", layout="wide")

# ========== 1. 資料庫連線 ==========
@st.cache_resource
def get_engine():
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    PROJECT_REF = st.secrets["PROJECT_REF"]
    POOLER_HOST = st.secrets["POOLER_HOST"]
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
    return create_engine(connection_string)

# ========== 2. 核心計算邏輯：勝率與期望值 ==========
@st.cache_data(ttl=3600)
def fetch_probability_data(year, threshold_low, threshold_high):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH revenue_stats AS (
        SELECT 
            stock_id,
            COUNT(*) FILTER (WHERE yoy_pct >= {threshold_low} AND yoy_pct < {threshold_high}) as hit_count
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
        GROUP BY stock_id
        HAVING COUNT(*) >= 11
    ),
    performance AS (
        SELECT 
            SPLIT_PART(symbol, '.', 1) as stock_id,
            ((year_close - year_open) / year_open) * 100 as annual_return
        FROM stock_annual_k WHERE year = '{year}'
    )
    SELECT 
        r.hit_count as "營收達標次數",
        COUNT(*) as "股票檔數",
        ROUND(AVG(p.annual_return)::numeric, 2) as "平均年漲幅%",
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY p.annual_return)::numeric, 2) as "漲幅中位數%",
        ROUND((COUNT(*) FILTER (WHERE p.annual_return > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(漲幅>20%)",
        ROUND((COUNT(*) FILTER (WHERE p.annual_return > 100) * 100.0 / COUNT(*))::numeric, 1) as "大賺率(漲幅>100%)"
    FROM revenue_stats r
    JOIN performance p ON r.stock_id = p.stock_id
    GROUP BY r.hit_count
    ORDER BY r.hit_count DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 3. 介面設計 ==========
st.title("🎲 營收爆發與股價勝率研究")
st.markdown(f"""
本頁面旨在回答一個核心問題：**「當營收爆發頻率達到多少次時，股價上漲變成了一種『高機率』事件？」**
我們定義「爆發」為月營收 YoY 落在特定區間。
""")

col1, col2 = st.columns(2)
with col1:
    target_year = st.selectbox("研究年度", ["2024", "2025"])
with col2:
    growth_range = st.select_slider(
        "設定營收年增率 (YoY) 爆發區間",
        options=[0, 20, 50, 100, 500, 1000],
        value=(100, 1000)
    )

low, high = growth_range
st.info(f"🔍 正在分析：一年 12 個月中，出現營收年增率在 **{low}% ~ {high}%** 之間次數與股價的關係")

df_prob = fetch_probability_data(target_year, low, high)

if not df_prob.empty:
    st.subheader(f"📊 {target_year} 年：營收達標次數 vs 期望值對照表")
    st.table(df_prob)
    
    # 視覺化期望值
    st.line_chart(df_prob.set_index("營收達標次數")[["平均年漲幅%", "漲幅中位數%"]])
else:
    st.warning("此區間樣本數不足，請嘗試調整篩選條件。")

st.markdown("""
---
**💡 老師筆記：**
1. **機率感**：看最後兩欄。如果「出現 10 次」的勝率高達 80%，那這就是你的選股 SOP。
2. **中位數 vs 平均數**：如果平均數很高但中位數很低，代表該組別是靠一兩隻「妖股」撐場，不具備普遍機率。
""")
