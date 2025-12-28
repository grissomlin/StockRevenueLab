import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="StockRevenueLab | 趨勢觀測站", page_icon="🧪", layout="wide")

st.sidebar.success("💡 想要看『勝率與機率分析』？請點選左側選單的 probability 頁面！")

st.title("🧪 StockRevenueLab: 全時段飆股基因對帳單")

# ========== 2. 安全資料庫連線 ==========
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
        st.error("❌ 資料庫連線失敗")
        st.stop()

# ========== 3. 數據抓取引擎 (支援動態年份) ==========
@st.cache_data(ttl=3600)
def fetch_main_data(year, calc_method):
    engine = get_engine()
    agg_func = "percentile_cont(0.5) WITHIN GROUP (ORDER BY m.yoy_pct)" if calc_method == "中位數 (推薦)" else "AVG(m.yoy_pct)"
    
    # 自動計算民國年
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH annual_bins AS (
        SELECT symbol, ((year_close - year_open) / year_open) * 100 AS annual_return,
            CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END AS return_bin
        FROM stock_annual_k WHERE year = '{year}'
    ),
    monthly_yoy AS (
        SELECT stock_id, report_month, yoy_pct FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND (LENGTH(report_month) = {len(str(minguo_year))}+3))
    )
    SELECT b.return_bin, m.report_month, {agg_func} as val, COUNT(DISTINCT b.symbol) as group_sample_count
    FROM annual_bins b
    JOIN monthly_yoy m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    GROUP BY b.return_bin, m.report_month
    ORDER BY b.return_bin, m.report_month;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. 介面篩選 (解鎖 2020-2025) ==========
st.sidebar.header("🔬 研究條件篩選")
# 這裡直接把範圍拉大
target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)])
calc_method = st.sidebar.radio("熱力圖指標", ["中位數 (推薦)", "平均值"])

df = fetch_main_data(target_year, calc_method)

if not df.empty:
    st.subheader(f"📊 {target_year} 「漲幅區間 vs 營收成長」熱力圖")
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    fig = px.imshow(pivot_df, color_continuous_scale="RdYlGn", aspect="auto", text_auto=".1f")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning(f"⚠️ 資料庫中尚無 {target_year} 年的完整比對資料，請檢查數據匯入狀況。")
