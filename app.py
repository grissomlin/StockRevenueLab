import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置 ==========
st.set_page_config(
    page_title="StockRevenueLab | 飆股基因對帳單",
    page_icon="🧪",
    layout="wide"
)

# 自定義 CSS
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { border-left: 5px solid #ff4b4b; background-color: white; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

st.title("🧪 StockRevenueLab: 2024 飆股基因對帳單")

st.markdown("""
### 2024 年的翻倍股，真的是靠營收撐起來的嗎？

**💡 為什麼數據要這樣對齊？**
本研究精確對齊了影響 2024 年股價的 **12 份黃金報表**：
* **起點：** 2023/12 營收 (民國 112_12) —— 這份報表在 2024/01/10 前公布，直接驅動年初股價。
* **終點：** 2024/11 營收 (民國 113_11) —— 這份報表在 2024/12/10 前公布，驅動了年底股價。
---
""")

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
        st.error("❌ 資料庫連線失敗，請檢查 Secrets 設定。")
        st.stop()

# ========== 3. 數據抓取引擎 ==========
@st.cache_data(ttl=3600)
def fetch_main_data(year, calc_method):
    engine = get_engine()
    
    # 決定聚合函數
    if calc_method == "中位數 (推薦)":
        agg_func = "percentile_cont(0.5) WITHIN GROUP (ORDER BY m.yoy_pct)"
    else:
        agg_func = "AVG(m.yoy_pct)"
    
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH annual_bins AS (
        SELECT 
            symbol,
            ((year_close - year_open) / year_open) * 100 AS annual_return,
            CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END AS return_bin
        FROM stock_annual_k
        WHERE year = '{year}'
    ),
    monthly_yoy AS (
        -- 確保完整抓取 12 個月：前年 12 月 + 當年 01-11 月
        SELECT stock_id, report_month, yoy_pct 
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
    )
    SELECT 
        b.return_bin,
        m.report_month,
        {agg_func} as val,
        COUNT(DISTINCT b.symbol) as group_sample_count
    FROM annual_bins b
    JOIN monthly_yoy m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    GROUP BY b.return_bin, m.report_month
    ORDER BY b.return_bin, m.report_month;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. UI 側邊欄 ==========
st.sidebar.header("🔬 研究條件篩選")
target_year = st.sidebar.selectbox("分析年度", ["2024", "2025"], index=0)
calc_method = st.sidebar.radio("熱力圖指標", ["中位數 (推薦)", "平均值"])

# ========== 5. 儀表板主視圖 ==========
df = fetch_main_data(target_year, calc_method)

if not df.empty:
    actual_months = df['report_month'].nunique()
    total_samples = df.groupby('return_bin')['group_sample_count'].max().sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("研究樣本總數", f"{int(total_samples)} 檔")
    with col2:
        st.metric("觀測月份完整度", f"{actual_months} / 12 個月")
    with col3:
        st.metric("數據來源", "全市場上市櫃/興櫃")

    st.subheader(f"📊 {target_year} 「漲幅區間 vs 營收成長」熱力圖")
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="報表月份", y="漲幅區間", color="YoY %"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="RdYlGn",
        aspect="auto",
        text_auto=".1f"
    )
    st.plotly_chart(fig, use_container_width=True)

    # ========== 6. 區間領頭羊 (同時顯示平均與中位數) ==========
    st.write("---")
    st.subheader("🔍 區間業績點名：誰是該區間的成長王？")
    
    selected_bin = st.selectbox("選擇漲幅區間查看清單：", pivot_df.index[::-1])
    
    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1
    
    detail_query = f"""
    WITH target_stocks AS (
        SELECT symbol FROM stock_annual_k 
        WHERE year = '{target_year}' 
        AND (
            CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END
        ) = '{selected_bin}'
    )
    SELECT 
        m.stock_id as "代號",
        m.stock_name as "名稱",
        ROUND(AVG(m.yoy_pct)::numeric, 2) as "平均年增率 %",
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY m.yoy_pct)::numeric, 2) as "中位數年增率 %"
    FROM monthly_revenue m
    JOIN target_stocks t ON m.stock_id = SPLIT_PART(t.symbol, '.', 1)
    WHERE m.report_month = '{prev_minguo_year}_12' 
       OR (m.report_month LIKE '{minguo_year}_%' AND m.report_month <= '{minguo_year}_11')
    GROUP BY m.stock_id, m.stock_name
    ORDER BY "平均年增率 %" DESC
    LIMIT 10;
    """
    
    with get_engine().connect() as conn:
        top_df = pd.read_sql_query(text(detail_query), conn)
    
    if not top_df.empty:
        st.write(f"🏆 **{selected_bin}** 區間中，營收表現最亮眼的 10 檔公司：")
        st.table(top_df)
    else:
        st.info("該區間暫無數據。")

    with st.expander("👉 查看原始數據矩陣"):
        st.dataframe(pivot_df.style.format("{:.1f}%"), use_container_width=True)

else:
    st.warning("⚠️ 數據加載中或資料庫內無資料。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 讓數據說真話")
