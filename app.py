import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置與標題 ==========
st.set_page_config(
    page_title="StockRevenueLab | 飆股基因對帳單",
    page_icon="🧪",
    layout="wide"
)

# 自定義 CSS 讓界面更具專業感
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { border-left: 5px solid #ff4b4b; background-color: white; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

st.title("🧪 StockRevenueLab: 2024 飆股基因對帳單")

# --- 白話解釋區 ---
st.markdown("""
### 2024 年的翻倍股，真的是靠營收撐起來的嗎？

**💡 為什麼數據要這樣對齊？**
一般人研究 2024 年會看 1月到12月的營收，但**那是錯的**。
因為 2024 年 1 月 2 日開盤時，你手上最新能參考的報表是 **2023 年 12 月**發布的。

為了還原真相，本研究精確對齊了影響 2024 年股價的 **12 份黃金報表**：
* **起點：** 2023/12 營收（這份報表驅動了 2024 年初的股價）
* **終點：** 2024/11 營收（這份報表驅動了 2024 年底的股價）
---
""")

# ========== 2. 安全資料庫連線 (使用 Secrets) ==========
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
        st.error("❌ 無法連線至資料庫，請確認 Streamlit Secrets 設定。")
        st.stop()

# ========== 3. 數據抓取引擎 ==========
@st.cache_data(ttl=3600)
def fetch_main_data(year, calc_method):
    engine = get_engine()
    
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

# ========== 4. UI 介面設計 ==========
st.sidebar.header("🔬 研究條件篩選")
target_year = st.sidebar.selectbox("分析年度", ["2024", "2025"], index=0)
calc_method = st.sidebar.radio("統計指標", ["中位數 (推薦)", "平均值"])

st.sidebar.markdown("---")
st.sidebar.caption(f"數據對應月份：{int(target_year)-1912}_12 至 {int(target_year)-1911}_11")

# ========== 5. 熱力圖呈現 ==========
df = fetch_main_data(target_year, calc_method)

if not df.empty:
    total_samples = df.groupby('return_bin')['group_sample_count'].max().sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("研究樣本總數", f"{int(total_samples)} 檔")
    with col2:
        st.metric("分析時間範圍", f"{target_year} 全年度")
    with col3:
        st.metric("數據來源", "全市場上市櫃/興櫃公司")

    st.subheader(f"📊 {target_year} 「漲幅區間 vs 營收成長」熱力圖")
    
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    
    # 修正語法錯誤：確保引號閉合且參數正確
    fig = px.imshow(
        pivot_df,
        labels=dict(x="報表月份", y="年度漲幅區間", color="YoY %"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="RdYlGn",
        aspect="auto",
        text_auto=".1f"
    )
    fig.update_layout(xaxis_nticks=12)
    st.plotly_chart(fig, use_container_width=True)

    # ========== 6. 區間領頭羊 ==========
    st.write("---")
    st.subheader("🔍 點名時間：看看這些區間的「業績領頭羊」是誰？")
    
    selected_bin = st.selectbox("選擇一個漲幅區間查看前 10 名營收王：", pivot_df.index[::-1])
    
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
        m.stock_id as "公司代號",
        m.stock_name as "公司名稱",
        ROUND(AVG(m.yoy_pct)::numeric, 2) as "平均營收年增率 %"
    FROM monthly_revenue m
    JOIN target_stocks t ON m.stock_id = SPLIT_PART(t.symbol, '.', 1)
    WHERE m.report_month = '{prev_minguo_year}_12' 
       OR (m.report_month LIKE '{minguo_year}_%' AND m.report_month <= '{minguo_year}_11')
    GROUP BY m.stock_id, m.stock_name
    ORDER BY "平均營收年增率 %" DESC
    LIMIT 10;
    """
    
    with get_engine().connect() as conn:
        top_df = pd.read_sql_query(text(detail_query), conn)
    
    if not top_df.empty:
        st.table(top_df)
    else:
        st.info("該區間暫無對應數據。")

    with st.expander("👉 查看原始數據矩陣"):
        st.dataframe(pivot_df.style.format("{:.1f}%"), use_container_width=True)

else:
    st.warning("⚠️ 資料庫中尚未發現對應年度的分析資料。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 讓數據說真話")
