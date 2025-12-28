import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置與專業風格 ==========
st.set_page_config(
    page_title="StockRevenueLab | 全市場量化研究",
    page_icon="🧪",
    layout="wide"
)

# 自定義 CSS
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { border-left: 5px solid #007bff; background-color: white; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

st.title("🧪 StockRevenueLab: 台股飆股與營收關聯研究")
st.markdown("""
本研究室透過 **SQL 聚合技術**，對齊 **股價 (Price)** 與 **財報 (Revenue)** 的時序資訊。
重點在於解決台灣市場特有的代號格式（.TW/.TWO）與財報揭露滯後問題，還原最真實的市場規律。
---
""")

# ========== 2. 安全資料庫連線 ==========
@st.cache_resource
def get_engine():
    try:
        # 從 Streamlit Secrets 讀取連線資訊
        DB_PASSWORD = st.secrets["DB_PASSWORD"]
        PROJECT_REF = st.secrets["PROJECT_REF"]
        POOLER_HOST = st.secrets["POOLER_HOST"]
        
        encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
        connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
        return create_engine(connection_string)
    except Exception as e:
        st.error("❌ 偵測到連線設定錯誤。請確保 Streamlit 後台 Secrets 已設定。")
        st.stop()

# ========== 3. 數據核心引擎 (優化對齊邏輯) ==========
@st.cache_data(ttl=3600)
def fetch_analysis_data(year, calc_method):
    engine = get_engine()
    
    # 計算方式切換
    if calc_method == "中位數 (推薦)":
        agg_func = "percentile_cont(0.5) WITHIN GROUP (ORDER BY m.yoy_pct)"
    else:
        agg_func = "AVG(m.yoy_pct)"
    
    # 民國與西元轉換邏輯
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    # 修正點：使用 SPLIT_PART 處理 .TW 與 .TWO，找回上櫃公司樣本
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
        -- 對齊 2024 年股價受影響的 12 份報表
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

# ========== 4. 側邊欄控制與 UI ==========
st.sidebar.header("🔬 研究參數控制")
target_year = st.sidebar.selectbox("分析目標年度", ["2024", "2025"], index=0)
calc_method = st.sidebar.radio("統計指標", ["中位數 (推薦)", "平均值"])

st.sidebar.markdown("---")
st.sidebar.caption("數據最後同步時間: 2025-12-28")

# ========== 5. 儀表板視覺化呈現 ==========
df = fetch_analysis_data(target_year, calc_method)

if not df.empty:
    # A. 數據亮點
    total_samples = df.groupby('return_bin')['group_sample_count'].max().sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("總研究樣本 (含上市櫃)", f"{int(total_samples)} 檔")
    with col2:
        st.metric("時間跨度", f"{target_year} Full Year")
    with col3:
        st.metric("連線引擎", "PostgreSQL (Supabase)")

    # B. 熱力圖主視覺
    st.subheader(f"📈 {target_year} 漲幅分箱 vs 營收成長熱力圖 ({calc_method})")
    
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    
    # 建立 Plotly 熱力圖
    fig = px.imshow(
        pivot_df,
        labels=dict(x="資訊發布月份", y="年度漲幅區間", color="YoY %"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="RdYlGn_r",
        aspect="auto",
        text_auto=".1f"
    )
    
    fig.update_layout(xaxis_nticks=12)
    st.plotly_chart(fig, use_container_width=True)

    # C. 專業洞察
    st.markdown("""
    ### 🕵️ 數據洞察筆記
    * **樣本找回率**：本次更新使用了 `SPLIT_PART` 函數，成功解決了上櫃公司代號 (.TWO) 的匹配問題，樣本數已回升至全市場水平。
    * **關聯性分析**：觀察右側區間（高漲幅組），若顏色長期呈現深綠，說明營收增長具有**高度持續性**，這是長線飆股的特徵。
    * **異常排除**：若切換為『平均值』出現誇張數值，多為單一公司低基期影響，『中位數』更能反映組別共性。
    """)

    # D. 原始數據
    with st.expander("🔍 檢視完整數據矩陣"):
        st.dataframe(pivot_df.style.format("{:.1f}%"), use_container_width=True)

else:
    st.warning("⚠️ 查無數據，請確認資料庫中 stock_annual_k 與 monthly_revenue 是否已匯入正確年度之資料。")

st.markdown("---")
st.caption("Developed by StockRevenueLab Team | Powered by Streamlit & Supabase")
