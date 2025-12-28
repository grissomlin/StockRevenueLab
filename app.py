import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置與標題 ==========
st.set_page_config(
    page_title="StockRevenueLab | 台股量化研究室",
    page_icon="🧪",
    layout="wide"
)

# 自定義 CSS 讓界面更具專業感
st.markdown("""
    <style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🧪 StockRevenueLab: 飆股基因與營收關聯深度研究")
st.markdown("""
本研究室旨在透過大數據分析（2020-2025），實證台股市場中**「年度漲幅」**與**「月報揭露資訊」**之間的因果律。
我們關注的核心問題是：*強勁的營收增長，是否真的是驅動超額報酬的唯一燃料？*
---
""")

# ========== 2. 安全資料庫連線 (使用 Streamlit Secrets) ==========
@st.cache_resource
def get_engine():
    try:
        # 從 Streamlit 後台的 Secrets 讀取敏感資訊
        DB_PASSWORD = st.secrets["DB_PASSWORD"]
        PROJECT_REF = st.secrets["PROJECT_REF"]
        POOLER_HOST = st.secrets["POOLER_HOST"]
        
        encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
        connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
        return create_engine(connection_string)
    except Exception as e:
        st.error("❌ 無法讀取資料庫連線資訊。請確認 Streamlit Secrets 是否已設定。")
        st.info("需要在 Secrets 設定: DB_PASSWORD, PROJECT_REF, POOLER_HOST")
        st.stop()

# ========== 3. 數據抓取與處理邏輯 ==========
@st.cache_data(ttl=3600)
def fetch_analysis_data(year, calc_method):
    engine = get_engine()
    
    # 根據選擇切換計算方式：中位數 (排除極端值) 或 平均值
    if calc_method == "中位數 (推薦)":
        agg_func = "percentile_cont(0.5) WITHIN GROUP (ORDER BY m.yoy_pct)"
    else:
        agg_func = "AVG(m.yoy_pct)"
    
    # 精確對齊 SQL：對應台灣財報揭露滯後性 (民國紀年)
    # 研究 2024 年時，應參考 112_12 至 113_11 的報表
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
        COUNT(DISTINCT b.symbol) as stock_count
    FROM annual_bins b
    JOIN monthly_yoy m ON LEFT(b.symbol, 4) = m.stock_id
    GROUP BY b.return_bin, m.report_month
    ORDER BY b.return_bin, m.report_month;
    """
    
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. 側邊欄與交互控制 ==========
st.sidebar.image("https://img.icons8.com/fluency/96/test-tube.png", width=80)
st.sidebar.header("研究參數篩選")

target_year = st.sidebar.selectbox("觀測年度", ["2024", "2025"], index=0)
calc_method = st.sidebar.radio(
    "統計量指標", 
    ["中位數 (推薦)", "平均值"], 
    help="中位數能有效過濾掉因低基期導致的萬%營收成長雜訊，反映群體真實趨勢。"
)

st.sidebar.markdown("---")
st.sidebar.write("⚙️ **系統狀態**")
st.sidebar.success("資料庫連線正常")
st.sidebar.info(f"當前觀測：{target_year} 年數據")

# ========== 5. 主要視覺化看板 ==========
df = fetch_analysis_data(target_year, calc_method)

if not df.empty:
    # 轉換數據格式供熱力圖使用
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    
    # A. 數據概覽 Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("研究樣本總數", f"{df['stock_count'].max()} 檔")
    with col2:
        st.metric("觀測月份", "12 個月")
    with col3:
        st.metric("數據精度", "日線級聚合")

    # B. 交互式熱力圖
    st.subheader(f"📊 {target_year} 年「漲幅區間 vs 月營收 YoY」動態熱力圖")
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="資訊揭露月份 (民國_月)", y="年度漲幅區間", color="營收年增率 %"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="RdYlGn_r", # 綠色代表高成長，紅色代表衰退
        aspect="auto",
        text_auto=".1f"
    )
    
    fig.update_layout(
        xaxis_nticks=12,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=30, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # C. 研究洞察總結
    with st.expander("💡 如何解讀這張熱力圖？"):
        st.markdown(f"""
        1. **顏色越綠 (YoY 越高)**：代表該漲幅區間的股票，其營收成長動能越強。
        2. **橫向觀察**：看特定組別（如 100-200%）是否在整年都維持穩定的綠色，這代表「持續性成長」。
        3. **縱向觀察**：看某個月份是否全市場集體變綠，這反映了整體的經濟循環或季節性效應。
        4. **極端值警告**：若使用『平均值』看到數千%的數字，通常是低基期陷阱，建議切換回『中位數』。
        """)

    # D. 原始數據表格
    st.subheader("📋 詳細數據矩陣")
    st.dataframe(
        pivot_df.style.background_gradient(cmap='RdYlGn_r', axis=None).format("{:.1f}%"),
        use_container_width=True
    )

else:
    st.warning("⚠️ 數據加載中或資料庫內無符合條件之數據。")

# ========== 6. 頁尾資訊 ==========
st.markdown("---")
st.caption(f"© 2025 StockRevenueLab | 數據來源：Supabase Cloud PostgreSQL | 最後更新：{target_year}-12")
