import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置 ==========
st.set_page_config(
    page_title="StockRevenueLab | 趨勢觀測站",
    page_icon="🧪",
    layout="wide"
)

# 自定義 CSS 美化
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { border-left: 5px solid #ff4b4b; background-color: white; padding: 10px; border-radius: 5px; }
    div[data-testid="stExpander"] { border: 1px solid #e0e0e0; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# 側邊欄導引
st.sidebar.success("💡 想要看『勝率分析』？請點選左側選單的 probability 頁面！")

st.title("🧪 StockRevenueLab: 全時段飆股基因對帳單")
st.markdown("#### 透過 16 萬筆真實數據，揭開業績與股價漲幅的神秘面紗")

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
        st.error("❌ 資料庫連線失敗，請檢查 Streamlit Secrets 設定。")
        st.stop()

# ========== 3. 數據抓取引擎 (熱力圖專用) ==========
@st.cache_data(ttl=3600)
def fetch_heatmap_data(year, metric_col, calc_method):
    engine = get_engine()
    # 決定聚合函數
    if calc_method == "中位數 (推薦)":
        agg_func = f"percentile_cont(0.5) WITHIN GROUP (ORDER BY m.{metric_col})"
    else:
        agg_func = f"AVG(m.{metric_col})"
    
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    # 這裡的邏輯：抓取前一年 12 月 + 當年 1~12 月，共 13 份報表
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
    monthly_stats AS (
        SELECT stock_id, report_month, {metric_col} 
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND LENGTH(report_month) <= 7)
    )
    SELECT 
        b.return_bin,
        m.report_month,
        {agg_func} as val,
        COUNT(DISTINCT b.symbol) as stock_count
    FROM annual_bins b
    JOIN monthly_stats m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    GROUP BY b.return_bin, m.report_month
    ORDER BY b.return_bin, m.report_month;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. 側邊欄 UI ==========
st.sidebar.header("🔬 研究條件篩選")
target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
metric_choice = st.sidebar.radio("成長指標", ["年增率 (YoY)", "月增率 (MoM)"], help="YoY看長期趨勢，MoM看短期爆發")
calc_method = st.sidebar.radio("統計指標", ["中位數 (推薦)", "平均值"], help="中位數能排除極端離群值")

target_col = "yoy_pct" if metric_choice == "年增率 (YoY)" else "mom_pct"

# ========== 5. 儀表板主視圖 ==========
df = fetch_heatmap_data(target_year, target_col, calc_method)

if not df.empty:
    # 頂部指標
    actual_months = df['report_month'].nunique()
    total_samples = df.groupby('return_bin')['stock_count'].max().sum()
    
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("研究樣本總數", f"{int(total_samples):,} 檔")
    with c2: st.metric("當前觀測年度", f"{target_year} 年")
    with c3: st.metric("數據完整度", f"{actual_months} 個月份")

    # 熱力圖
    st.subheader(f"📊 {target_year} 「漲幅區間 vs {metric_choice}」業績對照熱力圖")
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="報表月份", y="漲幅區間", color=f"{metric_choice} %"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="RdYlGn",
        aspect="auto",
        text_auto=".1f"
    )
    fig.update_xaxes(side="top")
    st.plotly_chart(fig, use_container_width=True)

    # ========== 6. 深度挖掘：領頭羊與備註搜尋 ==========
    st.write("---")
    st.subheader(f"🔍 {target_year} 深度挖掘：區間業績王與關鍵字搜尋")
    st.info("想知道為什麼某個區間營收特別綠？直接選取該區間，並輸入關鍵字搜尋原因！")

    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        selected_bin = st.selectbox("🎯 選擇漲幅區間：", pivot_df.index[::-1])
    with col_b:
        display_limit = st.select_slider("顯示筆數", options=[10, 20, 50, 100], value=50)
    with col_c:
        search_keyword = st.text_input("💡 備註關鍵字（如：建案、訂單、CoWoS、新機）：", "")

    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1

    # 強大的 SQL：整合漲幅、平均營收與最新備註
    detail_query = f"""
    WITH target_stocks AS (
        SELECT symbol, ((year_close - year_open) / year_open) * 100 as annual_ret 
        FROM stock_annual_k 
        WHERE year = '{target_year}' AND (CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END) = '{selected_bin}'
    ),
    latest_remarks AS (
        -- 取得該年度最後一個有備註的月份資料
        SELECT DISTINCT ON (stock_id) stock_id, remark 
        FROM monthly_revenue 
        WHERE (report_month LIKE '{minguo_year}_%' OR report_month = '{prev_minguo_year}_12')
          AND remark IS NOT NULL AND remark <> '-' AND remark <> ''
        ORDER BY stock_id, report_month DESC
    )
    SELECT 
        m.stock_id as "代號", 
        m.stock_name as "名稱",
        ROUND(t.annual_ret::numeric, 1) as "年度實際漲幅%",
        ROUND(AVG(m.yoy_pct)::numeric, 1) as "年增平均%", 
        ROUND(AVG(m.mom_pct)::numeric, 1) as "月增平均%",
        r.remark as "最新營收備註"
    FROM monthly_revenue m
    JOIN target_stocks t ON m.stock_id = SPLIT_PART(t.symbol, '.', 1)
    LEFT JOIN latest_remarks r ON m.stock_id = r.stock_id
    WHERE (m.report_month LIKE '{minguo_year}_%' OR m.report_month = '{prev_minguo_year}_12')
      AND (m.stock_name LIKE '%{search_keyword}%' OR m.remark LIKE '%{search_keyword}%')
    GROUP BY m.stock_id, m.stock_name, t.annual_ret, r.remark
    ORDER BY "年度實際漲幅%" DESC 
    LIMIT {display_limit};
    """
    
    with get_engine().connect() as conn:
        res_df = pd.read_sql_query(text(detail_query), conn)
        if not res_df.empty:
            st.write(f"🏆 在 **{selected_bin}** 區間中，符合條件的前 {len(res_df)} 檔公司：")
            st.dataframe(res_df, use_container_width=True, height=500)
        else:
            st.info("💡 目前區間或關鍵字下找不到符合的公司。")

    with st.expander("👉 查看原始數據矩陣"):
        st.dataframe(pivot_df.style.format("{:.1f}%"), use_container_width=True)

else:
    st.warning(f"⚠️ 找不到 {target_year} 年的數據。請確認資料庫中已匯入該年度股價與營收。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 讓 16 萬筆數據說真話")
