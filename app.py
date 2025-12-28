import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="StockRevenueLab | 趨勢觀測站", page_icon="🧪", layout="wide")

# 自定義美化
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { border-left: 5px solid #ff4b4b; background-color: white; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

st.sidebar.success("🚀 數據已補齊！目前擁有 2019-2025 完整 16 萬筆數據。")

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
    except Exception:
        st.error("❌ 資料庫連線失敗")
        st.stop()

# ========== 3. 數據抓取引擎 (支援多指標) ==========
@st.cache_data(ttl=3600)
def fetch_main_data(year, metric_col, calc_method):
    engine = get_engine()
    # 統計指標選擇
    if calc_method == "中位數 (推薦)":
        agg_func = f"percentile_cont(0.5) WITHIN GROUP (ORDER BY m.{metric_col})"
    else:
        agg_func = f"AVG(m.{metric_col})"
    
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
    monthly_stats AS (
        SELECT stock_id, report_month, {metric_col} 
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND LENGTH(report_month) <= 6)
    )
    SELECT b.return_bin, m.report_month, {agg_func} as val, COUNT(DISTINCT b.symbol) as stock_count
    FROM annual_bins b
    JOIN monthly_stats m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    GROUP BY b.return_bin, m.report_month
    ORDER BY b.return_bin, m.report_month;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. 側邊欄控制項 ==========
st.sidebar.header("🔬 研究條件篩選")
target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)])
metric_choice = st.sidebar.radio("成長指標", ["年增率 (YoY)", "月增率 (MoM)"])
calc_method = st.sidebar.radio("統計指標", ["中位數 (推薦)", "平均值"])

target_col = "yoy_pct" if metric_choice == "年增率 (YoY)" else "mom_pct"

# ========== 5. 視覺化呈現 ==========
df = fetch_main_data(target_year, target_col, calc_method)

if not df.empty:
    col1, col2, col3 = st.columns(3)
    with col1: st.metric("觀測樣本數", f"{int(df.groupby('return_bin')['stock_count'].max().sum()):,} 檔")
    with col2: st.metric("研究指標", metric_choice)
    with col3: st.metric("數據完整度", f"{df['report_month'].nunique()} 個月")

    st.subheader(f"📊 {target_year} 「漲幅區間 vs {metric_choice}」熱力圖")
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')
    fig = px.imshow(pivot_df, color_continuous_scale="RdYlGn", aspect="auto", text_auto=".1f",
                    labels=dict(x="報表月份", y="漲幅區間", color=f"{metric_choice}%"))
    st.plotly_chart(fig, use_container_width=True)

    # ========== 6. 區間領頭羊 + 備註搜尋 ==========
    st.write("---")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        selected_bin = st.selectbox("🎯 選擇漲幅區間：", pivot_df.index[::-1])
    with col_b:
        search_keyword = st.text_input("🔍 搜尋公司名稱或備註關鍵字（如：產能、訂單、建案）：", "")

    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1

    detail_query = f"""
    WITH target_stocks AS (
        SELECT symbol FROM stock_annual_k 
        WHERE year = '{target_year}' AND (CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END) = '{selected_bin}'
    )
    SELECT m.stock_id as "代號", m.stock_name as "名稱",
           ROUND(AVG(m.yoy_pct)::numeric, 1) as "年增%", 
           ROUND(AVG(m.mom_pct)::numeric, 1) as "月增%",
           m.remark as "營收備註 (最後一筆)"
    FROM monthly_revenue m
    JOIN target_stocks t ON m.stock_id = SPLIT_PART(t.symbol, '.', 1)
    WHERE (m.report_month = '{prev_minguo_year}_12' OR m.report_month LIKE '{minguo_year}_%')
      AND (m.stock_name LIKE '%{search_keyword}%' OR m.remark LIKE '%{search_keyword}%')
    GROUP BY m.stock_id, m.stock_name, m.remark
    ORDER BY "年增%" DESC LIMIT 15;
    """
    with get_engine().connect() as conn:
        res_df = pd.read_sql_query(text(detail_query), conn)
        if not res_df.empty:
            st.write(f"🏆 **{selected_bin}** 區間中表現最強的公司：")
            st.dataframe(res_df, use_container_width=True)
        else:
            st.info("找不到符合關鍵字的資料。")

else:
    st.warning(f"⚠️ 2019-2023 數據已匯入，但可能尚未完成 14 欄位重建，請確認 import_db.py 執行成功。")
