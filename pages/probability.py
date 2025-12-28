import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="機率研究室 | StockRevenueLab", layout="wide")

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

# ========== 3. 數據抓取引擎 (12個月精確版) ==========
@st.cache_data(ttl=3600)
def fetch_prob_data(year, metric_col, low, high):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    # 核心邏輯：抓取影響該年度股價的 12 份黃金報表
    # 起點：前年底 12 月 (於當年 1/10 公布)
    # 終點：當年 11 月 (於當年 12/10 公布)
    query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits 
        FROM monthly_revenue 
        WHERE (
            report_month = '{prev_minguo_year}_12' 
            OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
        )
        AND {metric_col} >= {low} AND {metric_col} < {high}
        GROUP BY stock_id
    ),
    perf_table AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, 
               ((year_close - year_open) / year_open)*100 as ret
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
        return pd.read_sql_query(text(query), conn)

# ========== 4. UI 介面設計 ==========
st.title("🎲 營收爆發與股價期望值")
st.markdown("##### 研究「營收連續達標」與「股價翻倍機率」的因果關係")

with st.sidebar:
    st.header("🔬 設定研究參數")
    target_year = st.selectbox("研究年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.selectbox("研究指標", ["yoy_pct", "mom_pct"], index=0, help="yoy為年增率，mom為月增率")
    # 設定爆發區間
    growth_range = st.select_slider(
        "設定營收年增率 (YoY) 爆發區間", 
        options=[-50, 0, 20, 50, 100, 500, 1000], 
        value=(100, 1000)
    )

# 執行分析
df_prob = fetch_prob_data(target_year, study_metric, growth_range[0], growth_range[1])

if not df_prob.empty:
    # A. 顯示統計總表
    st.subheader(f"📊 {target_year} 年：營收達標次數統計 (全市場樣本)")
    st.table(df_prob)
    
    # B. 點名功能：找出是哪些股票
    st.write("---")
    st.subheader("🔍 區間名單點名")
    
    # 取得當前表格中的爆發次數列表
    hit_options = df_prob["爆發次數"].tolist()
    selected_hits = st.selectbox("請選擇『爆發次數』來查看具體股票名單：", hit_options)
    
    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1
    
    # 查詢名單的 SQL (同樣採用 12 個月邏輯)
    list_query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits 
        FROM monthly_revenue 
        WHERE (
            report_month = '{prev_minguo_year}_12' 
            OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
        )
        AND {study_metric} >= {growth_range[0]} AND {study_metric} < {growth_range[1]}
        GROUP BY stock_id
    )
    SELECT h.stock_id as "代號", m.stock_name as "名稱",
           ROUND(((k.year_close - k.year_open)/k.year_open*100)::numeric, 1) as "年度漲幅%",
           ROUND(AVG(m.yoy_pct)::numeric, 1) as "年增平均%",
           STRING_AGG(DISTINCT m.remark, ' | ') FILTER (WHERE m.remark <> '-' AND m.remark <> '') as "關鍵備註"
    FROM hit_table h
    JOIN stock_annual_k k ON h.stock_id = SPLIT_PART(k.symbol, '.', 1) AND k.year = '{target_year}'
    JOIN monthly_revenue m ON h.stock_id = m.stock_id 
      AND (m.report_month LIKE '{minguo_year}_%' OR m.report_month = '{prev_minguo_year}_12')
    WHERE h.hits = {selected_hits}
    GROUP BY h.stock_id, m.stock_name, k.year_close, k.year_open
    ORDER BY "年度漲幅%" DESC;
    """
    
    with get_engine().connect() as conn:
        detail_df = pd.read_sql_query(text(list_query), conn)
        st.write(f"🏆 在 {target_year} 年『營收爆發 {selected_hits} 次』的名單如下：")
        st.dataframe(detail_df, use_container_width=True)

    # C. 勝率視覺化
    st.write("---")
    st.subheader("🎯 期望值視覺化")
    chart_data = df_prob.set_index("爆發次數")[["勝率(>20%)", "翻倍率(>100%)"]]
    st.bar_chart(chart_data)

else:
    st.info(f"💡 在 {target_year} 年及設定的區間下，沒有符合條件的股票樣本。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 讓數據說真話")
