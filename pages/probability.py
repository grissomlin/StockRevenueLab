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
    
    # 修改後的 SQL：統計與明細分離
    query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits 
        FROM monthly_revenue 
        WHERE (report_month LIKE '{minguo_year}_%' OR report_month = '{prev_minguo_year}_12')
          AND {metric_col} >= {low} AND {metric_col} < {high}
        GROUP BY stock_id
    ),
    perf_table AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, 
               ((year_close - year_open) / year_open)*100 as ret,
               symbol
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

st.title("🎲 營收爆發與股價期望值")

with st.sidebar:
    target_year = st.selectbox("研究年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.selectbox("研究指標", ["yoy_pct", "mom_pct"])
    growth_range = st.select_slider("設定爆發區間 (%)", options=[-50, 0, 20, 50, 100, 500, 1000], value=(50, 500))

df_prob = fetch_prob_data(target_year, study_metric, growth_range[0], growth_range[1])

if not df_prob.empty:
    st.subheader(f"📊 {target_year} 年：營收爆發次數統計")
    st.table(df_prob)
    
    # --- 新增：點名功能 ---
    st.write("---")
    st.subheader("🔍 查看該次數下的股票名單")
    selected_hits = st.selectbox("選擇要點名的爆發次數：", df_prob["爆發次數"].tolist())
    
    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1
    
    list_query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits 
        FROM monthly_revenue 
        WHERE (report_month LIKE '{minguo_year}_%' OR report_month = '{prev_minguo_year}_12')
          AND {study_metric} >= {growth_range[0]} AND {study_metric} < {growth_range[1]}
        GROUP BY stock_id
    )
    SELECT h.stock_id as "代號", m.stock_name as "名稱",
           ROUND(((k.year_close - k.year_open)/k.year_open*100)::numeric, 1) as "年度漲幅%",
           STRING_AGG(DISTINCT m.remark, ' | ') FILTER (WHERE m.remark <> '-' AND m.remark <> '') as "關鍵備註"
    FROM hit_table h
    JOIN stock_annual_k k ON h.stock_id = SPLIT_PART(k.symbol, '.', 1) AND k.year = '{target_year}'
    JOIN monthly_revenue m ON h.stock_id = m.stock_id AND (m.report_month LIKE '{minguo_year}_%' OR m.report_month = '{prev_minguo_year}_12')
    WHERE h.hits = {selected_hits}
    GROUP BY h.stock_id, m.stock_name, k.year_close, k.year_open
    ORDER BY "年度漲幅%" DESC;
    """
    
    with get_engine().connect() as conn:
        detail_stocks = pd.read_sql_query(text(list_query), conn)
        st.write(f"🏆 爆發 **{selected_hits}** 次的股票清單（按漲幅排序）：")
        st.dataframe(detail_stocks, use_container_width=True)

    st.bar_chart(df_prob.set_index("爆發次數")[["勝率(>20%)", "翻倍率(>100%)"]])
else:
    st.info("此年度或條件下暫無足夠樣本。")
