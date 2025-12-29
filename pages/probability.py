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

# ========== 3. 數據抓取引擎 ==========
@st.cache_data(ttl=3600)
def fetch_prob_data(year, metric_col, low, high):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
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
           ROUND(AVG(p.ret)::numeric, 1) as "平均年度漲幅%",
           ROUND((COUNT(*) FILTER (WHERE p.ret > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(>20%)",
           ROUND((COUNT(*) FILTER (WHERE p.ret > 100) * 100.0 / COUNT(*))::numeric, 1) as "翻倍率(>100%)"
    FROM hit_table h JOIN perf_table p ON h.stock_id = p.stock_id
    GROUP BY h.hits ORDER BY h.hits DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. UI 介面設計 ==========
st.title("🎲 營收爆發與年度報酬機率")
st.markdown("""
透過 12 份影響年度股價的報表（**前年底 12 月 ~ 當年 11 月**），
我們統計當營收爆發次數增加時，該股票在該年度 **年 K 線** 的表現期望值。
""")

with st.sidebar:
    st.header("🔬 設定研究參數")
    target_year = st.sidebar.selectbox("研究年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.selectbox("研究指標", ["yoy_pct", "mom_pct"], index=0, help="yoy為年增率，mom為月增率")
    growth_range = st.select_slider(
        "設定爆發區間 (%)", 
        options=[-50, 0, 20, 50, 100, 500, 1000], 
        value=(100, 1000)
    )

df_prob = fetch_prob_data(target_year, study_metric, growth_range[0], growth_range[1])

if not df_prob.empty:
    # A. 顯示統計總表
    st.subheader(f"📊 {target_year} 年：營收達標次數 vs 期望報酬對照表")
    st.table(df_prob)
    
    # B. AI 分析助手區 (手動建構 Markdown 表格以避免依賴錯誤)
    st.write("---")
    st.subheader("🤖 AI 全數據策略診斷")
    
    # 手動建立表格文字，不使用 to_markdown()
    header = "| " + " | ".join(df_prob.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(df_prob.columns)) + " |"
    rows = []
    for _, row in df_prob.iterrows():
        rows.append("| " + " | ".join(map(str, row.values)) + " |")
    table_md = "\n".join([header, separator] + rows)
    
    prompt_text = (
        f"請擔任專業量化分析師，分析台灣股市 {target_year} 年的數據。\n"
        f"研究條件：營收 {study_metric} 落在 {growth_range[0]}% 至 {growth_range[1]}% 區間。\n\n"
        f"以下是完整統計對照表：\n\n{table_md}\n\n"
        f"請針對這份表格進行深度分析：\n"
        f"1. 觀察『爆發次數』與『平均年度漲幅』、『勝率』之間是否存在正相關？\n"
        f"2. 找出期望值最高（兼顧樣本數與漲幅）的黃金次數區間。\n"
        f"3. 根據此數據趨勢，給予投資者在面對營收連續爆發股時的操作建議。"
    )

    col_prompt, col_link = st.columns([2, 1])
    with col_prompt:
        st.write("📋 **第一步：複製完整分析數據**")
        st.code(prompt_text, language="text")
        st.caption("提示：此提示詞已包含上方整張表格內容，AI 能進行全樣本趨勢分析。")

    with col_link:
        st.write("🚀 **第二步：送往 AI**")
        encoded_prompt = urllib.parse.quote(prompt_text)
        
        st.link_button("🔥 ChatGPT (全自動帶入)", f"https://chatgpt.com/?q={encoded_prompt}")
        st.link_button("Ⓜ️ Copilot (需貼上)", "https://www.bing.com/chat")
        st.link_button("🌐 Claude.ai (需貼上)", "https://claude.ai/")
        st.info("💡 只有 ChatGPT 支援完整帶入。")

    # C. 點名功能
    st.write("---")
    st.subheader("🔍 區間名單點名")
    hit_options = df_prob["爆發次數"].tolist()
    selected_hits = st.selectbox("選擇『爆發次數』查看具體名單：", hit_options)
    
    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1
    
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
        st.write(f"🏆 {target_year} 年『營收達標 {selected_hits} 次』的股票清單：")
        st.dataframe(detail_df, use_container_width=True)

    # D. 勝率視覺化
    st.write("---")
    st.subheader("🎯 期望值視覺化")
    chart_data = df_prob.set_index("爆發次數")[["勝率(>20%)", "翻倍率(>100%)"]]
    st.bar_chart(chart_data)

else:
    st.info(f"💡 在 {target_year} 年及設定區間下，沒有符合條件的樣本。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 數據週期：2019-2025")
