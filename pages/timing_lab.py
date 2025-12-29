import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go
import os

# 嘗試匯入 AI 套件
try:
    import google.generativeai as genai
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="公告行為研究室 | StockRevenueLab", layout="wide")

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

# ========== 3. 輔助函數 ==========
def get_ai_summary_dist(df, col_name):
    data = df[col_name].dropna()
    if data.empty: return "無數據"
    total = len(data)
    bins = [-float('inf'), -5, -1, 1, 5, float('inf')]
    labels = ["大跌(<-5%)", "小跌", "持平", "小漲", "大漲(>5%)"]
    counts, _ = np.histogram(data, bins=bins)
    summary = []
    for label, count in zip(labels, counts):
        if count > 0:
            summary.append(f"{label}:{int(count)}檔({(count/total*100):.1f}%)")
    return " / ".join(summary)

# ========== 4. 核心數據讀取 (含初次爆發邏輯) ==========
@st.cache_data(ttl=3600)
def fetch_timing_data(year, metric_col, limit, keyword):
    engine = get_engine()
    minguo_year = int(year) - 1911
    query = f"""
    WITH raw_events AS (
        SELECT stock_id, stock_name, report_month, {metric_col}, remark,
               LAG({metric_col}) OVER (PARTITION BY stock_id ORDER BY report_month) as prev_metric
        FROM monthly_revenue
        WHERE report_month LIKE '{minguo_year}_%' OR report_month LIKE '{int(minguo_year)-1}_12'
    ),
    spark_events AS (
        SELECT *,
               CASE 
                 WHEN RIGHT(report_month, 2) = '12' THEN (LEFT(report_month, 3)::int + 1 + 1911)::text || '-01-10'
                 ELSE (LEFT(report_month, 3)::int + 1911)::text || '-' || LPAD((RIGHT(report_month, 2)::int + 1)::text, 2, '0') || '-10'
               END::date as base_date
        FROM raw_events
        WHERE {metric_col} >= {limit} 
          AND (prev_metric < {limit} OR prev_metric IS NULL)
          AND report_month LIKE '{minguo_year}_%'
          AND (remark LIKE '%%{keyword}%%' OR stock_name LIKE '%%{keyword}%%')
    ),
    weekly_calc AS (
        SELECT symbol, date, w_close,
               (w_close - LAG(w_close) OVER (PARTITION BY symbol ORDER BY date)) / 
               NULLIF(LAG(w_close) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 as weekly_ret
        FROM stock_weekly_k
    ),
    final_detail AS (
        SELECT 
            e.stock_id, e.stock_name, e.report_month, e.{metric_col} as growth_val, e.remark,
            AVG(CASE WHEN c.date >= e.base_date - interval '38 days' AND c.date < e.base_date - interval '9 days' THEN c.weekly_ret END) * 4 as pre_month,
            AVG(CASE WHEN c.date >= e.base_date - interval '9 days' AND c.date <= e.base_date - interval '3 days' THEN c.weekly_ret END) as pre_week,
            AVG(CASE WHEN c.date > e.base_date - interval '3 days' AND c.date <= e.base_date + interval '4 days' THEN c.weekly_ret END) as announce_week,
            AVG(CASE WHEN c.date > e.base_date + interval '4 days' AND c.date <= e.base_date + interval '11 days' THEN c.weekly_ret END) as after_week_1,
            AVG(CASE WHEN c.date > e.base_date + interval '11 days' AND c.date <= e.base_date + interval '30 days' THEN c.weekly_ret END) as after_month
        FROM spark_events e
        JOIN weekly_calc c ON e.stock_id = SPLIT_PART(c.symbol, '.', 1)
        GROUP BY e.stock_id, e.stock_name, e.report_month, e.{metric_col}, e.remark, e.base_date
    )
    SELECT * FROM final_detail WHERE pre_week IS NOT NULL ORDER BY pre_month DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 5. 使用介面區 ==========
with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 關鍵字搜尋", "")

st.title(f"🕵️ {target_year} 年 公告行為研究室 4.0")

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # A. 數據看板 (新增中位數)
    total_n = len(df)
    
    # 計算平均與中位數
    stats = {
        "m_mean": round(df['pre_month'].mean(), 2),
        "m_median": round(df['pre_month'].median(), 2),
        "w_mean": round(df['pre_week'].mean(), 2),
        "a_mean": round(df['announce_week'].mean(), 2),
        "f_median": round(df['after_month'].median(), 2)
    }
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本總數", total_n)
    c2.metric("T-1月平均 / 中位", f"{stats['m_mean']}% / {stats['m_median']}%")
    c3.metric("T-1周平均", f"{stats['w_mean']}%")
    c4.metric("T周(公告)平均", f"{stats['a_mean']}%")
    c5.metric("T+1月(波段)中位", f"{stats['f_median']}%")
    st.write("---")
    
    # B. 原始數據明細 (新增複製功能)
    st.subheader("🏆 原始數據明細")
    
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        # 將全量數據轉成 Markdown 方便 AI 閱讀
        copy_data = df[['stock_id', 'stock_name', 'growth_val', 'pre_month', 'after_month', 'remark']].to_markdown(index=False)
        st.download_button(label="📋 下載全量明細 (CSV)", data=df.to_csv(index=False).encode('utf-8'), file_name=f'stock_data_{target_year}.csv')
    
    with col_btn2:
        if st.checkbox("🔍 顯示全量 Markdown 數據 (用於手動複製給 AI)"):
            st.code(copy_data, language="text")
            st.caption("提示：這會包含所有檔名的漲幅與備註，適合餵給 Claude 3.5 或 Gemini Pro 進行深度個股診斷。")

    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})
    st.write("---")

    # C. AI 診斷 (加入中位數對照)
    st.divider()
    st.subheader("🤖 AI 投資行為診斷 (含中位數分析)")
    
    dist_txt = (
        f"1.T-1月分佈: {get_ai_summary_dist(df, 'pre_month')}\n"
        f"2.T-1周分佈: {get_ai_summary_dist(df, 'pre_week')}\n"
        f"3.T周分佈: {get_ai_summary_dist(df, 'announce_week')}\n"
        f"4.T+1月分佈: {get_ai_summary_dist(df, 'after_month')}"
    )

    prompt_text = (
        f"分析台股 {target_year} 年營收爆發行為。樣本數 {total_n}。\n"
        f"【核心數據統計】：\n"
        f"- T-1月：平均 {stats['m_mean']}%, 中位數 {stats['m_median']}%\n"
        f"- T-1周：平均 {stats['w_mean']}%\n"
        f"- T周(公告)：平均 {stats['a_mean']}%\n"
        f"- T+1月(波段)：中位數 {stats['f_median']}%\n\n"
        f"【分佈摘要】：\n{dist_txt}\n\n"
        f"請解讀：當『平均值』遠大於『中位數』時，是否代表僅有少數飆股撐場？針對此統計特徵，建議投資人該如何佈局？"
    )

    col_p, col_l = st.columns([2, 1])
    with col_p:
        st.code(prompt_text, language="text")
    
    with col_l:
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("🔥 ChatGPT (網址自動帶入)", f"https://chatgpt.com/?q={encoded_p}")
        st.link_button("♊ 開啟 Gemini 官網 (強烈推薦貼上明細)", "https://gemini.google.com/app")
        
        if st.button("🔒 啟動內建 Gemini 深度診斷"):
            st.session_state.run_ai_4 = True

    if st.session_state.get("run_ai_4", False):
        with st.form("ai_4"):
            user_pw = st.text_input("研究員密碼：", type="password")
            if st.form_submit_button("執行"):
                if user_pw == st.secrets["AI_ASK_PASSWORD"]:
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    with st.spinner("AI 正在比對平均數與中位數..."):
                        response = model.generate_content(prompt_text)
                        st.info("### 🤖 內建專家診斷報告")
                        st.markdown(response.text)
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無符合條件之樣本。")
