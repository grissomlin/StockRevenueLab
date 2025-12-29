import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go

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

# ========== 3. 數據濃縮函數 (解決網址過長問題) ==========
def get_ai_summary_dist(df, col_name):
    """將分佈高度濃縮為 5 個核心區間以節省網址字數"""
    data = df[col_name].dropna()
    if data.empty: return "無數據"
    
    total = len(data)
    # 定義固定的核心區間
    bins = [-float('inf'), -5, -1, 1, 5, float('inf')]
    labels = ["大跌(<-5%)", "小跌(-5%~-1%)", "持平(-1%~1%)", "小漲(1%~5%)", "大漲(>5%)"]
    counts, _ = np.histogram(data, bins=bins)
    
    summary = []
    for label, count in zip(labels, counts):
        if count > 0:
            summary.append(f"{label}:{int(count)}檔({(count/total*100):.1f}%)")
    return " / ".join(summary)

def create_big_hist(df, col_name, title, color, desc):
    data = df[col_name].dropna()
    if data.empty: return
    counts, bins = np.histogram(data, bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    total = len(data)
    texts = [f"<b>{int(c)}檔</b>" for c in counts]
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=2)
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=400, margin=dict(t=80, b=40))
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **科學解讀：** {desc}")
    st.markdown("---")

# ========== 4. 核心 SQL ==========
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

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # A. 數據看板
    total_n = len(df)
    m_avg, w_avg, a_avg, f_avg = round(df['pre_month'].mean(), 2), round(df['pre_week'].mean(), 2), round(df['announce_week'].mean(), 2), round(df['after_month'].mean(), 2)
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本數", total_n)
    c2.metric("T-1月平均", f"{m_avg}%")
    c3.metric("T-1周平均", f"{w_avg}%")
    c4.metric("T周平均", f"{a_avg}%")
    c5.metric("T+1月平均", f"{f_avg}%")

    st.write("---")
    
    # B. 原始明細
    st.subheader(f"🏆 {target_year} 年 原始數據明細")
    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})

    st.write("---")

    # C. 分佈圖
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "公告前 30 天走勢。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b", "公告前一週走勢。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00", "公告當週表現。")
    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢區)", "#1e90ff", "一個月後的波段結局。")

    # D. AI 指令與密碼驗證
    st.divider()
    st.subheader("🤖 AI 投資行為診斷 (濃縮數據版)")
    
    # 生成濃縮的分佈文字
    dist_txt = (
        f"1.T-1月分佈: {get_ai_summary_dist(df, 'pre_month')}\n"
        f"2.T-1周分佈: {get_ai_summary_dist(df, 'pre_week')}\n"
        f"3.T周分佈: {get_ai_summary_dist(df, 'announce_week')}\n"
        f"4.T+1月分佈: {get_ai_summary_dist(df, 'after_month')}"
    )

    prompt_text = (
        f"分析台股 {target_year} 年營收爆發行為。樣本數 {total_n}。\n"
        f"平均報酬：T-1月 {m_avg}%, T-1周 {w_avg}%, T周 {a_avg}%, T+1月 {f_avg}%\n\n"
        f"【分佈摘要】\n{dist_txt}\n\n"
        f"請解讀此年度市場資訊先行程度，並給予策略建議。"
    )

    col_p, col_l = st.columns([2, 1])
    with col_p:
        st.code(prompt_text, language="text")
        st.caption("💡 如果自動跳轉失敗，請點擊右上角複製代碼後貼上。")

    with col_l:
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("♊ 開啟 Gemini (穩定推薦)", "https://gemini.google.com/app")
        st.link_button("🔥 開啟 ChatGPT (全自動嘗試)", f"https://chatgpt.com/?q={encoded_p}")
        
        if st.button("🔒 密碼驗證：直接提問"):
            st.session_state.check_pw = True

    if st.session_state.get("check_pw", False):
        with st.form("pw"):
            p = st.text_input("密碼：", type="password")
            if st.form_submit_button("執行"):
                if p == st.secrets["AI_ASK_PASSWORD"]:
                    st.success("通過！")
                    st.markdown(f'<meta http-equiv="refresh" content="0;url=https://chatgpt.com/?q={encoded_p}">', unsafe_allow_html=True)
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無樣本。")
