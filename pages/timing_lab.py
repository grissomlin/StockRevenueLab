import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go
import os
from scipy.stats import skew, kurtosis

# 嘗試匯入 AI 套件
try:
    import google.generativeai as genai
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="公告行為研究室 5.0 | 全維度診斷", layout="wide")

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

# ========== 3. 數據輔助函數 ==========
def get_ai_summary_dist(df, col_name):
    data = df[col_name].dropna()
    if data.empty: return "無數據"
    total = len(data)
    bins = [-float('inf'), -5, -1, 1, 5, float('inf')]
    labels = ["大跌(<-5%)", "小跌", "持平", "小漲", "大漲(>5%)"]
    counts, _ = np.histogram(data, bins=bins)
    return " / ".join([f"{l}:{int(c)}檔({(c/total*100):.1f}%)" for l, c in zip(labels, counts) if c > 0])

def create_big_hist(df, col_name, title, color, desc):
    data = df[col_name].dropna()
    if data.empty: return
    m, med = data.mean(), data.median()
    counts, bins = np.histogram(data, bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=[f"{int(c)}" for c in counts], textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black")
    fig.add_vline(x=m, line_color="red", line_width=2, annotation_text=f"平均 {m:.1f}%")
    fig.add_vline(x=med, line_color="blue", line_width=2, annotation_text=f"中位 {med:.1f}%", annotation_position="bottom right")
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=400, margin=dict(t=80, b=40))
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **科學解讀：** {desc}")
    st.markdown("---")

# ========== 4. 核心數據讀取 ==========
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

# ========== 5. 介面與統計 ==========
with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider("爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 搜尋備註", "")

st.title(f"🕵️ {target_year} 年 公告行為研究室 5.0")

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # A. 全維度統計看板
    total_n = len(df)
    def get_stats(col):
        d = df[col].dropna()
        cv = d.std() / abs(d.mean()) if d.mean() != 0 else 0
        return d.mean(), d.median(), skew(d), kurtosis(d), cv

    m_mean, m_med, m_sk, m_ku, m_cv = get_stats('pre_month')
    w_mean, w_med, w_sk, w_ku, w_cv = get_stats('pre_week')
    a_mean, a_med, a_sk, a_ku, a_cv = get_stats('announce_week')
    aw1_mean, aw1_med, _, _, _ = get_stats('after_week_1')
    f_mean, f_med, _, _, _ = get_stats('after_month')

    st.subheader("🔬 核心統計看板")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本總數", f"{total_n} 檔")
    c2.metric("T-1月 (平均/中位)", f"{m_mean:.2f}%", f"中位: {m_med:.2f}%")
    c3.metric("T-1周 (平均/中位)", f"{w_mean:.2f}%", f"中位: {w_med:.2f}%")
    c4.metric("T周公告 (平均/中位)", f"{a_mean:.2f}%", f"中位: {a_med:.2f}%")
    c5.metric("T+1月波段 (中位數)", f"{f_med:.2f}%")
    
    st.write(f"**T-1月 偏度 (Skewness):** `{m_sk:.2f}` | **峰度 (Kurtosis):** `{m_ku:.2f}` | **變異係數 (CV):** `{m_cv:.2f}`")
    st.write("---")

    # B. 明細與複製
    st.subheader("🏆 原始數據明細")
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn2:
        if st.checkbox("🔍 產生 AI 深度診斷指令 (含右尾強勢股分析)"):
            # 找出 T-1 月漲幅 > 5% 的右尾個股
            right_tail_df = df[df['pre_month'] > 5]
            rt_count = len(right_tail_df)
            rt_mean = right_tail_df['pre_month'].mean()
            rt_list = right_tail_df[['stock_id', 'stock_name', 'pre_month', 'remark']].head(50).to_markdown(index=False)
            
            prompt_text = (
                f"分析台股 {target_year} 年營收爆發行為。總樣本 {total_n} 檔。\n"
                f"【宏觀統計】：T-1月平均 {m_mean:.2f}% / 中位數 {m_med:.2f}% (偏度 {m_sk:.2f}, 峰度 {m_ku:.2f})。\n"
                f"【右尾強勢標的分析】：\n"
                f"- 在公告前一個月即大漲 > 5% 的股票共有 {rt_count} 檔 (佔比 {(rt_count/total_n*100):.1f}%)。\n"
                f"- 這群『先行者』的平均漲幅高達 {rt_mean:.2f}%。\n\n"
                f"【部分先行者名單】：\n{rt_list}\n\n"
                f"請分析：這 {rt_count} 檔右尾標的是否存在顯著的『資訊不對稱』或產業集中現象？散戶應如何識別這些標的以避開公告後的利多出盡？"
            )
            st.code(prompt_text, language="text")

    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})
    st.write("---")

    # C. 分佈圖回歸
    st.subheader("📊 公告前後分佈趨勢")
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "觀察極端值。平均與中位數差值越巨，代表主力先行越精準。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (預跑區)", "#ff4b4b", "短線消息面反應。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00", "市場對營收爆發的最終定價。")
    create_big_hist(df, "after_week_1", "❸ T+1 周 (延續區)", "#32cd32", "漲勢是否具備慣性。")
    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢結局)", "#1e90ff", "利多出盡後的殘留價值。")

    # D. 內建 AI 診斷
    st.divider()
    if st.button("🔒 啟動內建 Gemini 專家診斷 (含右尾分析)"):
        st.session_state.run_ai_5 = True

    if st.session_state.get("run_ai_5", False):
        with st.form("ai_5"):
            pw = st.text_input("密碼：", type="password")
            if st.form_submit_button("執行"):
                if pw == st.secrets["AI_ASK_PASSWORD"]:
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    with st.spinner("正在進行偏度與右尾標的交叉解讀..."):
                        # 使用上面生成的 prompt_text
                        res = model.generate_content(prompt_text)
                        st.info("### 🤖 內建專家報告")
                        st.markdown(res.text)
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無資料。")
