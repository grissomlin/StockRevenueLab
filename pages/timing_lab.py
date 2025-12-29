import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go
import os

# 嘗試匯入統計與 AI 套件
try:
    from scipy.stats import skew, kurtosis
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    import google.generativeai as genai
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="公告行為研究室 5.5 | 全功能旗艦版", layout="wide")

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
    fig.add_vline(x=m, line_color="red", line_width=2, annotation_text=f"平均 {m:.2f}%")
    fig.add_vline(x=med, line_color="blue", line_width=2, annotation_text=f"中位 {med:.2f}%")
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=400, margin=dict(t=80, b=40))
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **解讀：** {desc}")

# ========== 4. 核心數據抓取 (SQL) ==========
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

# ========== 5. 主程式流程 ==========
with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider("爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 搜尋備註", "")

st.title(f"🕵️ {target_year} 年 公告行為研究室 5.5")

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # --- A. 統計看板 ---
    total_n = len(df)
    m_mean, m_med = df['pre_month'].mean(), df['pre_month'].median()
    m_sk = skew(df['pre_month']) if SCIPY_AVAILABLE else 0
    m_ku = kurtosis(df['pre_month']) if SCIPY_AVAILABLE else 0
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本總數", f"{total_n} 檔")
    c2.metric("T-1月 (平均/中位)", f"{m_mean:.2f}%", f"中位: {m_med:.2f}%")
    c3.metric("T-1周 (平均/中位)", f"{df['pre_week'].mean():.2f}%", f"中位: {df['pre_week'].median():.2f}%")
    c4.metric("T周公告 (平均/中位)", f"{df['announce_week'].mean():.2f}%", f"中位: {df['announce_week'].median():.2f}%")
    c5.metric("T+1月波段 (中位)", f"{df['after_month'].median():.2f}%")
    st.write(f"**📈 統計深探：** 偏度 `{m_sk:.2f}` | 峰度 `{m_ku:.2f}`")

    # --- B. 核心提示詞 (預先生成，確保後方按鈕抓得到) ---
    tail_df = df[df['pre_month'] > 5]
    tail_list = tail_df[['stock_id', 'stock_name', 'pre_month', 'remark']].head(100).to_markdown(index=False)
    
    final_prompt = (
        f"請解讀台股 {target_year} 年營收爆發行為。樣本 {total_n} 檔。\n"
        f"【指標數據】：平均報酬 T-1月 {m_mean:.2f}%, 中位數 {m_med:.2f}%, 偏度 {m_sk:.2f}。\n"
        f"【右尾先行名單 (T-1月 > 5%)】：共 {len(tail_df)} 檔，平均漲幅 {tail_df['pre_month'].mean():.2f}%。\n"
        f"名單如下：\n{tail_list}\n"
        f"請分析是否存在『資訊先行』現象，並給予策略建議。"
    )

    # --- C. 原始明細與提示詞顯示 ---
    st.subheader("🏆 原始數據明細與分析指令")
    if st.checkbox("🔍 顯示 AI 深度診斷提示詞 (包含右尾點名名單)"):
        st.code(final_prompt, language="text")
    
    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})

    # --- D. AI 平台按鈕 (絕不刪除) ---
    st.subheader("🚀 送往 AI 交叉驗證")
    encoded_p = urllib.parse.quote(final_prompt)
    b1, b2, b3, b4 = st.columns(4)
    b1.link_button("🔥 ChatGPT (全自動帶入)", f"https://chatgpt.com/?q={encoded_p}")
    b2.link_button("Ⓜ️ 通義千問 Qwen", "https://tongyi.aliyun.com/")
    b3.link_button("♊ Gemini 官網", "https://gemini.google.com/app")
    b4.link_button("🌐 Claude.ai", "https://claude.ai/")

    # --- E. 五張圖表 (不刪除圖表) ---
    st.write("---")
    st.subheader("📊 公告前後報酬分佈 (紅:平均, 藍:中位)")
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "公告前 30 天走勢。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (預跑區)", "#ff4b4b", "公告前 7 天走勢。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00", "公告當週反應。")
    create_big_hist(df, "after_week_1", "❸ T+1 周 (慣性區)", "#32cd32", "公告後一週追漲動能。")
    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢結局)", "#1e90ff", "一個月後的波段結局。")

    # --- F. 內建 AI 診斷 ---
    st.divider()
    if st.button("🔒 啟動內建 Gemini 專家診斷"):
        st.session_state.run_ai_55 = True

    if st.session_state.get("run_ai_55", False):
        with st.form("ai_form"):
            pw = st.text_input("密碼：", type="password")
            if st.form_submit_button("執行"):
                if pw == st.secrets["AI_ASK_PASSWORD"]:
                    if AI_AVAILABLE:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        model = genai.GenerativeModel('gemini-1.5-flash')
                        with st.spinner("AI 正在解析數據..."):
                            res = model.generate_content(final_prompt)
                            st.info("### 🤖 內建專家報告")
                            st.markdown(res.text)
                    else: st.error("環境套件缺失")
                else: st.error("密碼錯誤")
else:
    st.info("💡 查無資料。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 2019-2025")
