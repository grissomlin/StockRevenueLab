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
st.set_page_config(page_title="公告行為研究室 5.2 | 全維度診斷", layout="wide")

if not SCIPY_AVAILABLE:
    st.warning("⚠️ 偵測到環境缺少 `scipy`，偏度與峰度功能暫時失效。請在 requirements.txt 中加入 scipy。")

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
        st.error("❌ 資料庫連線失敗，請檢查 Streamlit Secrets")
        st.stop()

# ========== 3. 數據輔助函數 ==========
def calc_advanced_stats(data):
    """計算平均、中位、偏度、峰度與變異係數"""
    if data.empty: return 0, 0, 0, 0, 0
    m = data.mean()
    med = data.median()
    sk = skew(data) if SCIPY_AVAILABLE else 0
    ku = kurtosis(data) if SCIPY_AVAILABLE else 0
    cv = data.std() / abs(m) if m != 0 else 0
    return round(m, 2), round(med, 2), round(sk, 2), round(ku, 2), round(cv, 2)

def get_ai_summary_dist(df, col_name):
    data = df[col_name].dropna()
    if data.empty: return "無數據"
    total = len(data)
    bins = [-float('inf'), -5, -1, 1, 5, float('inf')]
    labels = ["大跌(<-5%)", "小跌", "持平", "小漲", "大漲(>5%)"]
    counts, _ = np.histogram(data, bins=bins)
    return " / ".join([f"{l}:{int(c)}檔({(c/total*100):.1f}%)" for l, c in zip(labels, counts) if c > 0])

def create_big_hist(df, col_name, title, color, desc):
    """繪製直方圖並標註平均與中位數"""
    data = df[col_name].dropna()
    if data.empty: return
    
    m, med, _, _, _ = calc_advanced_stats(data)
    counts, bins = np.histogram(data, bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=[f"{int(c)}" for c in counts], textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=1)
    fig.add_vline(x=m, line_color="red", line_width=2, annotation_text=f"平均 {m}%")
    fig.add_vline(x=med, line_color="blue", line_width=2, annotation_text=f"中位 {med}%", annotation_position="bottom right")
    
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=400, margin=dict(t=80, b=40))
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **解讀：** {desc}")
    st.markdown("---")

# ========== 4. 核心數據讀取 (SQL) ==========
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

# ========== 5. 介面呈現 ==========
with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("研究年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider("爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 搜尋備註", "")

st.title(f"🕵️ {target_year} 年 公告行為研究室 5.2")

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # A. 數據看板
    st.subheader("📊 核心統計指標")
    c1, c2, c3, c4, c5 = st.columns(5)
    
    m_m, m_d, m_s, m_k, m_c = calc_advanced_stats(df['pre_month'])
    w_m, w_d, _, _, _ = calc_advanced_stats(df['pre_week'])
    a_m, a_d, _, _, _ = calc_advanced_stats(df['announce_week'])
    f_m, f_d, _, _, _ = calc_advanced_stats(df['after_month'])
    
    c1.metric("樣本數", len(df))
    c2.metric("T-1月 (平均/中位)", f"{m_m}%", f"中位: {m_d}%")
    c3.metric("T-1周 (平均/中位)", f"{w_m}%", f"中位: {w_d}%")
    c4.metric("T周公告 (平均/中位)", f"{a_m}%", f"中位: {a_d}%")
    c5.metric("T+1月波段 (中位數)", f"{f_d}%")
    
    st.write(f"**📈 深度分佈指標 (T-1月)：** 偏度 `{m_s}` | 峰度 `{m_k}` | 變異係數 `{m_c}`")
    st.write("---")

    # B. 明細與複製功能
    st.subheader("🏆 原始數據明細")
    if st.checkbox("🔍 產生 AI 右尾強勢股診斷指令"):
        tail_df = df[df['pre_month'] > 5]
        tail_list = tail_df[['stock_id', 'stock_name', 'pre_month', 'remark']].head(50).to_markdown(index=False)
        rt_prompt = (
            f"分析 {target_year} 年營收爆發行為。總樣本 {len(df)} 檔。\n"
            f"【統計數據】：T-1月平均 {m_m}%，中位數 {m_d}%，偏度 {m_s}。\n"
            f"【右尾強勢個股 (漲 > 5%)】：共 {len(tail_df)} 檔，平均漲幅 {tail_df['pre_month'].mean():.1f}%。\n"
            f"名單如下：\n{tail_list}\n"
            f"請診斷這群標的是否有『資訊先行』現象，並給予投資建議。"
        )
        st.code(rt_prompt, language="text")

    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})

    # C. 完整五張分佈圖 (Mean/Median 並列)
    st.write("---")
    st.subheader("📊 階段報酬分佈趨勢")
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "公告前一個月。若平均值在紅線右方且遠離藍線，即為大戶提早卡位證據。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (預跑區)", "#ff4b4b", "公告前一周走勢。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00", "營收釋出反應。")
    create_big_hist(df, "after_week_1", "❸ T+1 周 (慣性區)", "#32cd32", "公告後續動能。")
    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢結局)", "#1e90ff", "一個月後的結果。")

    # D. 內建 AI 診斷
    st.divider()
    if st.button("🔒 啟動內建 Gemini 專家分析"):
        st.session_state.run_ai_52 = True

    if st.session_state.get("run_ai_52", False):
        with st.form("ai_form"):
            pw = st.text_input("密碼：", type="password")
            if st.form_submit_button("執行"):
                if pw == st.secrets["AI_ASK_PASSWORD"]:
                    if AI_AVAILABLE:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        model = genai.GenerativeModel('gemini-1.5-flash')
                        with st.spinner("正在進行偏度與右尾數據診斷..."):
                            res = model.generate_content(f"分析數據：T-1月平均 {m_m}%, 中位 {m_d}%, 偏度 {m_s}。請解讀市場資訊領先程度。")
                            st.info("### 🤖 內建專家診斷報告")
                            st.markdown(res.text)
                    else: st.error("環境套件缺失，無法執行分析。")
                else: st.error("密碼錯誤")
else:
    st.info("💡 查無樣本。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 2019-2025")
