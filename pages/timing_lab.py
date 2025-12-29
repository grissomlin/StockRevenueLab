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

# ========== 3. 數據濃縮函數 ==========
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

def create_big_hist(df, col_name, title, color, desc):
    data = df[col_name].dropna()
    if data.empty: return
    counts, bins = np.histogram(data, bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    texts = [f"<b>{int(c)}檔</b>" for c in counts]
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=2)
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=400, margin=dict(t=80, b=40))
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **科學解讀：** {desc}")
    st.markdown("---")

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
          AND (prev_metric < {limit} OR prev_metric IS NULL) -- 確保這是「初次」爆發
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
    st.header("🔬 策略參數設定")
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"設定 {study_metric} 爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 關鍵字搜尋", "")

st.title(f"🕵️ {target_year} 年 公告行為研究室")

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
    st.subheader("🏆 原始數據明細")
    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})
    st.write("---")

    # C. 完整分佈圖
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "公告前一個月的走勢。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b", "公告前一周的反應。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00", "公告正式釋出後的波動。")
    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢區)", "#1e90ff", "利多出盡還是主升段開端？")

    # D. AI 診斷專家系統 (加入參數詳情)
    st.divider()
    st.subheader("🤖 AI 投資行為診斷")
    
    dist_txt = (
        f"1.T-1月分佈: {get_ai_summary_dist(df, 'pre_month')}\n"
        f"2.T-1周分佈: {get_ai_summary_dist(df, 'pre_week')}\n"
        f"3.T周分佈: {get_ai_summary_dist(df, 'announce_week')}\n"
        f"4.T+1月分佈: {get_ai_summary_dist(df, 'after_month')}"
    )

    # 關鍵優化：將篩選條件寫入 Prompt
    metric_name = "年增率 (YoY)" if study_metric == "yoy_pct" else "月增率 (MoM)"
    prompt_text = (
        f"請擔任專業量化分析師，分析台股 {target_year} 年的營收公告數據。\n"
        f"【實驗參數設定】：\n"
        f"- 指標：{metric_name}\n"
        f"- 爆發門檻：設定為 {threshold}% 以上\n"
        f"- 樣本特性：僅包含『初次爆發』之個股 (即前一月未達標，本月首度衝破 {threshold}%)\n"
        f"- 樣本總數：{total_n} 檔\n\n"
        f"【全階段平均報酬】：\n"
        f"- 公告前一個月: {m_avg}% / 公告前一週: {w_avg}% / 公告當週: {a_avg}% / 公告後一個月: {f_avg}%\n\n"
        f"【分佈摘要數據】：\n{dist_txt}\n\n"
        f"請針對以上數據進行診斷：\n"
        f"1. 從 T-1 月與 T-1 週的漲幅分佈來看，是否有證據顯示『主力/內部人提早知道訊息並佈局』？(若 T-1 月平均報酬顯著為正且大漲檔數比例高，則機率極大)\n"
        f"2. 營收正式公告(T周)後，市場呈現的是『追加買盤』還是『利多出盡』？\n"
        f"3. 針對這組數據特徵，給予投資人最具期望值的進場點建議。"
    )

    col_p, col_l = st.columns([2, 1])
    with col_p:
        st.write("📋 **待分析指令 (含詳細實驗參數)**")
        st.code(prompt_text, language="text")

    with col_l:
        st.write("🚀 **外部與內建診斷**")
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("🔥 開啟 ChatGPT (網址帶入)", f"https://chatgpt.com/?q={encoded_p}")
        
        st.write("---")
        if st.button("🔒 密碼驗證：啟動內建 Gemini 診斷"):
            st.session_state.run_ai = True

    # 內建 Gemini 邏輯
    if st.session_state.get("run_ai", False):
        with st.form("ai_form"):
            user_pw = st.text_input("輸入研究員密碼：", type="password")
            if st.form_submit_button("執行分析"):
                if user_pw == st.secrets["AI_ASK_PASSWORD"]:
                    if AI_AVAILABLE:
                        try:
                            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                            models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                            target_model = next((m for m in models if "gemini-1.5-flash" in m), models[0])
                            
                            model = genai.GenerativeModel(target_model)
                            with st.spinner(f"正在分析 {total_n} 檔數據背景..."):
                                response = model.generate_content(prompt_text)
                                st.info(f"### 🤖 內建專家報告 ({target_model})")
                                st.markdown(response.text)
                        except Exception as e:
                            st.error(f"AI 調用失敗: {e}")
                    else: st.error("環境未安裝 google-generativeai")
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無符合條件之樣本。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 數據週期：2019-2025")
