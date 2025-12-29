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

# ========== 3. 數據輔助函數 ==========
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
    """繪製直方圖並顯示中位數與平均線"""
    data = df[col_name].dropna()
    if data.empty: return
    
    mean_val = data.mean()
    median_val = data.median()
    
    counts, bins = np.histogram(data, bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    texts = [f"<b>{int(c)}檔</b>" for c in counts]
    
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    
    # 加入垂直參考線
    fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=1)
    fig.add_vline(x=mean_val, line_color="red", line_width=2, annotation_text=f"平均 {mean_val:.1f}%")
    fig.add_vline(x=median_val, line_color="blue", line_width=2, annotation_text=f"中位 {median_val:.1f}%", annotation_position="bottom right")
    
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=400, margin=dict(t=80, b=40))
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **科學解讀：** {desc}")
    st.markdown("---")

# ========== 4. 核心 SQL 邏輯 (初次爆發) ==========
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

st.title(f"🕵️ {target_year} 年 公告行為研究室 4.2")

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # A. 數據看板 (Mean vs Median)
    total_n = len(df)
    
    def get_stats(col):
        return round(df[col].mean(), 2), round(df[col].median(), 2)

    m_mean, m_med = get_stats('pre_month')
    w_mean, w_med = get_stats('pre_week')
    a_mean, a_med = get_stats('announce_week')
    f_mean, f_med = get_stats('after_month')

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本總數", f"{total_n} 檔")
    c2.metric("T-1月(平均/中位)", f"{m_mean}%", f"中位: {m_med}%")
    c3.metric("T-1周(平均/中位)", f"{w_mean}%", f"中位: {w_med}%")
    c4.metric("T周公告(平均/中位)", f"{a_mean}%", f"中位: {a_med}%")
    c5.metric("T+1月波段(平均/中位)", f"{f_mean}%", f"中位: {f_med}%")
    st.write("---")
    
    # B. 原始明細清單 (含複製功能)
    st.subheader("🏆 原始數據明細")
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        st.download_button(label="📋 下載明細 CSV", data=df.to_csv(index=False).encode('utf-8'), file_name=f'stock_{target_year}.csv')
    with col_btn2:
        if st.checkbox("🔍 產生 AI 全量複製指令 (Markdown 表格)"):
            # 只取關鍵欄位以防字數過多
            copy_data = df[['stock_id', 'stock_name', 'growth_val', 'pre_month', 'pre_week', 'after_month', 'remark']].head(500).to_markdown(index=False)
            st.code(f"請針對以下 2024 年營收爆發股數據進行診斷，分析其 T-1 階段的『右尾(Outliers)』分佈與產業備註，判斷是否有資訊先行跡象：\n\n{copy_data}", language="text")
            st.caption("提示：為確保 AI 讀取，此處僅列出前 500 筆。")

    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})
    st.write("---")

    # C. 完整五張分佈圖 (Mean/Median 並列)
    st.subheader("📊 階段報酬分佈與偏度分析")
    
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", 
                    "若平均值顯著大於中位數，代表大資金早已進場『拉抬少數權值股』。")
    
    create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b", 
                    "若中位數仍趨近於 0 但平均值為正，代表只有極少數業內資訊領先者在偷跑。")
    
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周：市場反應)", "#ffaa00", 
                    "營收正式釋出後。若平均與中位線重合，代表利多已成為市場共識。")
    
    create_big_hist(df, "after_week_1", "❸ T+1 周 (公告後續：慣性區)", "#32cd32", 
                    "利多公佈後的追價動能。")

    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢結局)", "#1e90ff", 
                    "波段收尾。若中位數為負代表大多數爆發股最終都會回吐，只有少數強者恆強。")

    # D. AI 診斷 (引入偏度診斷)
    st.divider()
    st.subheader("🤖 AI 投資行為深度診斷")
    dist_txt = f"T-1月分佈: {get_ai_summary_dist(df, 'pre_month')}\nT+1月分佈: {get_ai_summary_dist(df, 'after_month')}"
    prompt_text = (
        f"分析台股 {target_year} 年營收爆發行為。樣本數 {total_n}。\n"
        f"【數據偏度分析】：\n"
        f"- T-1月：平均 {m_mean}%, 中位數 {m_med}% (差值: {round(m_mean - m_med, 2)}%)\n"
        f"- T-1周：平均 {w_mean}%, 中位數 {w_med}% (差值: {round(w_mean - w_med, 2)}%)\n"
        f"- T+1月：中位數 {f_med}%\n\n"
        f"【分佈摘要】：\n{dist_txt}\n\n"
        f"請解讀：差值代表的『右尾效應』。針對此年度，主力是否在營收爆發前一個月即有『資訊不對稱』的集中操作行為？"
    )

    cp, cl = st.columns([2, 1])
    with cp: st.code(prompt_text, language="text")
    with cl:
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("🔥 ChatGPT (全自動帶入)", f"https://chatgpt.com/?q={encoded_p}")
        if st.button("🔒 啟動內建 Gemini 專家診斷"):
            st.session_state.run_ai_42 = True

    if st.session_state.get("run_ai_42", False):
        with st.form("ai_form_final"):
            pw = st.text_input("研究員密碼：", type="password")
            if st.form_submit_button("執行診斷"):
                if pw == st.secrets["AI_ASK_PASSWORD"]:
                    if AI_AVAILABLE:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        # 自動尋找可用模型
                        all_m = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                        target_m = next((m for m in all_m if "gemini-1.5-flash" in m), all_m[0])
                        model = genai.GenerativeModel(target_m)
                        with st.spinner("AI 正在解析資訊不對稱痕跡..."):
                            res = model.generate_content(prompt_text)
                            st.info("### 🤖 內建專家報告")
                            st.markdown(res.text)
                    else: st.error("環境套件缺失")
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無符合條件之樣本。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 數據週期：2019-2025")
