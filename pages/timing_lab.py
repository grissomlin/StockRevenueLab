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
st.set_page_config(page_title="公告行為研究室 5.1 | 全維度診斷", layout="wide")

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
    """繪製直方圖並顯示中位數與平均線"""
    data = df[col_name].dropna()
    if data.empty: return
    
    mean_val = data.mean()
    median_val = data.median()
    
    counts, bins = np.histogram(data, bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    texts = [f"<b>{int(c)}檔</b>" for c in counts]
    
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    
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

# ========== 5. 介面與統計呈現 ==========
with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 搜尋備註", "")

st.title(f"🕵️ {target_year} 年 公告行為研究室 5.1")

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # A. 全維度看板 (補齊所有指標)
    total_n = len(df)
    
    def calc_stats(col):
        d = df[col].dropna()
        m, med = d.mean(), d.median()
        sk, ku = skew(d), kurtosis(d)
        cv = d.std() / abs(m) if m != 0 else 0
        return m, med, sk, ku, cv

    m_m, m_d, m_s, m_k, m_c = calc_stats('pre_month')
    w_m, w_d, w_s, w_k, w_c = calc_stats('pre_week')
    a_m, a_d, a_s, a_k, a_c = calc_stats('announce_week')
    aw_m, aw_d, aw_s, aw_k, aw_c = calc_stats('after_week_1')
    f_m, f_d, f_s, f_k, f_c = calc_stats('after_month')

    st.subheader("🔬 行為統計看板 (平均/中位數對照)")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本總數", f"{total_n} 檔")
    c2.metric("T-1月 (大戶區)", f"{m_m:.1f}%", f"中位: {m_d:.1f}%")
    c3.metric("T-1周 (預跑區)", f"{w_m:.1f}%", f"中位: {w_d:.1f}%")
    c4.metric("T周公告 (反應區)", f"{a_m:.1f}%", f"中位: {a_d:.1f}%")
    c5.metric("T+1月 (結局區)", f"{f_m:.1f}%", f"中位: {f_d:.1f}%")
    
    st.markdown(f"""
    **🔍 統計深探：**
    * **T-1月**：偏度 `{m_s:.2f}` (右偏代表大戶佈局) | 峰度 `{m_k:.2f}` (厚尾代表極端飆股) | 變異係數 `{m_c:.2f}`
    * **T周公告**：偏度 `{a_s:.2f}` | 峰度 `{a_k:.2f}`
    """)
    st.write("---")

    # B. 原始明細與複製
    st.subheader("🏆 原始數據明細")
    col_dl, col_copy = st.columns([1, 4])
    with col_dl:
        st.download_button("📋 下載 CSV", df.to_csv(index=False).encode('utf-8'), f"{target_year}_data.csv")
    with col_copy:
        if st.checkbox("🔍 產生 AI 右尾強勢股診斷指令"):
            # 選取 T-1月 漲幅 > 5% 的個股
            tail_df = df[df['pre_month'] > 5]
            tail_list = tail_df[['stock_id', 'stock_name', 'pre_month', 'remark']].head(50).to_markdown(index=False)
            rt_prompt = (
                f"分析 {target_year} 年營收爆發股。總數 {total_n} 檔。\n"
                f"【統計證據】：T-1月平均 {m_m:.1f}%, 中位數 {m_d:.1f}%。偏度 {m_s:.2f} 顯示極大右尾偏向。\n"
                f"【右尾強勢標的 (T-1月 > 5%)】：共 {len(tail_df)} 檔，平均漲幅 {tail_df['pre_month'].mean():.1f}%。\n"
                f"名單如下：\n{tail_list}\n"
                f"請分析這群標的是否具備『資訊先行』特徵，並建議如何識別此類標的。"
            )
            st.code(rt_prompt, language="text")

    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, height=400, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})
    st.write("---")

    # C. 完整五張分佈圖 (Mean/Median 並列)
    st.subheader("📊 公告行為各階段分佈趨勢")
    
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "公告前一個月。若平均值在紅線右方且遠離藍線，即為大戶提早卡位證據。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b", "公告前一周。用於捕捉消息洩漏後的最後衝刺。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周：市場反應)", "#ffaa00", "營收釋出。檢驗是驚喜追價還是利多出盡。")
    create_big_hist(df, "after_week_1", "❸ T+1 周 (公告後續：慣性區)", "#32cd32", "公告後續追漲意願。")
    create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢結局)", "#1e90ff", "中位數若低於 0 代表大多數股票利多出盡後會回吐。")

    # D. 內建 AI 診斷 (整合偏度與右尾)
    st.divider()
    if st.button("🔒 啟動內建 Gemini 專家診斷"):
        st.session_state.run_final_ai = True

    if st.session_state.get("run_final_ai", False):
        with st.form("ai_form"):
            pw = st.text_input("密碼：", type="password")
            if st.form_submit_button("執行分析"):
                if pw == st.secrets["AI_ASK_PASSWORD"]:
                    if AI_AVAILABLE:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        # 確保模型名稱正確
                        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                        target_model = next((m for m in models if "gemini-1.5-flash" in m), models[0])
                        model = genai.GenerativeModel(target_model)
                        
                        # 濃縮提示詞
                        final_prompt = (
                            f"分析台股 {target_year} 年。樣本 {total_n}。\n"
                            f"T-1月平均 {m_m:.1f}%, 中位 {m_d:.1f}%, 偏度 {m_s:.2f}。\n"
                            f"T周公告平均 {a_m:.1f}%, T+1月結局中位 {f_d:.1f}%。\n"
                            f"請針對這些『資訊不對稱』與『右尾效應』指標，給予投資策略建議。"
                        )
                        
                        with st.spinner("AI 正在解析資訊先行程度..."):
                            res = model.generate_content(final_prompt)
                            st.info("### 🤖 內建專家報告")
                            st.markdown(res.text)
                    else: st.error("套件缺少")
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無樣本。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 2019-2025")
