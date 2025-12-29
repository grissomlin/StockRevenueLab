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

# ========== 3. 繪圖輔助函數 ==========
def create_big_hist(df, col_name, title, color):
    if df[col_name].dropna().empty: return go.Figure()
    counts, bins = np.histogram(df[col_name].dropna(), bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    total = len(df)
    texts = [f"<b>{int(c)}檔</b><br>{(c/total*100):.1f}%" if c > 0 else "" for c in counts]
    
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black")
    fig.update_layout(title=dict(text=title, font=dict(size=22)), height=400, margin=dict(t=80, b=40))
    return fig

# ========== 4. 核心標題 ==========
st.title("🕵️ 營收公告行為研究室 3.0")

with st.sidebar:
    st.header("🔬 策略參數設定")
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"設定 {study_metric} 爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 關鍵字搜尋", "")

# --- 核心 SQL ---
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
    # A. 看板與數據
    total_n = len(df)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本數", total_n)
    c2.metric("T-1月勝率", f"{(df['pre_month']>0).sum()/total_n*100:.1f}%")
    c3.metric("T-1月平均", f"{df['pre_month'].mean():.2f}%")
    c4.metric("T-1周平均", f"{df['pre_week'].mean():.2f}%")
    c5.metric("公告後延續率", f"{(df['after_month']>0).sum()/total_n*100:.1f}%")

    st.write("---")
    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    display_df = df.rename(columns={
        "stock_id": "代號", "stock_name": "名稱", "report_month": "月份", "growth_val": f"{study_metric}%",
        "pre_month": "T-1月%", "pre_week": "T-1周%", "announce_week": "T周%", "after_week_1": "T+1周%", "after_month": "一個月後%", "remark": "備註"
    })

    st.dataframe(
        display_df.style.background_gradient(subset=["T-1月%", "T-1周%", "T周%", "T+1周%", "一個月後%"], cmap="RdYlGn"),
        use_container_width=True, height=400,
        column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗"), "備註": st.column_config.TextColumn(width="large")},
        hide_index=True
    )

    # B. 分佈圖
    st.subheader("📊 階段報酬分佈趨勢")
    st.plotly_chart(create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2"), use_container_width=True)
    st.plotly_chart(create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b"), use_container_width=True)
    st.plotly_chart(create_big_hist(df, "after_month", "❹ 公告後一個月 (趨勢區)", "#1e90ff"), use_container_width=True)

    # C. AI 助手與密碼保護按鈕
    st.divider()
    st.subheader("🤖 AI 投資行為診斷")
    
    prompt_text = (
        f"請解讀 {target_year} 年營收爆發後的股價行為。\n"
        f"數據顯示：T-1月平均報酬 {df['pre_month'].mean():.2f}%，T-1周預跑 {df['pre_week'].mean():.2f}%。\n"
        f"請問這種『先行程度』是否代表市場資訊不對稱？後續一個月的股價慣性通常如何？"
    )

    col_p, col_l = st.columns([2, 1])
    with col_p:
        st.code(prompt_text, language="text")
    
    with col_l:
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("🔥 ChatGPT (自動帶入)", f"https://chatgpt.com/?q={encoded_p}")
        st.link_button("♊ 開啟 Gemini (需貼上)", "https://gemini.google.com/app")
        st.link_button("🌐 開啟 Claude (需貼上)", "https://claude.ai/")
        
        # 密碼保護按鈕
        if st.button("🔒 直接詢問 AI (需權限)"):
            st.session_state.show_pw_dialog = True

    # 處理密碼彈窗邏輯
    if st.session_state.get("show_pw_dialog", False):
        with st.form("pw_form"):
            user_pw = st.text_input("請輸入研究員密碼：", type="password")
            submitted = st.form_submit_button("驗證並開啟對話")
            if submitted:
                if user_pw == st.secrets["AI_ASK_PASSWORD"]:
                    st.success("密碼正確！正在前往分析頁面...")
                    st.markdown(f'<meta http-equiv="refresh" content="0;url=https://chatgpt.com/?q={encoded_p}">', unsafe_allow_html=True)
                    st.session_state.show_pw_dialog = False
                else:
                    st.error("密碼錯誤，請重新輸入。")

else:
    st.info("💡 找不到符合條件的公司。")
