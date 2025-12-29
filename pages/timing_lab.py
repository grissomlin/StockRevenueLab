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

# ========== 3. 繪圖輔助函數 (含科學說明) ==========
def create_big_hist(df, col_name, title, color, desc):
    if df[col_name].dropna().empty: return go.Figure()
    counts, bins = np.histogram(df[col_name].dropna(), bins=25)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    total = len(df)
    texts = [f"<b>{int(c)}檔</b><br>{(c/total*100):.1f}%" if c > 0 else "" for c in counts]
    
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black")
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=350, margin=dict(t=50, b=40))
    
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 **階段分析：** {desc}")
    st.markdown("---")

# ========== 4. 數據抓取邏輯 (鎖定五階段) ==========
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
    SELECT * FROM final_detail WHERE pre_week IS NOT NULL;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 5. 主頁面執行 ==========
st.title("🕵️ 營收公告行為研究室 3.1 Pro")

with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"{study_metric} 門檻", 30, 300, 100)
    search_keyword = st.text_input("關鍵字搜尋", "")

df = fetch_timing_data(target_year, study_metric, threshold, search_keyword)

if not df.empty:
    # A. 數據看板 (兩位小數)
    total_n = len(df)
    stats = {
        "T_minus_1_month": round(df['pre_month'].mean(), 2),
        "T_minus_1_week": round(df['pre_week'].mean(), 2),
        "T_week": round(df['announce_week'].mean(), 2),
        "T_plus_1_week": round(df['after_week_1'].mean(), 2),
        "T_plus_1_month": round(df['after_month'].mean(), 2)
    }

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本數", total_n)
    c2.metric("T-1月平均", f"{stats['T_minus_1_month']}%")
    c3.metric("T-1周平均", f"{stats['T_minus_1_week']}%")
    c4.metric("T周(公告)平均", f"{stats['T_week']}%")
    c5.metric("T+1月平均", f"{stats['T_plus_1_month']}%")

    st.write("---")
    
    # B. 分佈圖趨勢
    st.subheader("📊 五階段報酬率分佈趨勢")
    
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", 
                    "觀察公告前 30 天是否有異常買盤。若此區間正值比例極高，代表大資金早已獲悉營收利多並提前卡位。")
    
    create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b", 
                    "公告前一週的表現。若此區間突然噴發，通常是短線客或業內資訊領先者在進行『預跑』。")
    
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周：市場反應)", "#ffaa00", 
                    "營收正式公告那一週的股價。若此處出現長陰線但營收極好，即為標準的『利多出盡』。")
    
    create_big_hist(df, "after_week_1", "❸ T+1 周 (公告後續：慣性區)", "#32cd32", 
                    "利多公佈後的追加買盤。若此區間能維持漲勢，代表營收爆發具有市場共識，非一日行情。")
    
    create_big_hist(df, "after_month", "❹ T+1 月 (一個月後：趨勢區)", "#1e90ff", 
                    "營收公佈一個月後的表現。用於判斷這次爆發是否啟動了長期的波段主升段。")

    # C. AI 指令區 (含全階段數據)
    st.divider()
    st.subheader("🤖 AI 全階段行為診斷")
    
    prompt_text = (
        f"請擔任量化分析師，解讀台股 {target_year} 年營收爆發後的五階段股價行為。\n"
        f"【全階段平均報酬數據】：\n"
        f"1. 公告前一個月 (T-1 month)：{stats['T_minus_1_month']}%\n"
        f"2. 公告前一週 (T-1 week)：{stats['T_minus_1_week']}%\n"
        f"3. 公告當週 (T week)：{stats['T_week']}%\n"
        f"4. 公告後一週 (T+1 week)：{stats['T_plus_1_week']}%\n"
        f"5. 公告後一個月 (T+1 month)：{stats['T_plus_1_month']}%\n\n"
        f"請分析：這組數據顯示出『資訊領先』還是『落後反應』？投資人應該在五個階段中的哪一點切入勝率最高？"
    )

    cp, cl = st.columns([2, 1])
    with cp:
        st.code(prompt_text, language="text")
    with cl:
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("🔥 ChatGPT (五階段數據帶入)", f"https://chatgpt.com/?q={encoded_p}")
        st.link_button("♊ 開啟 Gemini (需手動貼上)", "https://gemini.google.com/app")
        
        if st.button("🔒 研究員密碼對話 (保護模式)"):
            st.session_state.ask_pw = True

    if st.session_state.get("ask_pw", False):
        with st.form("pw_form"):
            user_pw = st.text_input("輸入密碼：", type="password")
            if st.form_submit_button("驗證"):
                if user_pw == st.secrets["AI_ASK_PASSWORD"]:
                    st.success("通過！正在跳轉...")
                    st.markdown(f'<meta http-equiv="refresh" content="0;url=https://chatgpt.com/?q={encoded_p}">', unsafe_allow_html=True)
                else:
                    st.error("密碼錯誤")

else:
    st.info("💡 查無符合條件之樣本。")
