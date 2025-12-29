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

# ========== 3. 數據核心運算函數 ==========
def get_distribution_text(df, col_name):
    """將分佈數據轉換為文字，方便餵給 AI"""
    data = df[col_name].dropna()
    if data.empty: return "無數據"
    counts, bins = np.histogram(data, bins=10) # 為了節省 Token，分 10 個區間
    total = len(data)
    dist_str = ""
    for i in range(len(counts)):
        if counts[i] > 0:
            dist_str += f"- [{bins[i]:.1f}% ~ {bins[i+1]:.1f}%]: {counts[i]}檔 ({(counts[i]/total*100):.1f}%)\n"
    return dist_str

def create_big_hist(df, col_name, title, color, desc):
    data = df[col_name].dropna()
    if data.empty: return
    counts, bins = np.histogram(data, bins=20)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    total = len(data)
    texts = [f"<b>{int(c)}檔</b>" for c in counts]
    fig = go.Figure(data=[go.Bar(x=bin_centers, y=counts, text=texts, textposition='outside', marker_color=color)])
    fig.add_vline(x=0, line_dash="dash", line_color="black")
    fig.update_layout(title=dict(text=title, font=dict(size=20)), height=350, margin=dict(t=50, b=40))
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

# ========== 5. 主介面邏輯 ==========
st.title("🕵️ 營收公告行為研究室 3.4 旗艦版")

with st.sidebar:
    st.header("🔬 參數設定")
    target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider("門檻", 30, 300, 100)
    search_key = st.text_input("關鍵字", "")

df = fetch_timing_data(target_year, study_metric, threshold, search_key)

if not df.empty:
    # A. 數據看板
    total_n = len(df)
    m_avg = round(df['pre_month'].mean(), 2)
    w_avg = round(df['pre_week'].mean(), 2)
    a_avg = round(df['announce_week'].mean(), 2)
    f_avg = round(df['after_month'].mean(), 2)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("樣本數", total_n)
    c2.metric("T-1月平均", f"{m_avg}%")
    c3.metric("T-1周平均", f"{w_avg}%")
    c4.metric("T周(公告)平均", f"{a_avg}%")
    c5.metric("T+1月平均", f"{f_avg}%")

    st.write("---")
    
    # B. 原始明細
    st.subheader(f"🏆 {target_year} 年 數據明細清單")
    df['連結'] = df['stock_id'].apply(lambda x: f"https://www.wantgoo.com/stock/{x}/technical-chart")
    st.dataframe(df, use_container_width=True, column_config={"連結": st.column_config.LinkColumn("圖表", display_text="🔗")})

    st.write("---")

    # C. 五階段分佈圖 (與文字解讀)
    create_big_hist(df, "pre_month", "⓪ T-1 月 (大戶佈局區)", "#8a2be2", "公告前一個月走勢，檢驗大資金是否有超前佈局痕跡。")
    create_big_hist(df, "pre_week", "❶ T-1 周 (短線預跑區)", "#ff4b4b", "公告前一週走勢，檢驗短線資訊領先者是否進行預跑。")
    create_big_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00", "公告那一週表現。正值代表驚喜，負值代表利多出盡。")
    create_big_hist(df, "after_week_1", "❸ T+1 周 (慣性區)", "#32cd32", "公告後續追漲動能，檢驗市場共識強度。")
    create_big_hist(df, "after_month", "❹ T+1 月 (趨勢區)", "#1e90ff", "一個月後的波段結局，檢驗爆發是否能啟動長波段。")

    # D. AI 指令區 (關鍵：全數據分佈帶入)
    st.divider()
    st.subheader("🤖 AI 全維度分佈診斷")

    # 自動生成五階段分佈明細文字
    dist_reports = {
        "T-1月": get_distribution_text(df, "pre_month"),
        "T-1周": get_distribution_text(df, "pre_week"),
        "T周": get_distribution_text(df, "announce_week"),
        "T+1周": get_distribution_text(df, "after_week_1"),
        "T+1月": get_distribution_text(df, "after_month")
    }

    prompt_text = (
        f"請擔任量化分析師，解讀台股 {target_year} 年營收爆發後的五階段股價行為。\n"
        f"【全樣本統計】：{total_n} 檔。平均報酬：T-1月 {m_avg}%，T-1周 {w_avg}%，T周 {a_avg}%，T+1月 {f_avg}%。\n\n"
        f"【五階段詳細分佈數據】：\n"
        f"1. T-1月(佈局區)分佈：\n{dist_reports['T-1月']}\n"
        f"2. T-1周(預跑區)分佈：\n{dist_reports['T-1周']}\n"
        f"3. T周(公告區)分佈：\n{dist_reports['T周']}\n"
        f"4. T+1周(慣性區)分佈：\n{dist_reports['T+1周']}\n"
        f"5. T+1月(波段區)分佈：\n{dist_reports['T+1月']}\n\n"
        f"請分析：分佈數據中是否出現『少數權值股帶動平均』還是『普漲行情』？在哪個階段進場最能避開利多出盡的風險？"
    )

    cp, cl = st.columns([2, 1])
    with cp: st.code(prompt_text, language="text")
    with cl:
        encoded_p = urllib.parse.quote(prompt_text)
        st.link_button("♊ 直接詢問 Gemini (推薦數據分析)", "https://gemini.google.com/app")
        st.link_button("🔥 開啟 ChatGPT (全數據帶入)", f"https://chatgpt.com/?q={encoded_p}")
        if st.button("🔒 密碼保護：解鎖直接提問"):
            st.session_state.unlock = True

    if st.session_state.get("unlock", False):
        with st.form("pw"):
            p = st.text_input("密碼", type="password")
            if st.form_submit_button("驗證"):
                if p == st.secrets["AI_ASK_PASSWORD"]:
                    st.markdown(f'<meta http-equiv="refresh" content="0;url=https://chatgpt.com/?q={encoded_p}">', unsafe_allow_html=True)
                else: st.error("密碼錯誤")

else:
    st.info("💡 查無符合樣本。")
