import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="公告行為研究 | StockRevenueLab", layout="wide")

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
        st.error("❌ 資料庫連線失敗，請檢查 Secrets")
        st.stop()

# ========== 3. 核心標題 ==========
st.title("🕵️ 營收公告行為研究室")
st.markdown("""
### 揭開「利多出盡」與「主力預跑」的真相
我們以每月 **10 號** 作為法定公告基準點，分析爆發成長股在前後四周的股價走勢。
本頁面使用 **還原股價 (Adj Close)** 進行計算。
""")

# --- 側邊欄控制 ---
with st.sidebar:
    st.header("🔬 研究設定")
    threshold = st.slider("營收爆發門檻 (YoY %)", 30, 300, 100)
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    
# --- 修正後的 SQL 邏輯 ---
@st.cache_data(ttl=3600)
def fetch_timing_impact(year, yoy_limit):
    engine = get_engine()
    minguo_year = int(year) - 1911
    
    # 修正重點：先算出週報酬(returns)，再進行分類匯總
    query = f"""
    WITH events AS (
        SELECT stock_id, report_month, yoy_pct,
               CASE 
                 WHEN RIGHT(report_month, 2) = '12' THEN (LEFT(report_month, 3)::int + 1 + 1911)::text || '-01-10'
                 ELSE (LEFT(report_month, 3)::int + 1911)::text || '-' || LPAD((RIGHT(report_month, 2)::int + 1)::text, 2, '0') || '-10'
               END::date as base_date
        FROM monthly_revenue
        WHERE yoy_pct >= {yoy_limit} AND report_month LIKE '{minguo_year}_%'
    ),
    weekly_calc AS (
        SELECT 
            w.symbol, w.date, w.w_close,
            (w.w_close - LAG(w.w_close) OVER (PARTITION BY w.symbol ORDER BY w.date)) / 
            NULLIF(LAG(w.w_close) OVER (PARTITION BY w.symbol ORDER BY w.date), 0) * 100 as weekly_ret
        FROM stock_weekly_k w
    ),
    event_returns AS (
        SELECT 
            e.stock_id, e.report_month,
            AVG(CASE WHEN c.date >= e.base_date - interval '9 days' AND c.date <= e.base_date - interval '3 days' THEN c.weekly_ret END) as pre_week,
            AVG(CASE WHEN c.date > e.base_date - interval '3 days' AND c.date <= e.base_date + interval '4 days' THEN c.weekly_ret END) as announce_week,
            AVG(CASE WHEN c.date > e.base_date + interval '4 days' AND c.date <= e.base_date + interval '11 days' THEN c.weekly_ret END) as after_week_1,
            AVG(CASE WHEN c.date > e.base_date + interval '11 days' AND c.date <= e.base_date + interval '30 days' THEN c.weekly_ret END) as after_month
        FROM events e
        JOIN weekly_calc c ON e.stock_id = SPLIT_PART(c.symbol, '.', 1)
        GROUP BY e.stock_id, e.report_month
    )
    SELECT 
        COUNT(*) as "總樣本數",
        ROUND(AVG(pre_week)::numeric, 2) as "公告前一周漲幅%",
        ROUND(AVG(announce_week)::numeric, 2) as "公告當周漲幅%",
        ROUND(AVG(after_week_1)::numeric, 2) as "公告後一周漲幅%",
        ROUND(AVG(after_month)::numeric, 2) as "公告後一個月漲幅%",
        ROUND((COUNT(*) FILTER (WHERE pre_week > 2) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 1) as "主力預跑率%",
        ROUND((COUNT(*) FILTER (WHERE announce_week < -2) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 1) as "利多出盡跌價率%"
    FROM event_returns
    WHERE pre_week IS NOT NULL;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

df_timing = fetch_timing_impact(target_year, threshold)

if not df_timing.empty and df_timing["總樣本數"].iloc[0] > 0:
    res = df_timing.iloc[0]
    
    c1, c2, c3 = st.columns(3)
    c1.metric("觀測樣本事件", f"{int(res['總樣本數'])} 次")
    c2.metric("平均預跑漲幅", f"{res['公告前一周漲幅%']}%")
    c3.metric("利多出盡機率", f"{res['利多出盡跌價率%']}%")

    st.write("---")
    st.subheader("📈 公告前後周報酬趨勢圖")
    
    plot_data = pd.DataFrame({
        "階段": ["公告前一周", "公告當周", "公告後一周", "公告後一個月"],
        "平均漲跌 %": [res["公告前一周漲幅%"], res["公告當周漲幅%"], res["公告後一周漲幅%"], res["公告後一個月漲幅%"]]
    })
    
    import plotly.express as px
    fig = px.bar(plot_data, x="階段", y="平均漲跌 %", color="平均漲跌 %",
                 color_continuous_scale="RdYlGn", text_auto=".2f")
    st.plotly_chart(fig, use_container_width=True)

    st.info(f"💡 **大數據洞察**：在 {target_year} 年，當營收 YoY > {threshold}% 時：")
    if res['公告前一周漲幅%'] > res['公告當周漲幅%']:
        st.warning("👉 **市場呈現『主力預跑』特徵**：公告前的漲幅大於公告後，需注意利多出盡風險。")
    else:
        st.success("👉 **市場呈現『趨勢延續』特測**：公告後仍有動能。")

else:
    st.warning("⚠️ 尚未偵測到符合條件的數據。請確認周 K 資料庫 (stock_weekly_k) 是否已匯入清洗後的數據。")
