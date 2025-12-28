import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

st.set_page_config(page_title="公告行為研究 | StockRevenueLab", layout="wide")

@st.cache_resource
def get_engine():
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    PROJECT_REF = st.secrets["PROJECT_REF"]
    POOLER_HOST = st.secrets["POOLER_HOST"]
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
    return create_engine(connection_string)

st.title("🕵️ 營收公告行為研究室")
st.markdown("""
### 揭開「利多出盡」與「主力預跑」的真相
我們以每月 **10 號** 作為法定公告基準點，分析爆發成長股在前後四周的股價走勢。
""")

# --- 側邊欄控制 ---
with st.sidebar:
    st.header("🔬 研究設定")
    threshold = st.slider("營收爆發門檻 (YoY %)", 30, 300, 100)
    target_year = st.selectbox("分析年度", [str(y) for y in range(2024, 2019, -1)])
    
# --- 核心 SQL：區間報酬對齊 ---
@st.cache_data(ttl=3600)
def fetch_timing_impact(year, yoy_limit):
    engine = get_engine()
    minguo_year = int(year) - 1911
    
    # 邏輯說明：
    # 1. 找出當年所有符合 YoY 的事件
    # 2. 定義公告月 (M+1) 的 10 號
    # 3. 透過日期區間關聯周 K 線
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
    weekly_returns AS (
        SELECT 
            e.stock_id, e.report_month, e.base_date,
            -- Week T-1: 公告前夕 (1號~7號)
            AVG(CASE WHEN w.date >= e.base_date - interval '9 days' AND w.date <= e.base_date - interval '3 days' 
                THEN (w.w_close - w.w_open)/w.w_open END) * 100 as pre_week,
            -- Week T: 公告當周 (8號~14號)
            AVG(CASE WHEN w.date > e.base_date - interval '3 days' AND w.date <= e.base_date + interval '4 days' 
                THEN (w.w_close - w.w_open)/w.w_open END) * 100 as announce_week,
            -- Week T+1: 公告後一周
            AVG(CASE WHEN w.date > e.base_date + interval '4 days' AND w.date <= e.base_date + interval '11 days' 
                THEN (w.w_close - w.w_open)/w.w_open END) * 100 as after_week_1,
            -- Week T+2~4: 公告後一個月
            AVG(CASE WHEN w.date > e.base_date + interval '11 days' AND w.date <= e.base_date + interval '30 days' 
                THEN (w.w_close - w.w_open)/w.w_open END) * 100 as after_month
        FROM events e
        JOIN stock_weekly_k w ON e.stock_id = SPLIT_PART(w.symbol, '.', 1)
        GROUP BY e.stock_id, e.report_month, e.base_date
    )
    SELECT 
        COUNT(*) as "總樣本數",
        ROUND(AVG(pre_week)::numeric, 2) as "公告前一周漲幅%",
        ROUND(AVG(announce_week)::numeric, 2) as "公告當周漲幅%",
        ROUND(AVG(after_week_1)::numeric, 2) as "公告後一周漲幅%",
        ROUND(AVG(after_month)::numeric, 2) as "公告後一個月漲幅%",
        ROUND((COUNT(*) FILTER (WHERE pre_week > 2) * 100.0 / COUNT(*))::numeric, 1) as "主力預跑率%",
        ROUND((COUNT(*) FILTER (WHERE announce_week < -2) * 100.0 / COUNT(*))::numeric, 1) as "利多出盡跌價率%"
    FROM weekly_returns
    WHERE pre_week IS NOT NULL;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

df_timing = fetch_timing_impact(target_year, threshold)

if not df_timing.empty and df_timing["總樣本數"].iloc[0] > 0:
    # --- 數據儀表板 ---
    res = df_timing.iloc[0]
    
    col1, col2, col3 = st.columns(3)
    col1.metric("觀測樣本事件", f"{int(res['總樣本數'])} 次")
    col2.metric("平均預跑漲幅", f"{res['公告前一周漲幅%']}%")
    col3.metric("利多出盡機率", f"{res['利利多出盡跌價率%']}%")

    st.write("---")
    st.subheader("📈 公告前後周報酬趨勢圖")
    
    # 準備繪圖數據
    plot_data = pd.DataFrame({
        "階段": ["公告前一周", "公告當周", "公告後一周", "公告後一個月"],
        "平均漲跌 %": [res["公告前一周漲幅%"], res["公告當周漲幅%"], res["公告後一周漲幅%"], res["公告後一個月漲幅%"]]
    })
    st.bar_chart(data=plot_data, x="階段", y="平均漲跌 %")

    # --- 專業分析建議 ---
    st.success(f"💡 **大數據洞察**：在 {target_year} 年，當營收 YoY > {threshold}% 時：")
    if res['公告前一周漲幅%'] > res['公告當周漲幅%']:
        st.write("👉 **市場呈現『早知道』特徵**：公告前的漲幅大於公告後，建議不要在 10 號之後才追高。")
    else:
        st.write("👉 **市場呈現『追價型』特徵**：公告後仍有不錯的動能，分批進場勝率較高。")

else:
    st.info("尚未匯入周線數據 `stock_weekly_k` 或該年度無符合條件的爆發事件。")
