import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

st.set_page_config(page_title="公告時序研究 | StockRevenueLab", layout="wide")

@st.cache_resource
def get_engine():
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    PROJECT_REF = st.secrets["PROJECT_REF"]
    POOLER_HOST = st.secrets["POOLER_HOST"]
    connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
    return create_engine(connection_string)

st.title("🕵️ 公告時序研究室：誰在早知道？")
st.markdown("""
本研究追蹤營收公告日前後的股價脈絡：
* **前一周 (T-1)**：主力是否先行進場？
* **公告周 (T)**：市場對好消息的即時反應。
* **後 1~4 周**：追價動能能否持續，還是會利多出盡？
""")

with st.sidebar:
    threshold = st.slider("營收爆發門檻 (YoY %)", 50, 500, 100)
    year_filter = st.selectbox("分析年度", ["2024", "2023", "2022", "2021"])

# 這邊的 SQL 邏輯非常專業：
# 1. 找出爆發月份
# 2. 定義公告日為下個月 10 號
# 3. 關聯周 K 找出該日期前後的報酬
query = f"""
WITH target_events AS (
    SELECT stock_id, report_month, yoy_pct,
           -- 計算公告基準日 (報表月份的下個月 10 號)
           CASE 
             WHEN RIGHT(report_month, 2) = '12' THEN (LEFT(report_month, 3)::int + 1)::text || '-01-10'
             ELSE LEFT(report_month, 4) || (RIGHT(report_month, 2)::int + 1)::text || '-10'
           END::date as announce_date
    FROM monthly_revenue
    WHERE yoy_pct >= {threshold} AND report_month LIKE '{(int(year_filter)-1911)}_%'
),
timing_stats AS (
    SELECT 
        e.stock_id, e.report_month,
        -- 前一周報酬 (公告日前 7~14 天)
        AVG(CASE WHEN w.date >= e.announce_date - interval '14 days' AND w.date < e.announce_date - interval '7 days' 
            THEN (w.w_close - w.w_open)/w.w_open * 100 END) as week_minus_1,
        -- 當周報酬 (公告日前 0~7 天)
        AVG(CASE WHEN w.date >= e.announce_date - interval '7 days' AND w.date <= e.announce_date 
            THEN (w.w_close - w.w_open)/w.w_open * 100 END) as announce_week,
        -- 公告後 4 周平均報酬
        AVG(CASE WHEN w.date > e.announce_date AND w.date <= e.announce_date + interval '28 days' 
            THEN (w.w_close - w.w_open)/w.w_open * 100 END) as month_after
    FROM target_events e
    JOIN stock_weekly_k w ON e.stock_id = SPLIT_PART(w.symbol, '.', 1)
    GROUP BY e.stock_id, e.report_month
)
SELECT 
    COUNT(*) as "樣本數",
    ROUND(AVG(week_minus_1)::numeric, 2) as "前一周平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE week_minus_1 > 3) * 100.0 / COUNT(*))::numeric, 1) as "主力預跑率(>3%)",
    ROUND(AVG(announce_week)::numeric, 2) as "公告周平均漲幅%",
    ROUND(AVG(month_after)::numeric, 2) as "公告後一個月平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE month_after < week_minus_1) * 100.0 / COUNT(*))::numeric, 1) as "利多出盡(轉跌)機率"
FROM timing_stats
WHERE week_minus_1 IS NOT NULL;
"""

# 顯示結果... (略，結構同 probability.py)
