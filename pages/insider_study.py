import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

st.set_page_config(page_title="主力早知道 | StockRevenueLab", layout="wide")

@st.cache_resource
def get_engine():
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    PROJECT_REF = st.secrets["PROJECT_REF"]
    POOLER_HOST = st.secrets["POOLER_HOST"]
    connection_string = f"postgresql://postgres.{PROJECT_REF}:{urllib.parse.quote_plus(DB_PASSWORD)}@{POOLER_HOST}:5432/postgres?sslmode=require"
    return create_engine(connection_string)

st.title("🕵️ 主力早知道？營收爆發前後的股價行為")
st.markdown("""
本研究分析 **「第一次營收爆發」** 時，市場的反應。
* **主力預跑 (Month T)**：報表尚未公佈，股價是否先漲？
* **利多追價 (Month T+1)**：報表公佈後，市場是否跟進？
""")

threshold = st.slider("設定爆發門檻 (YoY %)", 20, 300, 100)

query = f"""
WITH first_events AS (
    -- 找出每檔股票第一次 YoY > threshold 的月份
    SELECT stock_id, report_month, yoy_pct,
           LAG(yoy_pct) OVER(PARTITION BY stock_id ORDER BY report_month) as prev_yoy
    FROM monthly_revenue
    WHERE yoy_pct >= {threshold}
),
filtered_first AS (
    -- 確保是「第一次」爆發 (前一個月沒達標，或是第一筆資料)
    SELECT * FROM first_events WHERE prev_yoy IS NULL OR prev_yoy < {threshold}
),
price_behavior AS (
    SELECT 
        f.stock_id, f.report_month, f.yoy_pct,
        -- 當月漲幅 (主力預跑)
        ((p1.m_close - p1.m_open)/p1.m_open * 100) as pre_run_ret,
        -- 下個月漲幅 (利多反應)
        ((p2.m_close - p2.m_open)/p2.m_open * 100) as post_run_ret
    FROM filtered_first f
    JOIN stock_monthly_k p1 ON f.stock_id = SPLIT_PART(p1.symbol, '.', 1) AND f.report_month = p1.report_month
    -- 這裡用複雜的對齊抓取 T+1 月
    LEFT JOIN stock_monthly_k p2 ON p1.symbol = p2.symbol 
      AND p2.report_month = (
          CASE WHEN RIGHT(p1.report_month, 2) = '12' 
          THEN (LEFT(p1.report_month, 3)::int + 1)::text || '_01'
          ELSE LEFT(p1.report_month, 4) || LPAD((RIGHT(p1.report_month, 2)::int + 1)::text, 2, '0')
          END
      )
)
SELECT 
    COUNT(*) as "總事件樣本",
    ROUND(AVG(pre_run_ret)::numeric, 1) as "預跑平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE pre_run_ret > 5) * 100.0 / COUNT(*))::numeric, 1) as "主力預跑率(漲幅>5%)",
    ROUND(AVG(post_run_ret)::numeric, 1) as "公佈後平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE post_run_ret > 5) * 100.0 / COUNT(*))::numeric, 1) as "公佈後追價率(漲幅>5%)",
    ROUND((COUNT(*) FILTER (WHERE post_run_ret < -5) * 100.0 / COUNT(*))::numeric, 1) as "利多出盡機率(跌幅>5%)"
FROM price_behavior
"""

with get_engine().connect() as conn:
    res = pd.read_sql_query(text(query), conn)
    if not res.empty:
        st.subheader("📊 全市場大數據分析結果")
        st.table(res)
        
        st.info("💡 註：『主力預跑』指營收月份當月。例如 11 月營收 12/10 才公佈，但 11 月股價就先漲了。")
    else:
        st.warning("請先執行月 K 線補齊腳本。")
