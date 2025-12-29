# ===============================
# StockRevenueLab 4.3
# 公告行為研究室（右尾偏執版）
# ===============================

import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go

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
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    PROJECT_REF = st.secrets["PROJECT_REF"]
    POOLER_HOST = st.secrets["POOLER_HOST"]
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    conn = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
    return create_engine(conn)

# ========== 3. 分佈與偏度工具 ==========
def get_ai_summary_dist(df, col):
    data = df[col].dropna()
    if data.empty: return "無數據"
    bins = [-np.inf, -5, -1, 1, 5, np.inf]
    labels = ["大跌(<-5%)", "小跌", "持平", "小漲", "大漲(>5%)"]
    counts, _ = np.histogram(data, bins=bins)
    total = len(data)
    return " / ".join(
        f"{l}:{c}檔({c/total*100:.1f}%)"
        for l, c in zip(labels, counts) if c > 0
    )

def calc_rtc(series):
    s = series.dropna()
    if len(s) < 20: return np.nan
    q95 = np.percentile(s, 95)
    q75 = np.percentile(s, 75)
    q25 = np.percentile(s, 25)
    med = np.median(s)
    iqr = q75 - q25
    return round((q95 - med) / iqr, 2) if iqr != 0 else np.nan

def calc_tdir(series):
    s = series.dropna()
    if len(s) < 20: return np.nan
    top10 = s.quantile(0.9)
    return round(s[s >= top10].mean() / s.median(), 2) if s.median() != 0 else np.nan

def create_big_hist(df, col, title, color, desc):
    data = df[col].dropna()
    if data.empty: return
    mean, med = data.mean(), data.median()
    counts, bins = np.histogram(data, bins=25)
    centers = (bins[:-1] + bins[1:]) / 2

    fig = go.Figure(go.Bar(x=centers, y=counts, marker_color=color))
    fig.add_vline(x=0, line_dash="dash")
    fig.add_vline(x=mean, line_color="red", annotation_text=f"平均 {mean:.2f}%")
    fig.add_vline(x=med, line_color="blue", annotation_text=f"中位 {med:.2f}%")
    fig.update_layout(title=title, height=380)
    st.plotly_chart(fig, use_container_width=True)
    st.info(f"💡 {desc}")
    st.divider()

# ========== 4. SQL ==========
@st.cache_data(ttl=3600)
def fetch_timing_data(year, metric, limit, keyword):
    engine = get_engine()
    my = int(year) - 1911
    q = f"""
    WITH raw AS (
        SELECT stock_id, stock_name, report_month, {metric}, remark,
               LAG({metric}) OVER (PARTITION BY stock_id ORDER BY report_month) prev
        FROM monthly_revenue
        WHERE report_month LIKE '{my}_%' OR report_month LIKE '{my-1}_12'
    ),
    evt AS (
        SELECT *,
        CASE WHEN RIGHT(report_month,2)='12'
        THEN (LEFT(report_month,3)::int+1912)||'-01-10'
        ELSE (LEFT(report_month,3)::int+1911)||'-'||LPAD((RIGHT(report_month,2)::int+1)::text,2,'0')||'-10'
        END::date base_date
        FROM raw
        WHERE {metric}>={limit}
          AND (prev<{limit} OR prev IS NULL)
          AND report_month LIKE '{my}_%'
          AND (remark LIKE '%%{keyword}%%' OR stock_name LIKE '%%{keyword}%%')
    ),
    wk AS (
        SELECT symbol, date,
        (w_close-LAG(w_close) OVER (PARTITION BY symbol ORDER BY date))
        /NULLIF(LAG(w_close) OVER (PARTITION BY symbol ORDER BY date),0)*100 ret
        FROM stock_weekly_k
    )
    SELECT e.stock_id,e.stock_name,e.report_month,e.{metric} growth_val,e.remark,
    AVG(CASE WHEN date BETWEEN base_date-38 AND base_date-9 THEN ret END)*4 pre_month,
    AVG(CASE WHEN date BETWEEN base_date-9 AND base_date-3 THEN ret END) pre_week,
    AVG(CASE WHEN date BETWEEN base_date-3 AND base_date+4 THEN ret END) announce_week,
    AVG(CASE WHEN date BETWEEN base_date+4 AND base_date+11 THEN ret END) after_week_1,
    AVG(CASE WHEN date BETWEEN base_date+11 AND base_date+30 THEN ret END) after_month
    FROM evt e JOIN wk ON e.stock_id=SPLIT_PART(symbol,'.',1)
    GROUP BY e.stock_id,e.stock_name,e.report_month,e.{metric},e.remark,base_date
    """
    return pd.read_sql(text(q), engine)

# ========== 5. UI ==========
with st.sidebar:
    st.header("🔬 研究設定")
    year = st.selectbox("年度", [str(y) for y in range(2025, 2019, -1)], 1)
    metric = st.radio("指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider("爆發門檻 %", 30, 300, 100)
    kw = st.text_input("關鍵字")

st.title(f"🕵️ {year} 公告行為研究室 4.3")

df = fetch_timing_data(year, metric, threshold, kw)

if df.empty:
    st.info("無符合樣本")
    st.stop()

# ========== Dashboard ==========
def stat(col): return round(df[col].mean(),2), round(df[col].median(),2)

m_mean,m_med = stat("pre_month")
w_mean,w_med = stat("pre_week")
a_mean,a_med = stat("announce_week")
aw_mean,aw_med = stat("after_week_1")
f_mean,f_med = stat("after_month")

rtc = calc_rtc(df["pre_month"])
tdir = calc_tdir(df["pre_month"])

c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("樣本", len(df))
c2.metric("T-1月", f"{m_mean}%", f"中位 {m_med}%")
c3.metric("T-1周", f"{w_mean}%", f"中位 {w_med}%")
c4.metric("T周", f"{a_mean}%", f"中位 {a_med}%")
c5.metric("T+1周", f"{aw_mean}%", f"中位 {aw_med}%")
c6.metric("右尾偏執", f"RTC {rtc}", f"TDIR {tdir}")

# ========== 圖表 ==========
create_big_hist(df,"pre_month","⓪ T-1月 內部人","purple","右尾越強＝資訊不對稱越明顯")
create_big_hist(df,"pre_week","❶ T-1周 偷跑","red","少數人知道")
create_big_hist(df,"announce_week","❷ T周 確認","orange","市場共識")
create_big_hist(df,"after_week_1","❸ T+1周 延續","green","是否追價")
create_big_hist(df,"after_month","❹ T+1月 消化","blue","時間成本")

# ========== AI Prompt ==========
prompt = f"""
分析台股 {year} 年營收爆發（樣本 {len(df)}）。
T-1月 平均 {m_mean}% / 中位 {m_med}% / RTC {rtc} / TDIR {tdir}
T-1周 平均 {w_mean}% / 中位 {w_med}%
T周 平均 {a_mean}% / 中位 {a_med}%
T+1周 平均 {aw_mean}% / 中位 {aw_med}%
T+1月 中位 {f_med}%

請判斷：
1️⃣ 是否存在公告前資訊不對稱的集中布局？
2️⃣ 右尾是否主導整體報酬？
3️⃣ 公告後是否具延續性？
"""

st.code(prompt)
