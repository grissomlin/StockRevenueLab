import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="公告行為研究室 2.0 | StockRevenueLab", layout="wide")

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
        st.error("❌ 連線失敗")
        st.stop()

# ========== 3. 核心標題 ==========
st.title("🕵️ 營收公告行為研究室 2.0")

with st.expander("📝 研究邏輯（漲跌比例說明）"):
    st.markdown("""
    * **漲跌比例**：計算在所有符合條件的樣本中，前一周股價呈現正報酬的家數佔比。
    * **極端預跑 (>10%)**：這代表主力不只是「先行」，而是「瘋狂掃貨」，這類股票公告後的利多出盡風險通常最高。
    """)

# --- 側邊欄控制 ---
with st.sidebar:
    st.header("🔬 策略參數")
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"設定 {study_metric} 爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 備註關鍵字 (如: 交屋, 訂單)", "")

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
            e.stock_id, e.stock_name, e.report_month, 
            ROUND(e.{metric_col}::numeric, 1) as growth_val, 
            e.remark,
            ROUND(AVG(CASE WHEN c.date >= e.base_date - interval '9 days' AND c.date <= e.base_date - interval '3 days' THEN c.weekly_ret END)::numeric, 2) as pre_week,
            ROUND(AVG(CASE WHEN c.date > e.base_date - interval '3 days' AND c.date <= e.base_date + interval '4 days' THEN c.weekly_ret END)::numeric, 2) as announce_week,
            ROUND(AVG(CASE WHEN c.date > e.base_date + interval '11 days' AND c.date <= e.base_date + interval '30 days' THEN c.weekly_ret END)::numeric, 2) as after_month
        FROM spark_events e
        JOIN weekly_calc c ON e.stock_id = SPLIT_PART(c.symbol, '.', 1)
        GROUP BY e.stock_id, e.stock_name, e.report_month, e.{metric_col}, e.remark, e.base_date
    )
    SELECT * FROM final_detail WHERE pre_week IS NOT NULL ORDER BY pre_week DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

df = fetch_timing_data(target_year, study_metric, threshold, search_remark)

if not df.empty:
    # --- A. 深度統計看板 ---
    total_n = len(df)
    up_count = (df['pre_week'] > 0).sum()
    super_up = (df['pre_week'] >= 10).sum()
    down_count = (df['pre_week'] < 0).sum()
    super_down = (df['pre_week'] <= -10).sum()

    st.subheader(f"📊 {target_year} 年 T-1周 (公告前夕) 漲跌分佈統計")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("樣本總數", f"{total_n} 檔")
    c2.metric("漲跌家數比", f"{up_count} 漲 / {down_count} 跌", f"{up_count/total_n*100:.1f}% 勝率")
    c3.metric("強勢預跑 (>10%)", f"{super_up} 檔", f"{super_up/total_n*100:.1f}% 比例")
    c4.metric("利多出盡比例", f"{(df['after_month'] < df['pre_week']).sum()} 檔", f"{(df['after_month'] < df['pre_week']).sum()/total_n*100:.1f}%")

    # --- B. 分佈直方圖 ---
    st.write("---")
    fig_hist = px.histogram(df, x="pre_week", nbins=50, 
                            title="公告前一周 (T-1) 漲跌幅分佈圖",
                            labels={'pre_week': '漲跌幅 %'},
                            color_discrete_sequence=['#ff4b4b'])
    fig_hist.add_vline(x=0, line_dash="dash", line_color="black")
    st.plotly_chart(fig_hist, use_container_width=True)

    # --- C. 個股清單 ---
    st.subheader("🏆 初號機個股清單與明細")
    display_df = df.rename(columns={
        "stock_id": "代號", "stock_name": "名稱", "report_month": "月份",
        "growth_val": f"{study_metric}%", "pre_week": "T-1周%",
        "announce_week": "T周%", "after_month": "一個月後%", "remark": "備註"
    })

    st.dataframe(
        display_df.style.background_gradient(subset=["T-1周%", "T周%", "一個月後%"], cmap="RdYlGn"),
        use_container_width=True, height=500,
        column_config={
            f"{study_metric}%": st.column_config.NumberColumn(format="%.2f"),
            "T-1周%": st.column_config.NumberColumn(format="%.2f"),
            "備註": st.column_config.TextColumn(width="large")
        }
    )
else:
    st.info("💡 找不到符合條件的公司。")
