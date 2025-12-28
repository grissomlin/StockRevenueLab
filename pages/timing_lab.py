import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px

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

# ========== 3. 核心標題 ==========
st.title("🕵️ 營收公告行為研究室 2.0")

with st.expander("📝 研究邏輯與名詞定義（必讀）"):
    st.markdown("""
    * **初號機邏輯 (First Spark)**：判定「上個月營收未達標，本月突然噴發」的公司。
    * **T-1 周 (主力預跑)**：每月 1~7 號。此時報表尚未公布，觀察是否有主力提前卡位。
    * **T 周 (消息噴發)**：每月 8~14 號。包含法定公告基準日 10 號。
    * **T+1 周與後一個月**：觀察消息公佈後的延續性。
    """)

# --- 側邊欄控制 ---
with st.sidebar:
    st.header("🔬 策略參數")
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"設定 {study_metric} 爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 備註關鍵字搜尋 (如: 交屋, 中油, 認列)", "")

# --- 核心 SQL：簡化數值與初號機邏輯 ---
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
            ROUND(AVG(CASE WHEN c.date > e.base_date + interval '4 days' AND c.date <= e.base_date + interval '11 days' THEN c.weekly_ret END)::numeric, 2) as after_week_1,
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
    # --- A. 統計看板 ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("初號機樣本數", f"{len(df)} 檔")
    c2.metric("T-1周平均漲幅", f"{df['pre_week'].mean():.2f}%")
    
    pre_run_prob = (df['pre_week'] > 2).sum() / len(df) * 100
    post_drop_prob = (df['after_month'] < df['pre_week']).sum() / len(df) * 100
    
    c3.metric("主力預跑機率", f"{pre_run_prob:.1f}%")
    c4.metric("利多出盡機率", f"{post_drop_prob:.1f}%")

    # --- B. 趨勢圖表 ---
    st.write("---")
    plot_df = pd.DataFrame({
        "階段": ["前一周(T-1)", "公告周(T)", "後一周(T+1)", "後一個月"],
        "平均報酬 %": [
            df['pre_week'].mean(), 
            df['announce_week'].mean(),
            df['after_week_1'].mean(),
            df['after_month'].mean()
        ]
    })
    fig = px.bar(plot_df, x="階段", y="平均報酬 %", color="平均報酬 %", 
                 color_continuous_scale="RdYlGn", text_auto=".2f")
    st.plotly_chart(fig, use_container_width=True)

    # --- C. 符合條件的公司清單 ---
    st.subheader(f"🏆 {target_year} 年符合門檻個股清單")
    
    display_df = df.rename(columns={
        "stock_id": "代號", "stock_name": "名稱", "report_month": "月份",
        "growth_val": f"{study_metric}%", "pre_week": "T-1周(預跑)%",
        "announce_week": "T周(公告)%", "after_week_1": "T+1周%", 
        "after_month": "一個月後%", "remark": "營收備註"
    })

    try:
        st.dataframe(
            display_df.style.background_gradient(subset=["T-1周(預跑)%", "T周(公告)%", "一個月後%"], cmap="RdYlGn"),
            use_container_width=True, 
            height=600,
            column_config={
                "營收備註": st.column_config.TextColumn("營收備註", width="large"),
                "代號": st.column_config.TextColumn("代號", width="small")
            }
        )
    except Exception:
        st.dataframe(display_df, use_container_width=True, height=600)

else:
    st.info("💡 找不到符合的公司，請嘗試降低門檻或更換關鍵字。")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 數據週期：2019-2025")
