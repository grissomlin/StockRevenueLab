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

# ========== 3. 核心標題與邏輯說明 ==========
st.title("🕵️ 營收公告行為研究室 2.0")

with st.expander("📝 研究邏輯與名詞定義（必讀）"):
    st.markdown("""
    * **初號機邏輯 (First Spark)**：系統會自動判定「上個月營收未達標，本月突然噴發」的公司。這能幫你避開已經漲過頭的股票，專注在**起漲點**。
    * **T-1 周 (主力預跑)**：每月 1~7 號。此時報表尚未公布，若大漲代表主力先行。
    * **T 周 (消息噴發)**：每月 8~14 號。包含法定公告日 10 號，觀察市場對利多的即時情緒。
    * **T+1 周與後一個月**：觀察消息公布後，是「利多出盡」回檔，還是「波段啟動」續噴。
    """)

# --- 側邊欄控制 ---
with st.sidebar:
    st.header("🔬 策略參數")
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"], help="yoy為年增率，mom為月增率")
    threshold = st.slider(f"設定 {study_metric} 爆發門檻 %", 30, 300, 100)
    
# --- 核心 SQL：加入「初號機」判定與明細 ---
@st.cache_data(ttl=3600)
def fetch_timing_data(year, metric_col, yoy_limit):
    engine = get_engine()
    minguo_year = int(year) - 1911
    
    query = f"""
    WITH raw_events AS (
        -- 找出符合門檻的月份，並抓取前一個月的數據來比對
        SELECT stock_id, stock_name, report_month, {metric_col}, remark,
               LAG({metric_col}) OVER (PARTITION BY stock_id ORDER BY report_month) as prev_metric
        FROM monthly_revenue
        WHERE report_month LIKE '{minguo_year}_%' OR report_month LIKE '{int(minguo_year)-1}_12'
    ),
    spark_events AS (
        -- 「初號機」判定：本月 > 門檻 且 (上月 < 門檻 或 上月為空)
        SELECT *,
               CASE 
                 WHEN RIGHT(report_month, 2) = '12' THEN (LEFT(report_month, 3)::int + 1 + 1911)::text || '-01-10'
                 ELSE (LEFT(report_month, 3)::int + 1911)::text || '-' || LPAD((RIGHT(report_month, 2)::int + 1)::text, 2, '0') || '-10'
               END::date as base_date
        FROM raw_events
        WHERE {metric_col} >= {yoy_limit} 
          AND (prev_metric < {yoy_limit} OR prev_metric IS NULL)
          AND report_month LIKE '{minguo_year}_%'
    ),
    weekly_calc AS (
        SELECT symbol, date, w_close,
               (w_close - LAG(w_close) OVER (PARTITION BY symbol ORDER BY date)) / 
               NULLIF(LAG(w_close) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 as weekly_ret
        FROM stock_weekly_k
    ),
    final_detail AS (
        SELECT 
            e.stock_id, e.stock_name, e.report_month, e.{metric_col} as growth_val,
            AVG(CASE WHEN c.date >= e.base_date - interval '9 days' AND c.date <= e.base_date - interval '3 days' THEN c.weekly_ret END) as pre_week,
            AVG(CASE WHEN c.date > e.base_date - interval '3 days' AND c.date <= e.base_date + interval '4 days' THEN c.weekly_ret END) as announce_week,
            AVG(CASE WHEN c.date > e.base_date + interval '4 days' AND c.date <= e.base_date + interval '11 days' THEN c.after_week_1_ret END) as after_week_1, -- 這裡簡化
            AVG(CASE WHEN c.date > e.base_date + interval '11 days' AND c.date <= e.base_date + interval '30 days' THEN c.weekly_ret END) as after_month,
            e.remark
        FROM spark_events e
        JOIN weekly_calc c ON e.stock_id = SPLIT_PART(c.symbol, '.', 1)
        GROUP BY e.stock_id, e.stock_name, e.report_month, e.{metric_col}, e.base_date, e.remark
    )
    SELECT * FROM final_detail WHERE pre_week IS NOT NULL ORDER BY pre_week DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

df = fetch_timing_data(target_year, study_metric, threshold)

if not df.empty:
    # --- A. 統計看板 ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("初號機爆發樣本", f"{len(df)} 檔")
    c2.metric("公告前預跑均值", f"{df['pre_week'].mean():.2f}%")
    
    # 計算機率
    pre_run_prob = (df['pre_week'] > 3).sum() / len(df) * 100
    post_drop_prob = (df['after_month'] < df['pre_week']).sum() / len(df) * 100
    
    c3.metric("主力預跑機率 (>3%)", f"{pre_run_prob:.1f}%")
    c4.metric("利多出盡機率", f"{post_drop_prob:.1f}%")

    # --- B. 趨勢圖表 ---
    st.write("---")
    plot_df = pd.DataFrame({
        "階段": ["前一周(T-1)", "公告周(T)", "後一個月"],
        "平均報酬 %": [df['pre_week'].mean(), df['announce_week'].mean(), df['after_month'].mean()]
    })
    fig = px.bar(plot_data, x="階段", y="平均報酬 %", color="平均報酬 %", color_continuous_scale="RdYlGn", text_auto=".2f")
    st.plotly_chart(fig, use_container_width=True)

    # --- C. 符合條件的公司清單 ---
    st.subheader(f"🏆 {target_year} 年符合門檻的『初號機』名單與表現")
    st.markdown("這裡列出了所有「突然爆發」的公司，你可以觀察它們公告前的預跑幅度與公告後的回檔。")
    
    # 重新命名欄位讓讀者更好懂
    display_df = df.rename(columns={
        "stock_id": "代號", "stock_name": "名稱", "report_month": "爆發月份",
        "growth_val": f"{study_metric}%", "pre_week": "前一周(預跑)%",
        "announce_week": "公告周%", "after_month": "後一個月%", "remark": "營收備註"
    })
    
    # 使用 dataframe 呈現，並加上樣式顏色
    st.dataframe(
        display_df.style.background_gradient(subset=["前一周(預跑)%", "後一個月%"], cmap="RdYlGn"),
        use_container_width=True, height=500
    )

else:
    st.warning("⚠️ 此條件下找不到符合的公司。建議調低門檻或切換指標試試看！")
