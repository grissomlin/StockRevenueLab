import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px
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

# ========== 3. 繪圖輔助函數 (關鍵：計算分佈並標註) ==========
def create_enhanced_hist(df, col_name, title, color):
    if df[col_name].dropna().empty:
        return go.Figure()
    
    # 1. 計算分佈數據
    counts, bins = np.histogram(df[col_name].dropna(), bins=20)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    total = len(df)
    percentages = (counts / total) * 100
    
    # 2. 建立標籤文字 (例如: 15檔\n12.5%)
    texts = [f"{int(c)}檔<br>{p:.1f}%" if c > 0 else "" for c, p in zip(counts, percentages)]
    
    # 3. 使用 go.Bar 繪圖以獲得最大控制權
    fig = go.Figure(data=[
        go.Bar(
            x=bin_centers,
            y=counts,
            text=texts,
            textposition='outside',
            marker_color=color,
            hovertemplate="區間: %{x:.2f}%<br>家數: %{y}檔<br>比例: %{text}<extra></extra>"
        )
    ])
    
    fig.add_vline(x=0, line_dash="dash", line_color="black")
    fig.update_layout(
        title=title,
        xaxis_title="漲跌幅 %",
        yaxis_title="家數",
        margin=dict(t=50, b=20, l=10, r=10),
        height=350,
        showlegend=False
    )
    return fig

# ========== 4. 核心標題 ==========
st.title("🕵️ 營收公告行為研究室 2.0")

# --- 側邊欄控制 ---
with st.sidebar:
    st.header("🔬 策略參數設定")
    target_year = st.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    study_metric = st.radio("成長指標", ["yoy_pct", "mom_pct"])
    threshold = st.slider(f"設定 {study_metric} 爆發門檻 %", 30, 300, 100)
    search_remark = st.text_input("🔍 備註關鍵字 (如: 訂單, 日本, 交屋)", "")

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
            ROUND(e.{metric_col}::numeric, 2) as growth_val, 
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
    total_n = len(df)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("樣本總數", f"{total_n}")
    c2.metric("T-1 預跑勝率", f"{(df['pre_week']>0).sum()/total_n*100:.1f}%")
    c3.metric("T-1 平均報酬", f"{df['pre_week'].mean():.2f}%")
    c4.metric("利多出盡比例", f"{(df['after_month'] < df['pre_week']).sum()/total_n*100:.1f}%")

    st.write("---")
    
    # --- B. 初號機個股清單 ---
    st.subheader(f"🏆 {target_year} 年 初號機清單 (四階段對照)")
    display_df = df.rename(columns={
        "stock_id": "代號", "stock_name": "名稱", "report_month": "月份",
        "growth_val": f"{study_metric}%", 
        "pre_week": "T-1周(預跑)%", "announce_week": "T周(公告)%", 
        "after_week_1": "T+1周(後續)%", "after_month": "一個月後%", "remark": "備註"
    })

    st.dataframe(
        display_df.style.background_gradient(subset=["T-1周(預跑)%", "T周(公告)%", "T+1周(後續)%", "一個月後%"], cmap="RdYlGn"),
        use_container_width=True, height=450,
        column_config={
            f"{study_metric}%": st.column_config.NumberColumn(format="%.2f"),
            "備註": st.column_config.TextColumn(width="large")
        }
    )

    st.write("---")

    # --- C. 四張分布圖 (底部並排，顯示家數與比例) ---
    st.subheader("📊 階段報酬率分佈對照 (含家數與比例標記)")
    
    row1_col1, row1_col2 = st.columns(2)
    row2_col1, row2_col2 = st.columns(2)

    with row1_col1:
        st.plotly_chart(create_enhanced_hist(df, "pre_week", "❶ T-1 周 (公告前夕)", "#ff4b4b"), use_container_width=True)
    with row1_col2:
        st.plotly_chart(create_enhanced_hist(df, "announce_week", "❷ T 周 (公告當周)", "#ffaa00"), use_container_width=True)
    with row2_col1:
        st.plotly_chart(create_enhanced_hist(df, "after_week_1", "❸ T+1 周 (公告後一周)", "#32cd32"), use_container_width=True)
    with row2_col2:
        st.plotly_chart(create_enhanced_hist(df, "after_month", "❹ 公告後一個月", "#1e90ff"), use_container_width=True)

else:
    st.info("💡 找不到符合條件的公司。")
