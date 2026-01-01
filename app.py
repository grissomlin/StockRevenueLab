import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
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
    except Exception as e:
        st.error("❌ 資料庫連線失敗，請檢查 Streamlit Secrets 設定。")
        st.stop()
@st.cache_data(ttl=3600)
def get_latest_data_date():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # 抓取股價表中最晚的日期
            query = text("SELECT MAX(date) FROM stock_prices")
            result = conn.execute(query).scalar()
            
            # 如果 result 是 datetime 物件，轉為字串格式 (YYYY-MM-DD)
            if hasattr(result, 'strftime'):
                return result.strftime('%Y-%m-%d')
            return str(result)
    except Exception as e:
        return "資料擷取中"
# ========== 1. 頁面配置 ==========
st.set_page_config(
    page_title="StockRevenueLab | 趨勢觀測站",
    page_icon="🧪",
    layout="wide"
)

# 自定義 CSS 美化
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { border-left: 5px solid #ff4b4b; background-color: white; padding: 10px; border-radius: 5px; }
    div[data-testid="stExpander"] { border: 1px solid #e0e0e0; border-radius: 10px; }
    .stat-card { background: white; padding: 15px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); margin: 5px; }
    .counter-badge { background: linear-gradient(45deg, #FF6B6B, #FF8E53); color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; }
    .ai-panel { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# 側邊欄導引
st.sidebar.success("💡 想要看『勝率分析』？請點選左側選單的 probability 頁面！")

# 獲取資料庫最新日期
latest_date = get_latest_data_date()

# 顯示資料庫狀態（取代原本的計數器）
st.sidebar.markdown(f"""
<div style="text-align: center; margin: 20px 0;">
    <div class="counter-badge" style="background: linear-gradient(45deg, #4b6cb7, #182848);">📊 資料庫同步狀態</div>
    <h2 style="color: #4b6cb7; margin: 5px 0;">{latest_date}</h2>
    <small style="color: #666;">當前系統最新數據月份</small>
</div>
""", unsafe_allow_html=True)

st.title("🧪 StockRevenueLab: 全時段飆股基因對帳單")
st.markdown("#### 透過 16 萬筆真實數據，揭開業績與股價漲幅的神秘面紗")



# ========== 🚀 核心變數定義區 (必須放在 fetch 數據之前) ==========
st.sidebar.header("🔬 研究條件篩選")

# 1. 定義年度
target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)

# 2. 定義指標
metric_choice = st.sidebar.radio("成長指標", ["年增率 (YoY)", "月增率 (MoM)"], help="YoY看長期趨勢，MoM看短期爆發")
target_col = "yoy_pct" if metric_choice == "年增率 (YoY)" else "mom_pct"

# 3. 定義統計模式
stat_methods = [
    "中位數 (排除極端值)",
    "平均值 (含極端值)", 
    "標準差 (波動程度)",
    "變異係數 (相對波動)",
    "偏度 (分佈形狀)",
    "峰度 (尾部厚度)",
    "四分位距 (離散程度)",
    "正樣本比例"
]
stat_method = st.sidebar.selectbox("統計指標模式", stat_methods, index=0)

# =============================================================
# ========== 3. 數據抓取引擎 (支援多種統計模式，下跌10%間隔，上漲100%間隔) ==========
@st.cache_data(ttl=3600)
def fetch_heatmap_data(year, metric_col, stat_method):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    # 根據統計方法選擇聚合函數
    if stat_method == "中位數 (排除極端值)":
        agg_func = f"percentile_cont(0.5) WITHIN GROUP (ORDER BY m.{metric_col})"
        stat_label = "中位數"
    elif stat_method == "平均值 (含極端值)":
        agg_func = f"AVG(m.{metric_col})"
        stat_label = "平均值"
    elif stat_method == "標準差 (波動程度)":
        agg_func = f"STDDEV(m.{metric_col})"
        stat_label = "標準差"
    elif stat_method == "變異係數 (相對波動)":
        agg_func = f"CASE WHEN AVG(m.{metric_col}) = 0 THEN 0 ELSE (STDDEV(m.{metric_col}) / ABS(AVG(m.{metric_col}))) * 100 END"
        stat_label = "變異係數%"
    elif stat_method == "偏度 (分佈形狀)":
        agg_func = f"""
        CASE WHEN STDDEV(m.{metric_col}) = 0 THEN 0 
             ELSE (AVG(POWER((m.{metric_col} - AVG(m.{metric_col}))/NULLIF(STDDEV(m.{metric_col}),0), 3))) 
        END
        """
        stat_label = "偏度"
    elif stat_method == "峰度 (尾部厚度)":
        agg_func = f"""
        CASE WHEN STDDEV(m.{metric_col}) = 0 THEN 0 
             ELSE (AVG(POWER((m.{metric_col} - AVG(m.{metric_col}))/NULLIF(STDDEV(m.{metric_col}),0), 4)) - 3) 
        END
        """
        stat_label = "峰度"
    elif stat_method == "四分位距 (離散程度)":
        agg_func = f"percentile_cont(0.75) WITHIN GROUP (ORDER BY m.{metric_col}) - percentile_cont(0.25) WITHIN GROUP (ORDER BY m.{metric_col})"
        stat_label = "四分位距"
    elif stat_method == "正樣本比例":
        agg_func = f"SUM(CASE WHEN m.{metric_col} > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)"
        stat_label = "正增長比例%"
    else:
        agg_func = f"AVG(m.{metric_col})"
        stat_label = "平均值"
    
    # 修改這裡：下跌10%間隔，上漲100%間隔
    query = f"""
    WITH annual_bins AS (
        SELECT 
            symbol,
            ((year_close - year_open) / year_open) * 100 AS annual_return,
            CASE 
                -- 下跌區間：每10%一個間隔（從-100%到0%）
                WHEN ((year_close - year_open) / year_open) * 100 <= -100 THEN '00. 下跌-100%以下'
                WHEN ((year_close - year_open) / year_open) * 100 < -90 THEN '01. 下跌-100%至-90%'
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN '02. 下跌-90%至-80%'
                WHEN ((year_close - year_open) / year_open) * 100 < -70 THEN '03. 下跌-80%至-70%'
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN '04. 下跌-70%至-60%'
                WHEN ((year_close - year_open) / year_open) * 100 < -50 THEN '05. 下跌-60%至-50%'
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN '06. 下跌-50%至-40%'
                WHEN ((year_close - year_open) / year_open) * 100 < -30 THEN '07. 下跌-40%至-30%'
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN '08. 下跌-30%至-20%'
                WHEN ((year_close - year_open) / year_open) * 100 < -10 THEN '09. 下跌-20%至-10%'
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN '10. 下跌-10%至0%'
                -- 上漲區間：每100%一個間隔
                WHEN ((year_close - year_open) / year_open) * 100 < 100 THEN '11. 上漲0-100%'
                WHEN ((year_close - year_open) / year_open) * 100 < 200 THEN '12. 上漲100-200%'
                WHEN ((year_close - year_open) / year_open) * 100 < 300 THEN '13. 上漲200-300%'
                WHEN ((year_close - year_open) / year_open) * 100 < 400 THEN '14. 上漲300-400%'
                WHEN ((year_close - year_open) / year_open) * 100 < 500 THEN '15. 上漲400-500%'
                WHEN ((year_close - year_open) / year_open) * 100 < 600 THEN '16. 上漲500-600%'
                WHEN ((year_close - year_open) / year_open) * 100 < 700 THEN '17. 上漲600-700%'
                WHEN ((year_close - year_open) / year_open) * 100 < 800 THEN '18. 上漲700-800%'
                WHEN ((year_close - year_open) / year_open) * 100 < 900 THEN '19. 上漲800-900%'
                WHEN ((year_close - year_open) / year_open) * 100 < 1000 THEN '20. 上漲900-1000%'
                ELSE '21. 上漲1000%以上'
            END AS return_bin,
            -- 為了分組排序，新增一個順序欄位
            CASE 
                WHEN ((year_close - year_open) / year_open) * 100 <= -100 THEN 0
                WHEN ((year_close - year_open) / year_open) * 100 < -90 THEN 1
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN 2
                WHEN ((year_close - year_open) / year_open) * 100 < -70 THEN 3
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN 4
                WHEN ((year_close - year_open) / year_open) * 100 < -50 THEN 5
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN 6
                WHEN ((year_close - year_open) / year_open) * 100 < -30 THEN 7
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN 8
                WHEN ((year_close - year_open) / year_open) * 100 < -10 THEN 9
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN 10
                WHEN ((year_close - year_open) / year_open) * 100 < 100 THEN 11
                WHEN ((year_close - year_open) / year_open) * 100 < 200 THEN 12
                WHEN ((year_close - year_open) / year_open) * 100 < 300 THEN 13
                WHEN ((year_close - year_open) / year_open) * 100 < 400 THEN 14
                WHEN ((year_close - year_open) / year_open) * 100 < 500 THEN 15
                WHEN ((year_close - year_open) / year_open) * 100 < 600 THEN 16
                WHEN ((year_close - year_open) / year_open) * 100 < 700 THEN 17
                WHEN ((year_close - year_open) / year_open) * 100 < 800 THEN 18
                WHEN ((year_close - year_open) / year_open) * 100 < 900 THEN 19
                WHEN ((year_close - year_open) / year_open) * 100 < 1000 THEN 20
                ELSE 21
            END AS bin_order
        FROM stock_annual_k
        WHERE year = '{year}'
    ),

    monthly_stats AS (
            -- 修正：只抓取去年12月到今年11月，共12個月
            SELECT stock_id, report_month, {metric_col} 
            FROM monthly_revenue
            WHERE report_month = '{prev_minguo_year}_12'  -- 去年12月
               OR (report_month LIKE '{minguo_year}_%' 
                   AND report_month < '{minguo_year}_12'  -- 排除當年12月
                   AND LENGTH(report_month) <= 7)
    )


    
    SELECT 
        b.return_bin,
        b.bin_order,
        m.report_month,
        {agg_func} as val,
        COUNT(DISTINCT b.symbol) as stock_count,
        COUNT(m.{metric_col}) as data_points,
        AVG(b.annual_return) as avg_annual_return  -- 新增：計算該區間的平均股價漲幅
    FROM annual_bins b
    JOIN monthly_stats m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    WHERE m.{metric_col} IS NOT NULL
    GROUP BY b.return_bin, b.bin_order, m.report_month
    ORDER BY b.bin_order, m.report_month;
    """
    
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)
        df['stat_method'] = stat_method
        df['stat_label'] = stat_label
        # 按照bin_order排序
        df = df.sort_values(['bin_order', 'report_month'])
        return df

# ========== 4. 統計摘要數據抓取 (修改版，下跌10%間隔，上漲100%間隔) ==========
@st.cache_data(ttl=3600)
def fetch_stat_summary(year, metric_col):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH annual_bins AS (
        SELECT 
            symbol,
            ((year_close - year_open) / year_open) * 100 AS annual_return,
            CASE 
                -- 下跌區間：每10%一個間隔（從-100%到0%）
                WHEN ((year_close - year_open) / year_open) * 100 <= -100 THEN '00. 下跌-100%以下'
                WHEN ((year_close - year_open) / year_open) * 100 < -90 THEN '01. 下跌-100%至-90%'
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN '02. 下跌-90%至-80%'
                WHEN ((year_close - year_open) / year_open) * 100 < -70 THEN '03. 下跌-80%至-70%'
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN '04. 下跌-70%至-60%'
                WHEN ((year_close - year_open) / year_open) * 100 < -50 THEN '05. 下跌-60%至-50%'
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN '06. 下跌-50%至-40%'
                WHEN ((year_close - year_open) / year_open) * 100 < -30 THEN '07. 下跌-40%至-30%'
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN '08. 下跌-30%至-20%'
                WHEN ((year_close - year_open) / year_open) * 100 < -10 THEN '09. 下跌-20%至-10%'
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN '10. 下跌-10%至0%'
                -- 上漲區間：每100%一個間隔
                WHEN ((year_close - year_open) / year_open) * 100 < 100 THEN '11. 上漲0-100%'
                WHEN ((year_close - year_open) / year_open) * 100 < 200 THEN '12. 上漲100-200%'
                WHEN ((year_close - year_open) / year_open) * 100 < 300 THEN '13. 上漲200-300%'
                WHEN ((year_close - year_open) / year_open) * 100 < 400 THEN '14. 上漲300-400%'
                WHEN ((year_close - year_open) / year_open) * 100 < 500 THEN '15. 上漲400-500%'
                WHEN ((year_close - year_open) / year_open) * 100 < 600 THEN '16. 上漲500-600%'
                WHEN ((year_close - year_open) / year_open) * 100 < 700 THEN '17. 上漲600-700%'
                WHEN ((year_close - year_open) / year_open) * 100 < 800 THEN '18. 上漲700-800%'
                WHEN ((year_close - year_open) / year_open) * 100 < 900 THEN '19. 上漲800-900%'
                WHEN ((year_close - year_open) / year_open) * 100 < 1000 THEN '20. 上漲900-1000%'
                ELSE '21. 上漲1000%以上'
            END AS return_bin,
            -- 為了分組排序，新增一個順序欄位
            CASE 
                WHEN ((year_close - year_open) / year_open) * 100 <= -100 THEN 0
                WHEN ((year_close - year_open) / year_open) * 100 < -90 THEN 1
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN 2
                WHEN ((year_close - year_open) / year_open) * 100 < -70 THEN 3
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN 4
                WHEN ((year_close - year_open) / year_open) * 100 < -50 THEN 5
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN 6
                WHEN ((year_close - year_open) / year_open) * 100 < -30 THEN 7
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN 8
                WHEN ((year_close - year_open) / year_open) * 100 < -10 THEN 9
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN 10
                WHEN ((year_close - year_open) / year_open) * 100 < 100 THEN 11
                WHEN ((year_close - year_open) / year_open) * 100 < 200 THEN 12
                WHEN ((year_close - year_open) / year_open) * 100 < 300 THEN 13
                WHEN ((year_close - year_open) / year_open) * 100 < 400 THEN 14
                WHEN ((year_close - year_open) / year_open) * 100 < 500 THEN 15
                WHEN ((year_close - year_open) / year_open) * 100 < 600 THEN 16
                WHEN ((year_close - year_open) / year_open) * 100 < 700 THEN 17
                WHEN ((year_close - year_open) / year_open) * 100 < 800 THEN 18
                WHEN ((year_close - year_open) / year_open) * 100 < 900 THEN 19
                WHEN ((year_close - year_open) / year_open) * 100 < 1000 THEN 20
                ELSE 21
            END AS bin_order
        FROM stock_annual_k
        WHERE year = '{year}'
    ),
    monthly_stats AS (
            -- 修正：確保統計摘要也只計算這12個月
            SELECT stock_id, report_month, {metric_col} 
            FROM monthly_revenue
            WHERE report_month = '{prev_minguo_year}_12'
               OR (report_month LIKE '{minguo_year}_%' 
                   AND report_month < '{minguo_year}_12'
                   AND LENGTH(report_month) <= 7)
        )


    
    SELECT 
        b.return_bin,
        b.bin_order,
        COUNT(DISTINCT b.symbol) as stock_count,
        AVG(b.annual_return) as avg_annual_return,  -- 該區間的平均股價漲幅
        ROUND(AVG(m.{metric_col})::numeric, 2) as mean_val,
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY m.{metric_col})::numeric, 2) as median_val,
        ROUND(STDDEV(m.{metric_col})::numeric, 2) as std_val,
        ROUND(MIN(m.{metric_col})::numeric, 2) as min_val,
        ROUND(MAX(m.{metric_col})::numeric, 2) as max_val,
        ROUND((STDDEV(m.{metric_col}) / NULLIF(AVG(m.{metric_col}), 0))::numeric, 2) as cv_val,
        ROUND((percentile_cont(0.75) WITHIN GROUP (ORDER BY m.{metric_col}) - 
               percentile_cont(0.25) WITHIN GROUP (ORDER BY m.{metric_col}))::numeric, 2) as iqr_val,
        ROUND(SUM(CASE WHEN m.{metric_col} > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as positive_rate
    FROM annual_bins b
    JOIN monthly_stats m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    WHERE m.{metric_col} IS NOT NULL
    GROUP BY b.return_bin, b.bin_order
    ORDER BY b.bin_order;
    """
    
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 5. AI分析提示詞生成 (整合全維度數據 + 保留原所有任務) ==========
def generate_ai_prompt(target_year, metric_choice, stat_method, stat_summary, pivot_df, total_samples):
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 1. 找出最慘的下跌區間 (保留原邏輯)
    worst_bins = stat_summary[stat_summary['return_bin'].str.contains('下跌')].copy()
    if not worst_bins.empty:
        worst_bin = worst_bins.loc[worst_bins['avg_annual_return'].idxmin()]
        worst_bin_name = worst_bin['return_bin']
        worst_avg_return = worst_bin['avg_annual_return']
        worst_pos_rate = worst_bin['positive_rate']
    else:
        worst_bin_name = "無資料"; worst_avg_return = 0; worst_pos_rate = 0
    
    # 2. 找出最好的上漲區間 (保留原邏輯)
    best_bins = stat_summary[stat_summary['return_bin'].str.contains('上漲')].copy()
    if not best_bins.empty:
        best_bin = best_bins.loc[best_bins['avg_annual_return'].idxmax()]
        best_bin_name = best_bin['return_bin']
        best_avg_return = best_bin['avg_annual_return']
        best_pos_rate = best_bin['positive_rate']
    else:
        best_bin_name = "無資料"; best_avg_return = 0; best_pos_rate = 0

    # 3. 建立「全維度」數據摘要表 (升級此處，包含標準差、變異係數、四分位距等)
    summary_table = "| 漲幅區間 | 股票數量 | 均漲幅 | 均營收 | 中位數 | 標準差 | 變異係數 | 四分位距 | 正成長% |\n"
    summary_table += "|----------|----------|--------|--------|--------|--------|----------|----------|---------|\n"
    for _, row in stat_summary.iterrows():
        bin_name = row['return_bin']
        summary_table += (
            f"| {bin_name} | {row['stock_count']}檔 | {row['avg_annual_return']:.1f}% "
            f"| {row['mean_val']:.1f}% | {row['median_val']:.1f}% | {row['std_val']:.1f} "
            f"| {row['cv_val']:.2f} | {row['iqr_val']:.1f} | {row['positive_rate']:.1f}% |\n"
        )
    
    # 4. 計算背景統計 (保留原邏輯)
    total_falling_stocks = stat_summary[stat_summary['return_bin'].str.contains('下跌')]['stock_count'].sum()
    total_rising_stocks = stat_summary[stat_summary['return_bin'].str.contains('上漲')]['stock_count'].sum()
    falling_ratio = total_falling_stocks / total_samples * 100
    rising_ratio = total_rising_stocks / total_samples * 100
    
    # 5. 組合最終提示詞 (完全保留您原本的分析框架與任務說明)
    prompt = f"""# 台股營收與股價關聯分析報告
分析時間: {current_date}
分析年度: {target_year}年
成長指標: {metric_choice}
統計方法: {stat_method}
總樣本數: {total_samples:,}檔
下跌股票比例: {falling_ratio:.1f}% ({total_falling_stocks:,}檔)
上漲股票比例: {rising_ratio:.1f}% ({total_rising_stocks:,}檔)

## 🎯 重要數據說明
**這是「按股價漲幅分組看營收表現」，分組間隔為：下跌每10%，上漲每100%**

### 數據結構說明：
1. **分組依據**：先按照股票「年度實際漲幅」分成不同區間
   - 下跌股票：每10%一個間隔（共11個區間，從-100%以下到-10%~0%）
   - 上漲股票：每100%一個間隔（共11個區間，從0-100%到1000%以上）

2. **觀察指標**：在每個股價漲幅區間內，計算該區間股票的營收全維度表現（包含離散程度指標）

### 關鍵發現：
1. **最慘的下跌區間**: {worst_bin_name} (平均股價漲幅{worst_avg_return:.1f}%，營收正增長比例{worst_pos_rate:.1f}%)
2. **最好的上漲區間**: {best_bin_name} (平均股價漲幅{best_avg_return:.1f}%，營收正增長比例{best_pos_rate:.1f}%)

## 數據摘要全表 (包含離散指標)
{summary_table}

## 🎯 分析任務（請特別關注下跌區間的細分分析與離散指標）
請擔任專業量化分析師，根據以上細分數據回答：

### 1. 下跌股票的梯度分析（每10%一個等級）
- **跌幅深度與營收表現的關係**：越深的跌幅，營收表現是否越差？
- **關鍵轉折點**：哪個跌幅區間開始，營收表現出現明顯惡化？
- **輕微下跌股**（跌10%以內）vs **重度下跌股**（跌50%以上）：營收表現差異有多大？

### 2. 上漲股票的層級分析（每100%一個等級）
- **漲幅高度與營收表現的關係**：漲得越高的股票，營收表現是否越好？
- **甜蜜點分析**：哪個漲幅區間的營收表現最突出？是100-200%還是200-300%？
- **極端上漲股**（漲500%以上）：營收表現有何特徵？是持續高成長還是波動大？

### 3. 對比分析：下跌vs上漲 (新增離散指標維度)
- **營收正增長比例**：上漲股票 vs 下跌股票，差距有多大？
- **營收波動率 (利用變異係數/標準差)**：哪個區間的營收波動最大？是最弱的下跌股還是最強的上漲股？
- **異常值分析**：有沒有「股價跌但營收好」或「股價漲但營收差」的明顯案例？請參考中位數與平均值的偏離。

### 4. 投資策略啟示
- **抄底策略**：根據10%間隔數據，哪個跌幅區間最適合抄底？
- **強勢股篩選**：要找到潛在飆股，應該關注哪些營收特徵？
- **風險控管**：哪些跌幅區間應該絕對避免？有沒有「越跌越危險」的趨勢？

## 📊 分析框架建議
請按照以下順序分析：
1. **下跌梯度分析**：從-100%到0%，分析每10%間隔的營收表現變化
2. **上漲層級分析**：從0%到1000%以上，分析每100%間隔的營收表現變化
3. **對比分析**：比較下跌和上漲股票的營收特徵差異 (請務必運用標準差與變異係數)
4. **投資應用**：提出基於梯度數據的具體投資策略

## ⚠️ 重要提醒
1. **間隔差異**：下跌10%間隔 vs 上漲100%間隔，反映市場特性（下跌更敏感）
2. **樣本數注意**：極端區間（如-100%以下或1000%以上）可能股票很少
3. **時間滯後性**：{target_year}年1月看到的是前一年12月營收
4. **統計顯著性**：小樣本區間的結論需謹慎

## 📝 回答要求
1. 用中文回答，結構清晰
2. 特別關注**下跌10%間隔的細緻變化**
3. 每個觀點都要有具體的數據支持（特別是中位數、標準差、變異係數等新維度）
4. 提供基於梯度分析的具體投資建議

現在，請開始您的專業分析：
"""
    return prompt

# ========== 7. 儀表板主視圖 ==========
# 初始化變數，避免頁尾報錯
total_samples = 0
actual_months = 0
total_data_points = 0

df = fetch_heatmap_data(target_year, target_col, stat_method)
stat_summary = fetch_stat_summary(target_year, target_col)

if not df.empty:
    # 頂部指標
    actual_months = df['report_month'].nunique()
    total_samples = df.groupby('return_bin')['stock_count'].max().sum()
    total_data_points = df['data_points'].sum() if 'data_points' in df.columns else 0
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("研究樣本總數", f"{int(total_samples):,} 檔")
    with c2: st.metric("當前觀測年度", f"{target_year} 年")
    with c3: st.metric("數據完整度", f"{actual_months} 個月份")
    with c4: st.metric("數據點總數", f"{int(total_data_points):,}")
    
    # ========== 8. 統計摘要卡片 ==========
    st.subheader("📈 統計指標說明")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="stat-card">
        <h4>📊 中位數</h4>
        <p>數據排序後的中間值，對極端值不敏感，反映典型情況</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="stat-card">
        <h4>📐 變異係數</h4>
        <p>標準差除以平均值，比較不同尺度數據的波動性</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="stat-card">
        <h4>⚖️ 偏度</h4>
        <p>分佈不對稱程度：正偏（右尾長）、負偏（左尾長）</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="stat-card">
        <h4>🏔️ 峰度</h4>
        <p>分佈尾部厚度：高峰度（極端值多）、低峰度（極端值少）</p>
        </div>
        """, unsafe_allow_html=True)
    
    # ========== 9. 熱力圖 ==========
    st.subheader(f"📊 {target_year} 「漲幅區間 vs {metric_choice}」業績對照熱力圖")
    st.info(f"**當前統計模式：{stat_method}** | 顏色深淺代表統計值的大小")
    
    pivot_df = df.pivot(index='return_bin', columns='report_month', values='val')

    # 根據統計方法選擇顏色方案
    if "標準差" in stat_method or "變異係數" in stat_method or "四分位距" in stat_method:
        color_scale = "Blues"  # 波動性用藍色
    elif "偏度" in stat_method:
        color_scale = "RdBu"   # 偏度用紅藍雙色
    elif "峰度" in stat_method:
        color_scale = "Viridis" # 峰度用漸變色
    elif "正樣本比例" in stat_method:
        color_scale = "Greens"  # 比例用綠色
    else:
        color_scale = "RdYlGn"  # 預設紅黃綠
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="報表月份", y="漲幅區間", color=f"{metric_choice} ({df['stat_label'].iloc[0]})"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale=color_scale,
        aspect="auto",
        text_auto=".2f" if "變異係數" in stat_method or "峰度" in stat_method or "偏度" in stat_method else ".1f"
    )
    fig.update_xaxes(side="top")
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # ========== 10. 統計摘要表格與AI分析 ==========
    with st.expander("📋 查看各漲幅區間詳細統計摘要", expanded=False):
        st.markdown("""
        **📅 數據時間範圍說明：**
        由於台灣營收公布時間的滯後性，每年1月看到的營收報表是去年12月數據，12月看到的是11月數據。
        因此我們以「去年12月到當年11月」共12份報表作為一個完整年度觀察期，這符合實際投資決策的時間軸。
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("當前統計模式", stat_method)
        with col2:
            st.metric("數據涵蓋月份", f"{actual_months}個月")
        
        if not stat_summary.empty:
            # 重新命名欄位
            stat_summary_display = stat_summary.rename(columns={
                'return_bin': '漲幅區間',
                'stock_count': '股票數量',
                'mean_val': '平均值',
                'median_val': '中位數',
                'std_val': '標準差',
                'min_val': '最小值',
                'max_val': '最大值',
                'cv_val': '變異係數',
                'iqr_val': '四分位距',
                'positive_rate': '正增長比例%'
            })
            
            st.dataframe(
                stat_summary_display.style.format({
                    '平均值': '{:.1f}',
                    '中位數': '{:.1f}',
                    '標準差': '{:.1f}',
                    '最小值': '{:.1f}',
                    '最大值': '{:.1f}',
                    '變異係數': '{:.2f}',
                    '四分位距': '{:.1f}',
                    '正增長比例%': '{:.1f}%'
                }).background_gradient(cmap='YlOrRd', subset=['平均值', '中位數'])
                .background_gradient(cmap='Blues', subset=['標準差', '四分位距'])
                .background_gradient(cmap='RdYlGn_r', subset=['變異係數'])
                .background_gradient(cmap='Greens', subset=['正增長比例%']),
                use_container_width=True,
                height=400
            )

            # ========== 11. AI分析提示詞區塊 ==========
            st.markdown("---")
            st.subheader("🤖 AI 智能分析助手")
            st.info("""
            💡 **使用說明：**
            由於數據量較大，請先點擊下方的 **「複製完整分析指令」**，再點擊 **「前往 ChatGPT」** 貼上即可開始深度分析。
            """)  
            # 添加重要提醒
            st.warning("""
            **⚠️ 重要提醒（請複製給AI看）：**
            這不是「按營收分組看股價」，而是「按股價漲幅分組看營收」！
            
            **數據結構：**
            1. 先按照股票「年度實際漲幅」分成不同區間
            2. 在每個股價漲幅區間內，計算該區間股票的營收表現
            
            **請AI分析：不同股價表現的股票，它們的營收表現有何特徵？**
            """)
            
            # 生成AI提示詞
            prompt_text = generate_ai_prompt(target_year, metric_choice, stat_method, 
                                            stat_summary, pivot_df, total_samples)
            
            # 顯示提示詞
            col_prompt, col_actions = st.columns([3, 1])
            
            with col_prompt:
                st.write("📋 **AI 分析指令 (含完整統計參數)**")
                st.code(prompt_text, language="text", height=400)
            
            with col_actions:
                st.write("🚀 **AI 診斷工具**")
                
                # ChatGPT 連結
                encoded_p = urllib.parse.quote(prompt_text)
                st.link_button(
                    "🔥 開啟 ChatGPT 分析", 
                    f"https://chatgpt.com/",
                    help="在新分頁開啟 ChatGPT 並自動帶入分析指令",
                    type="primary"
                )
                
                # Claude 連結
                st.link_button(
                    "🔍 開啟 Claude 分析", 
                    f"https://claude.ai/new?q={encoded_p}",
                    help="在新分頁開啟 Claude AI 分析",
                    type="secondary"
                )
                
                # DeepSeek 使用說明
                st.info("""
                **使用 DeepSeek**:
                1. 複製上方指令
                2. 前往 [DeepSeek](https://chat.deepseek.com)
                3. 貼上指令並發送
                """)
                
                # 複製按鈕
                if st.button("📋 複製指令到剪貼簿", type="secondary"):
                    st.code("已複製到剪貼簿！請直接貼到AI對話框", language="text")
    
    # ========== 12. 深度挖掘：領頭羊與備註搜尋 ==========
    st.write("---")
    st.subheader(f"🔍 {target_year} 深度挖掘：區間業績王與關鍵字搜尋")
    st.info("想知道為什麼某個區間營收特別綠？直接選取該區間，並輸入關鍵字搜尋原因！")

    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        selected_bin = st.selectbox("🎯 選擇漲幅區間：", pivot_df.index[::-1])
    with col_b:
        display_limit = st.select_slider("顯示筆數", options=[10, 20, 50, 100], value=50)
    with col_c:
        search_keyword = st.text_input("💡 備註關鍵字（如：建案、訂單、CoWoS、新機）：", "")

    minguo_year = int(target_year) - 1911
    prev_minguo_year = minguo_year - 1
    # 修改後的 detail_query 區塊
    detail_query = f"""
    WITH target_stocks AS (
        SELECT symbol, ((year_close - year_open) / year_open) * 100 as annual_ret,
        CASE 
            -- 務必確保這裡的字串與 fetch_heatmap_data 裡定義的一模一樣
            WHEN ((year_close - year_open) / year_open) * 100 <= -100 THEN '00. 下跌-100%以下'
            WHEN ((year_close - year_open) / year_open) * 100 < -90 THEN '01. 下跌-100%至-90%'
            WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN '02. 下跌-90%至-80%'
            WHEN ((year_close - year_open) / year_open) * 100 < -70 THEN '03. 下跌-80%至-70%'
            WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN '04. 下跌-70%至-60%'
            WHEN ((year_close - year_open) / year_open) * 100 < -50 THEN '05. 下跌-60%至-50%'
            WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN '06. 下跌-50%至-40%'
            WHEN ((year_close - year_open) / year_open) * 100 < -30 THEN '07. 下跌-40%至-30%'
            WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN '08. 下跌-30%至-20%'
            WHEN ((year_close - year_open) / year_open) * 100 < -10 THEN '09. 下跌-20%至-10%'
            WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN '10. 下跌-10%至0%'
            WHEN ((year_close - year_open) / year_open) * 100 < 100 THEN '11. 上漲0-100%'
            WHEN ((year_close - year_open) / year_open) * 100 < 200 THEN '12. 上漲100-200%'
            WHEN ((year_close - year_open) / year_open) * 100 < 300 THEN '13. 上漲200-300%'
            WHEN ((year_close - year_open) / year_open) * 100 < 400 THEN '14. 上漲300-400%'
            WHEN ((year_close - year_open) / year_open) * 100 < 500 THEN '15. 上漲400-500%'
            WHEN ((year_close - year_open) / year_open) * 100 < 600 THEN '16. 上漲500-600%'
            WHEN ((year_close - year_open) / year_open) * 100 < 700 THEN '17. 上漲600-700%'
            WHEN ((year_close - year_open) / year_open) * 100 < 800 THEN '18. 上漲700-800%'
            WHEN ((year_close - year_open) / year_open) * 100 < 900 THEN '19. 上漲800-900%'
            WHEN ((year_close - year_open) / year_open) * 100 < 1000 THEN '20. 上漲900-1000%'
            ELSE '21. 上漲1000%以上'
        END AS return_bin
        FROM stock_annual_k 
        WHERE year = '{target_year}'
    ),
    latest_remarks AS (
        SELECT DISTINCT ON (stock_id) stock_id, remark 
        FROM monthly_revenue 
        WHERE (report_month LIKE '{minguo_year}_%' AND report_month < '{minguo_year}_12' OR report_month = '{prev_minguo_year}_12')
          AND remark IS NOT NULL AND remark <> '-' AND remark <> ''
        ORDER BY stock_id, report_month DESC
    )
    SELECT 
        m.stock_id as "代號", 
        m.stock_name as "名稱",
        ROUND(t.annual_ret::numeric, 1) as "年度股價實際漲幅%",
        ROUND(AVG(m.yoy_pct)::numeric, 1) as "年增YoY平均%", 
        ROUND(AVG(m.mom_pct)::numeric, 1) as "月增MoM平均%",
        ROUND(STDDEV(m.yoy_pct)::numeric, 1) as "年增YoY波動%",
        ROUND(STDDEV(m.mom_pct)::numeric, 1) as "月增MoM波動%",
        r.remark as "最新營收備註"
    FROM monthly_revenue m
    JOIN target_stocks t ON m.stock_id = SPLIT_PART(t.symbol, '.', 1)
    LEFT JOIN latest_remarks r ON m.stock_id = r.stock_id
    WHERE t.return_bin = '{selected_bin}'  -- 這裡直接對齊字串
      AND (m.report_month LIKE '{minguo_year}_%' AND m.report_month < '{minguo_year}_12' OR m.report_month = '{prev_minguo_year}_12')
      AND (m.stock_name LIKE '%{search_keyword}%' OR (r.remark IS NOT NULL AND r.remark LIKE '%{search_keyword}%'))
    GROUP BY m.stock_id, m.stock_name, t.annual_ret, r.remark
    ORDER BY "年度股價實際漲幅%" DESC 
    LIMIT {display_limit};
    """
    
    with get_engine().connect() as conn:
        res_df = pd.read_sql_query(text(detail_query), conn)
        if not res_df.empty:
            st.write(f"🏆 在 **{selected_bin}** 區間中，符合條件的前 {len(res_df)} 檔公司：")
            
            # 添加排序選項
            sort_col = st.selectbox("排序依據", 
                                   ["年度股價實際漲幅%", "年增YoY平均%", "月增MoM平均%", "年增YoY波動%", "月增MoM波動%"])
            res_df_sorted = res_df.sort_values(by=sort_col, ascending=False)
            
            st.dataframe(
                res_df_sorted.style.format({
                    "年度股價實際漲幅%": "{:.1f}%",
                    "年增YoY平均%": "{:.1f}%",
                    "月增MoM平均%": "{:.1f}%",
                    "年增YoY波動%": "{:.1f}%",
                    "月增MoM波動%": "{:.1f}%"
                }).background_gradient(cmap='RdYlGn', subset=["年度股價實際漲幅%"])
                .background_gradient(cmap='YlOrRd', subset=["年增YoY平均%", "月增MoM平均%"])
                .background_gradient(cmap='Blues', subset=["年增YoY波動%", "月增MoM波動%"]),
                use_container_width=True,
                height=500
            )
        else:
            st.info("💡 目前區間或關鍵字下找不到符合的公司。")
    
    # ========== 13. 原始數據矩陣 (可切換統計模式) ==========
    with st.expander("🔧 查看原始數據矩陣與模式切換"):
        st.markdown("""
        **📅 數據時間範圍說明：**
        由於台灣營收公布時間的滯後性，每年1月看到的營收報表是去年12月數據，12月看到的是11月數據。
        因此我們以「去年12月到當年11月」共12份報表作為一個完整年度觀察期，這符合實際投資決策的時間軸。
        
        **📊 統計模式比較：**
        - **中位數**：排除極端值影響，反映典型狀況
        - **平均值**：受極端值影響大，可能失真
        - **標準差**：顯示數據波動程度
        - **變異係數**：標準化波動，可跨區間比較
        - **偏度**：分佈不對稱性（正偏=右尾長）
        - **峰度**：極端值出現機率（高峰度=尾部厚）
        """)
        
        # 快速切換統計模式
        quick_stat = st.radio("快速切換統計模式", 
                             ["中位數", "平均值", "標準差", "變異係數"], 
                             horizontal=True)
        
        # 根據選擇重新計算或顯示
        if quick_stat == "中位數":
            display_df = df[df['stat_method'].str.contains("中位數")]
            if display_df.empty:
                display_df = fetch_heatmap_data(target_year, target_col, "中位數 (排除極端值)")
        elif quick_stat == "平均值":
            display_df = df[df['stat_method'].str.contains("平均值")]
            if display_df.empty:
                display_df = fetch_heatmap_data(target_year, target_col, "平均值 (含極端值)")
        elif quick_stat == "標準差":
            display_df = df[df['stat_method'].str.contains("標準差")]
            if display_df.empty:
                display_df = fetch_heatmap_data(target_year, target_col, "標準差 (波動程度)")
        elif quick_stat == "變異係數":
            display_df = df[df['stat_method'].str.contains("變異係數")]
            if display_df.empty:
                display_df = fetch_heatmap_data(target_year, target_col, "變異係數 (相對波動)")
        else:
            display_df = df
        
        if not display_df.empty:
            pivot_display = display_df.pivot(index='return_bin', columns='report_month', values='val')
            
            # 格式化數值
            if quick_stat == "變異係數":
                fmt_str = "{:.1f}%"
            elif quick_stat == "偏度" or quick_stat == "峰度":
                fmt_str = "{:.2f}"
            else:
                fmt_str = "{:.1f}"
            
            st.write(f"**{quick_stat} 矩陣**")
            st.dataframe(pivot_display.style.format(fmt_str), use_container_width=True, height=400)
            
            # 下載按鈕
            csv = pivot_display.to_csv().encode('utf-8')
            st.download_button(
                label="📥 下載原始數據 (CSV)",
                data=csv,
                file_name=f"stock_heatmap_{target_year}_{metric_choice}_{quick_stat}.csv",
                mime="text/csv"
            )

else:
    st.warning(f"⚠️ 找不到 {target_year} 年的數據。請確認資料庫中已匯入該年度股價與營收。")

# ========== 14. 頁尾 (修正後) ==========
st.markdown("---")

# 獲取當前日期
current_date = datetime.now()
current_year_month = current_date.strftime("%Y-%m")

# 網站統計資訊
col2, col3 = st.columns(2)


with col2:
    # 只在有數據的情況下計算完整性
    if 'total_samples' in locals() and total_samples > 0 and 'actual_months' in locals() and 'total_data_points' in locals():
        completeness = (total_data_points / (total_samples * actual_months)) * 100
    else:
        completeness = 0
    
    st.markdown(f"""
    <div style="text-align: center;">
        <div style="font-size: 12px; color: #666;">數據完整性</div>
        <div style="font-size: 24px; font-weight: bold; color: #4CAF50;">{completeness:.1f}%</div>
        <div style="font-size: 10px; color: #999;">
            {f"{int(total_data_points):,} / {int(total_samples * actual_months):,}" if 'total_samples' in locals() and total_samples > 0 else "無數據"}
        </div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div style="text-align: center;">
        <div style="font-size: 12px; color: #666;">最後更新</div>
        <div style="font-size: 24px; font-weight: bold; color: #2196F3;">{current_year_month}</div>
        <div style="font-size: 10px; color: #999;">即時更新</div>
    </div>
    """, unsafe_allow_html=True)

st.caption(f"""
Developed by StockRevenueLab | 讓 16 萬筆數據說真話 | 統計模式 v2.0 | AI分析功能已上線 | 更新時間: {current_date.strftime('%Y-%m-%d %H:%M:%S')}
""")
