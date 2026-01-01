import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

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

# 網站計數器 (使用session state)
if 'visit_count' not in st.session_state:
    st.session_state.visit_count = 0
st.session_state.visit_count += 1

# 顯示計數器
st.sidebar.markdown(f"""
<div style="text-align: center; margin: 20px 0;">
    <div class="counter-badge">👁️ 今日訪問次數</div>
    <h2 style="color: #FF6B6B; margin: 5px 0;">{st.session_state.visit_count}</h2>
    <small style="color: #666;">感謝您的關注！</small>
</div>
""", unsafe_allow_html=True)

st.title("🧪 StockRevenueLab: 全時段飆股基因對帳單")
st.markdown("#### 透過 16 萬筆真實數據，揭開業績與股價漲幅的神秘面紗")

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

# ========== 3. 數據抓取引擎 (支援多種統計模式，包含細分下跌區間) ==========
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
    
    # 修改這裡：將下跌區間細分
    query = f"""
    WITH annual_bins AS (
        SELECT 
            symbol,
            ((year_close - year_open) / year_open) * 100 AS annual_return,
            CASE 
                -- 將下跌區間細分
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN '00. 下跌-80%以下'
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN '01. 下跌-60%至-80%'
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN '02. 下跌-40%至-60%'
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN '03. 下跌-20%至-40%'
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN '04. 下跌0%至-20%'
                -- 保持原來的正漲幅區間
                WHEN ((year_close - year_open) / year_open) * 100 >= 1000 THEN '11. 漲幅1000%+'
                ELSE LPAD(FLOOR(((year_close - year_open) / year_open) * 100)::text, 2, '0') || '. ' || 
                     (FLOOR(((year_close - year_open) / year_open) * 100))::text || '-' || 
                     (FLOOR(((year_close - year_open) / year_open) * 100) + 100)::text || '%'
            END AS return_bin,
            -- 為了分組排序，新增一個順序欄位
            CASE 
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN 0
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN 1
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN 2
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN 3
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN 4
                WHEN ((year_close - year_open) / year_open) * 100 >= 1000 THEN 20
                ELSE FLOOR(((year_close - year_open) / year_open) * 100) / 100 + 5
            END AS bin_order
        FROM stock_annual_k
        WHERE year = '{year}'
    ),
    monthly_stats AS (
        SELECT stock_id, report_month, {metric_col} 
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND LENGTH(report_month) <= 7)
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

# ========== 4. 統計摘要數據抓取 (修改版，包含細分下跌區間) ==========
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
                -- 將下跌區間細分
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN '00. 下跌-80%以下'
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN '01. 下跌-60%至-80%'
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN '02. 下跌-40%至-60%'
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN '03. 下跌-20%至-40%'
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN '04. 下跌0%至-20%'
                -- 保持原來的正漲幅區間
                WHEN ((year_close - year_open) / year_open) * 100 >= 1000 THEN '11. 漲幅1000%+'
                ELSE LPAD(FLOOR(((year_close - year_open) / year_open) * 100)::text, 2, '0') || '. ' || 
                     (FLOOR(((year_close - year_open) / year_open) * 100))::text || '-' || 
                     (FLOOR(((year_close - year_open) / year_open) * 100) + 100)::text || '%'
            END AS return_bin,
            -- 為了分組排序，新增一個順序欄位
            CASE 
                WHEN ((year_close - year_open) / year_open) * 100 < -80 THEN 0
                WHEN ((year_close - year_open) / year_open) * 100 < -60 THEN 1
                WHEN ((year_close - year_open) / year_open) * 100 < -40 THEN 2
                WHEN ((year_close - year_open) / year_open) * 100 < -20 THEN 3
                WHEN ((year_close - year_open) / year_open) * 100 < 0 THEN 4
                WHEN ((year_close - year_open) / year_open) * 100 >= 1000 THEN 20
                ELSE FLOOR(((year_close - year_open) / year_open) * 100) / 100 + 5
            END AS bin_order
        FROM stock_annual_k
        WHERE year = '{year}'
    ),
    monthly_stats AS (
        SELECT stock_id, report_month, {metric_col} 
        FROM monthly_revenue
        WHERE report_month = '{prev_minguo_year}_12'
           OR (report_month LIKE '{minguo_year}_%' AND LENGTH(report_month) <= 7)
    )
    SELECT 
        b.return_bin,
        b.bin_order,
        COUNT(DISTINCT b.symbol) as stock_count,
        AVG(b.annual_return) as avg_annual_return,  -- 新增：該區間的平均股價漲幅
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

# ========== 5. AI分析提示詞生成 (修改版，包含細分下跌區間) ==========
def generate_ai_prompt(target_year, metric_choice, stat_method, stat_summary, pivot_df, total_samples):
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 找出最慘的下跌區間
    worst_bins = stat_summary[stat_summary['return_bin'].str.contains('下跌')].copy()
    if not worst_bins.empty:
        worst_bin = worst_bins.loc[worst_bins['avg_annual_return'].idxmin()]
        worst_bin_name = worst_bin['return_bin']
        worst_avg_return = worst_bin['avg_annual_return']
        worst_pos_rate = worst_bin['positive_rate']
    else:
        worst_bin_name = "無資料"
        worst_avg_return = 0
        worst_pos_rate = 0
    
    # 找出最好的上漲區間
    best_bins = stat_summary[~stat_summary['return_bin'].str.contains('下跌')].copy()
    if not best_bins.empty:
        best_bin = best_bins.loc[best_bins['avg_annual_return'].idxmax()]
        best_bin_name = best_bin['return_bin']
        best_avg_return = best_bin['avg_annual_return']
        best_pos_rate = best_bin['positive_rate']
    else:
        best_bin_name = "無資料"
        best_avg_return = 0
        best_pos_rate = 0
    
    # 簡化統計摘要表格
    summary_table = ""
    for _, row in stat_summary.iterrows():
        bin_name = row['return_bin']
        # 簡化顯示
        if "下跌" in bin_name:
            simple_name = bin_name.split(' ')[1]  # 取出後面的部分
        else:
            simple_name = bin_name.split(' ')[1] if len(bin_name.split(' ')) > 1 else bin_name
        
        summary_table += f"| {simple_name} | {row['stock_count']}檔 | {row['avg_annual_return']:.1f}% | {row['mean_val']:.1f}% | {row['median_val']:.1f}% | {row['positive_rate']:.1f}% |\n"
    
    prompt = f"""# 台股營收與股價關聯分析報告 (細分下跌區間版)
分析時間: {current_date}
分析年度: {target_year}年
成長指標: {metric_choice}
統計方法: {stat_method}
總樣本數: {total_samples:,}檔

## 🎯 重要數據說明
**這是「按股價漲幅分組看營收表現」，且下跌區間已細分為5個等級！**

### 數據結構說明：
1. **分組依據**：先按照股票「年度實際漲幅」分成不同區間
   - 下跌股票細分為：-80%以下、-60%至-80%、-40%至-60%、-20%至-40%、0%至-20%
   - 上漲股票保持原有分組：0-100%、100-200%、...、1000%+

2. **觀察指標**：在每個股價漲幅區間內，計算該區間股票的營收表現

### 關鍵發現：
1. **最慘的下跌區間**: {worst_bin_name} (平均股價漲幅{worst_avg_return:.1f}%，營收正增長比例{worst_pos_rate:.1f}%)
2. **最好的上漲區間**: {best_bin_name} (平均股價漲幅{best_avg_return:.1f}%，營收正增長比例{best_pos_rate:.1f}%)

## 數據摘要表
| 股價漲幅區間 | 股票數量 | 平均股價漲幅 | 營收平均成長 | 營收中位數成長 | 正增長比例 |
|--------------|----------|--------------|--------------|----------------|------------|
{summary_table}

## 🎯 分析任務（請特別關注下跌區間的細分分析）
請擔任專業量化分析師，根據以上細分數據回答：

### 1. 下跌股票的深度分析
- **不同跌幅等級**的股票，營收表現有何差異？
  - 跌80%以上的股票 vs 跌20%以內的股票，營收表現差多少？
- **極度弱勢股**（跌60%以上）的營收特徵是什麼？有沒有「跌越多，營收越差」的趨勢？
- **輕微下跌股**（跌20%以內）的營收表現如何？是不是「營收還不錯，但股價小跌」？

### 2. 股價漲幅 vs 營收表現的完整圖譜
- 從「極度弱勢」到「超級強勢」，營收表現呈現什麼樣的變化曲線？
- 有沒有**轉折點**？例如：某個漲幅區間開始，營收表現明顯改善？
- **異常現象**：有沒有「股價跌很深但營收不錯」或「股價大漲但營收普通」的區間？

### 3. 投資策略啟示
- **抄底策略**：根據數據，哪種跌幅的股票最有「抄底價值」？
- **風險控管**：哪些下跌等級的股票應該絕對避免？
- **強勢股篩選**：要找到潛在飆股，應該關注哪些營收特徵？

### 4. 統計深度分析
- 各區間的**營收波動率**（標準差）有什麼規律？
- **正增長比例**的變化：股價表現越好的區間，營收正增長比例是否越高？
- **極端值分析**：最賺錢和最賠錢的區間，營收分佈有什麼特徵？

## 📊 分析框架建議
請按照以下順序分析：
1. **下跌階梯分析**：從最深跌幅到最淺跌幅，逐一分析營收表現
2. **整體趨勢分析**：繪製「股價漲幅 vs 營收表現」的完整曲線
3. **關鍵轉折點**：找出營收表現發生質變的股價區間
4. **投資應用**：基於細分數據提出更精準的投資策略

## ⚠️ 重要提醒
1. **下跌已細分**：現在有5個下跌等級，請分別分析
2. **樣本數注意**：有些下跌區間可能股票很少，分析時請注意統計顯著性
3. **避免倖存者偏差**：極度弱勢股可能已下市，這是倖存者樣本
4. **時間滯後性**：{target_year}年1月看到的是前一年12月營收

## 📝 回答要求
1. 用中文回答，結構清晰
2. 特別關注**下跌區間的細分比較**
3. 每個觀點都要有具體的數據支持
4. 提供實際可行的分級投資建議

現在，請開始您的專業分析：
"""
    
    return prompt

# ========== 在深度挖掘部分也需要修改選擇框 ==========
# 在深度挖掘區間選擇部分，修改選擇框的選項生成方式
# 這是修改第12部分（深度挖掘）中的selected_bin選擇框

# 修改前（大約在程式碼第320行附近）：
# selected_bin = st.selectbox("🎯 選擇漲幅區間：", pivot_df.index[::-1])

# 修改後：
# 我們需要確保pivot_df的index按照正確的順序排列
# 在熱力圖部分修改pivot_df的生成：

# ========== 6. 側邊欄 UI ==========
st.sidebar.header("🔬 研究條件篩選")
target_year = st.sidebar.selectbox("分析年度", [str(y) for y in range(2025, 2019, -1)], index=1)
metric_choice = st.sidebar.radio("成長指標", ["年增率 (YoY)", "月增率 (MoM)"], help="YoY看長期趨勢，MoM看短期爆發")

# 進階統計模式選項
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

stat_method = st.sidebar.selectbox("統計指標模式", stat_methods, index=0, 
                                   help="選擇不同的統計量來觀察數據特徵")

target_col = "yoy_pct" if metric_choice == "年增率 (YoY)" else "mom_pct"

# ========== 7. 儀表板主視圖 ==========
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
            # ========== 11. AI分析提示詞區塊 ==========
            st.markdown("---")
            st.subheader("🤖 AI 智能分析助手")
            
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
                    f"https://chatgpt.com/?q={encoded_p}",
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

    # 強大的 SQL：整合漲幅、平均營收與最新備註
    detail_query = f"""
    WITH target_stocks AS (
        SELECT symbol, ((year_close - year_open) / year_open) * 100 as annual_ret 
        FROM stock_annual_k 
        WHERE year = '{target_year}' AND (CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END) = '{selected_bin}'
    ),
    latest_remarks AS (
        -- 取得該年度最後一個有備註的月份資料
        SELECT DISTINCT ON (stock_id) stock_id, remark 
        FROM monthly_revenue 
        WHERE (report_month LIKE '{minguo_year}_%' OR report_month = '{prev_minguo_year}_12')
          AND remark IS NOT NULL AND remark <> '-' AND remark <> ''
        ORDER BY stock_id, report_month DESC
    )
    SELECT 
        m.stock_id as "代號", 
        m.stock_name as "名稱",
        ROUND(t.annual_ret::numeric, 1) as "年度實際漲幅%",
        ROUND(AVG(m.yoy_pct)::numeric, 1) as "年增平均%", 
        ROUND(AVG(m.mom_pct)::numeric, 1) as "月增平均%",
        ROUND(STDDEV(m.yoy_pct)::numeric, 1) as "年增波動%",
        ROUND(STDDEV(m.mom_pct)::numeric, 1) as "月增波動%",
        r.remark as "最新營收備註"
    FROM monthly_revenue m
    JOIN target_stocks t ON m.stock_id = SPLIT_PART(t.symbol, '.', 1)
    LEFT JOIN latest_remarks r ON m.stock_id = r.stock_id
    WHERE (m.report_month LIKE '{minguo_year}_%' OR m.report_month = '{prev_minguo_year}_12')
      AND (m.stock_name LIKE '%{search_keyword}%' OR m.remark LIKE '%{search_keyword}%')
    GROUP BY m.stock_id, m.stock_name, t.annual_ret, r.remark
    ORDER BY "年度實際漲幅%" DESC 
    LIMIT {display_limit};
    """
    
    with get_engine().connect() as conn:
        res_df = pd.read_sql_query(text(detail_query), conn)
        if not res_df.empty:
            st.write(f"🏆 在 **{selected_bin}** 區間中，符合條件的前 {len(res_df)} 檔公司：")
            
            # 添加排序選項
            sort_col = st.selectbox("排序依據", 
                                   ["年度實際漲幅%", "年增平均%", "月增平均%", "年增波動%", "月增波動%"])
            res_df_sorted = res_df.sort_values(by=sort_col, ascending=False)
            
            st.dataframe(
                res_df_sorted.style.format({
                    "年度實際漲幅%": "{:.1f}%",
                    "年增平均%": "{:.1f}%",
                    "月增平均%": "{:.1f}%",
                    "年增波動%": "{:.1f}%",
                    "月增波動%": "{:.1f}%"
                }).background_gradient(cmap='RdYlGn', subset=["年度實際漲幅%"])
                .background_gradient(cmap='YlOrRd', subset=["年增平均%", "月增平均%"])
                .background_gradient(cmap='Blues', subset=["年增波動%", "月增波動%"]),
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
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"""
    <div style="text-align: center;">
        <div style="font-size: 12px; color: #666;">網站訪問次數</div>
        <div style="font-size: 24px; font-weight: bold; color: #FF6B6B;">{st.session_state.visit_count}</div>
        <div style="font-size: 10px; color: #999;">本次會話</div>
    </div>
    """, unsafe_allow_html=True)

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
