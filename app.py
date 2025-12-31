import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
    </style>
    """, unsafe_allow_html=True)

# 側邊欄導引
st.sidebar.success("💡 想要看『勝率分析』？請點選左側選單的 probability 頁面！")

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

# ========== 3. 數據抓取引擎 (支援多種統計模式) ==========
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
        # 變異係數 = 標準差/平均值 * 100%
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
    
    # 這裡的邏輯：抓取前一年 12 月 + 當年 1~12 月，共 13 份報表
    query = f"""
    WITH annual_bins AS (
        SELECT 
            symbol,
            ((year_close - year_open) / year_open) * 100 AS annual_return,
            CASE 
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END AS return_bin
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
        m.report_month,
        {agg_func} as val,
        COUNT(DISTINCT b.symbol) as stock_count,
        COUNT(m.{metric_col}) as data_points
    FROM annual_bins b
    JOIN monthly_stats m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    WHERE m.{metric_col} IS NOT NULL
    GROUP BY b.return_bin, m.report_month
    ORDER BY b.return_bin, m.report_month;
    """
    
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)
        df['stat_method'] = stat_method
        df['stat_label'] = stat_label
        return df

# ========== 4. 統計摘要數據抓取 ==========
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
                WHEN (year_close - year_open) / year_open < 0 THEN '00. 下跌'
                WHEN (year_close - year_open) / year_open >= 10 THEN '11. 1000%+'
                ELSE LPAD(FLOOR((year_close - year_open) / year_open)::text, 2, '0') || '. ' || 
                     (FLOOR((year_close - year_open) / year_open)*100)::text || '-' || 
                     ((FLOOR((year_close - year_open) / year_open)+1)*100)::text || '%'
            END AS return_bin
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
        COUNT(DISTINCT b.symbol) as stock_count,
        ROUND(AVG(m.{metric_col})::numeric, 2) as mean_val,
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY m.{metric_col})::numeric, 2) as median_val,
        ROUND(STDDEV(m.{metric_col})::numeric, 2) as std_val,
        ROUND(MIN(m.{metric_col})::numeric, 2) as min_val,
        ROUND(MAX(m.{metric_col})::numeric, 2) as max_val,
        ROUND((STDDEV(m.{metric_col}) / NULLIF(AVG(m.{metric_col}), 0))::numeric, 2) as cv_val,
        ROUND((percentile_cont(0.75) WITHIN GROUP (ORDER BY m.{metric_col}) - 
               percentile_cont(0.25) WITHIN GROUP (ORDER BY m.{metric_col}))::numeric, 2) as iqr_val
    FROM annual_bins b
    JOIN monthly_stats m ON SPLIT_PART(b.symbol, '.', 1) = m.stock_id
    WHERE m.{metric_col} IS NOT NULL
    GROUP BY b.return_bin
    ORDER BY b.return_bin;
    """
    
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 5. 側邊欄 UI ==========
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

# ========== 6. 儀表板主視圖 ==========
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
    
    # ========== 7. 統計摘要卡片 ==========
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
    
    # ========== 8. 熱力圖 ==========
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
    
    # ========== 9. 統計摘要表格 ==========
    with st.expander("📋 查看各漲幅區間詳細統計摘要"):
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
                'iqr_val': '四分位距'
            })
            
            st.dataframe(
                stat_summary_display.style.format({
                    '平均值': '{:.1f}',
                    '中位數': '{:.1f}',
                    '標準差': '{:.1f}',
                    '最小值': '{:.1f}',
                    '最大值': '{:.1f}',
                    '變異係數': '{:.2f}',
                    '四分位距': '{:.1f}'
                }).background_gradient(cmap='YlOrRd', subset=['平均值', '中位數'])
                .background_gradient(cmap='Blues', subset=['標準差', '四分位距'])
                .background_gradient(cmap='RdYlGn_r', subset=['變異係數']),
                use_container_width=True
            )
    
    # ========== 10. 深度挖掘：領頭羊與備註搜尋 ==========
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
    
    # ========== 11. 原始數據矩陣 (可切換統計模式) ==========
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

st.markdown("---")
st.caption("Developed by StockRevenueLab | 讓 16 萬筆數據說真話 | 統計模式 v2.0")
