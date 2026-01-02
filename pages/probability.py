import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="機率研究室 2.0 | StockRevenueLab", layout="wide")

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
        st.error("❌ 資料庫連線失敗，請檢查 Secrets 設定")
        st.stop()

# ========== 3. 新增：獲取前後年度比較數據 ==========
@st.cache_data(ttl=3600)
def fetch_multi_year_data(stock_list, target_year, price_field="year_close"):
    """獲取指定股票在前後年度的表現"""
    if not stock_list:
        return pd.DataFrame()
    
    engine = get_engine()
    stock_ids = ','.join([f"'{id}'" for id in stock_list])
    
    query = f"""
    WITH years_data AS (
        SELECT 
            SPLIT_PART(symbol, '.', 1) as stock_id,
            year,
            (({price_field} - year_open) / year_open) * 100 as annual_return
        FROM stock_annual_k
        WHERE SPLIT_PART(symbol, '.', 1) IN ({stock_ids})
            AND year::integer BETWEEN {int(target_year)-2} AND {int(target_year)+1}
    )
    SELECT * FROM years_data;
    """
    
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. 修正：數據抓取引擎 (使用PERCENTILE_CONT計算中位數) ==========
@st.cache_data(ttl=3600)
def fetch_prob_data(year, metric_col, low, high, price_field="year_close"):
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits 
        FROM monthly_revenue 
        WHERE (
            report_month = '{prev_minguo_year}_12' 
            OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
        )
        AND {metric_col} >= {low} AND {metric_col} < {high}
        GROUP BY stock_id
    ),
    perf_table AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, 
                (({price_field} - year_open) / year_open)*100 as ret
        FROM stock_annual_k WHERE year = '{year}'
    ),
    joined_data AS (
        SELECT h.hits, p.ret
        FROM hit_table h 
        JOIN perf_table p ON h.stock_id = p.stock_id
    )
    SELECT 
        hits as "爆發次數", 
        COUNT(*) as "股票檔數",
        ROUND(AVG(ret)::numeric, 1) as "平均年度漲幅%",
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ret)::numeric, 1) as "中位數漲幅%",
        ROUND((COUNT(*) FILTER (WHERE ret > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(>20%)",
        ROUND((COUNT(*) FILTER (WHERE ret > 100) * 100.0 / COUNT(*))::numeric, 1) as "翻倍率(>100%)",
        ROUND(MIN(ret)::numeric, 1) as "最低漲幅%",
        ROUND(MAX(ret)::numeric, 1) as "最高漲幅%",
        ROUND(STDDEV(ret)::numeric, 1) as "標準差%"
    FROM joined_data
    GROUP BY hits 
    ORDER BY hits DESC;
    """
    
    try:
        with engine.connect() as conn:
            return pd.read_sql_query(text(query), conn)
    except Exception as e:
        st.error(f"❌ 數據查詢失敗: {str(e)}")
        # 如果中位數計算失敗，嘗試使用替代方法
        st.warning("⚠️ 嘗試使用替代查詢...")
        return fetch_prob_data_alt(year, metric_col, low, high, price_field)

# ========== 4.1 替代方案：如果PERCENTILE_CONT不可用 ==========
def fetch_prob_data_alt(year, metric_col, low, high, price_field="year_close"):
    """替代方案：使用Python計算中位數"""
    engine = get_engine()
    minguo_year = int(year) - 1911
    prev_minguo_year = minguo_year - 1
    
    # 先獲取原始數據
    query = f"""
    WITH hit_table AS (
        SELECT stock_id, COUNT(*) as hits 
        FROM monthly_revenue 
        WHERE (
            report_month = '{prev_minguo_year}_12' 
            OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
        )
        AND {metric_col} >= {low} AND {metric_col} < {high}
        GROUP BY stock_id
    ),
    perf_table AS (
        SELECT SPLIT_PART(symbol, '.', 1) as stock_id, 
                (({price_field} - year_open) / year_open)*100 as ret
        FROM stock_annual_k WHERE year = '{year}'
    )
    SELECT h.hits, p.ret
    FROM hit_table h 
    JOIN perf_table p ON h.stock_id = p.stock_id
    """
    
    with engine.connect() as conn:
        raw_df = pd.read_sql_query(text(query), conn)
    
    if raw_df.empty:
        return pd.DataFrame()
    
    # 使用Python計算統計量
    result = []
    for hits, group in raw_df.groupby('hits'):
        ret_series = group['ret']
        result.append({
            "爆發次數": hits,
            "股票檔數": len(group),
            "平均年度漲幅%": round(ret_series.mean(), 1),
            "中位數漲幅%": round(ret_series.median(), 1),
            "勝率(>20%)": round((ret_series > 20).sum() / len(group) * 100, 1),
            "翻倍率(>100%)": round((ret_series > 100).sum() / len(group) * 100, 1),
            "最低漲幅%": round(ret_series.min(), 1),
            "最高漲幅%": round(ret_series.max(), 1),
            "標準差%": round(ret_series.std(), 1) if len(group) > 1 else 0
        })
    
    return pd.DataFrame(result).sort_values("爆發次數", ascending=False)

# ========== 5. 新增：計算期望值指標 ==========
def calculate_expected_value(df):
    """計算期望值相關指標"""
    if df.empty:
        return pd.DataFrame()
    
    results = []
    for _, row in df.iterrows():
        hits = row["爆發次數"]
        count = row["股票檔數"]
        avg_return = row["平均年度漲幅%"]
        median_return = row["中位數漲幅%"]
        win_rate = row["勝率(>20%)"] / 100
        
        # 簡單期望值 = 平均報酬 * 股票檔數（權重）
        expected_value = avg_return * count
        
        # 風險調整後期望值（考慮標準差）
        std_dev = max(row.get("標準差%", 1), 1)
        risk_adjusted = avg_return / std_dev if std_dev > 0 else 0
        
        # 成功率調整期望值
        success_adjusted = avg_return * win_rate
        
        # 平均數與中位數差異
        mean_median_diff = avg_return - median_return
        
        results.append({
            "爆發次數": hits,
            "股票檔數": count,
            "平均年度漲幅%": avg_return,
            "中位數漲幅%": median_return,
            "平均-中位差": round(mean_median_diff, 1),
            "勝率(>20%)": row["勝率(>20%)"],
            "翻倍率(>100%)": row["翻倍率(>100%)"],
            "期望值分數": round(expected_value / 100, 2) if expected_value != 0 else 0,
            "風險調整分數": round(risk_adjusted, 2),
            "成功率分數": round(success_adjusted, 2),
            "綜合評分": round((expected_value/100 + risk_adjusted + success_adjusted) / 3, 2) if expected_value != 0 else 0
        })
    
    return pd.DataFrame(results)

# ========== 6. UI 介面設計 ==========
st.title("🎲 營收爆發與年度報酬機率分析 2.0")
st.markdown("""
**研究目標**：分析月增率(MoM)或年增率(YoY)出現特定次數與股價年度報酬的關係

**研究期間**：前一年12月 ~ 目標年11月（共12份月營收報告）
**股價計算**：目標年度全年漲跌幅（年K線開盤到收盤）
""")

with st.sidebar:
    st.header("🔬 研究參數設定")
    target_year = st.sidebar.selectbox("目標年度", [str(y) for y in range(2025, 2019, -1)], index=1)
    
    study_metric = st.selectbox(
        "研究指標",
        ["yoy_pct", "mom_pct"],
        format_func=lambda x: "年增率(YoY)" if x == "yoy_pct" else "月增率(MoM)",
        index=0,
        help="年增率：與去年同期比較；月增率：與上月比較"
    )
    
    metric_name = "年增率(YoY)" if study_metric == "yoy_pct" else "月增率(MoM)"
    
    # 新增：股價計算方式選單
    st.markdown("---")
    price_calc = st.radio(
        "📈 股價計算方式",
        ["收盤價 (實戰版)", "最高價 (極限版)"],
        help="收盤價：實際年度報酬 | 最高價：年度最大潛在漲幅",
        index=0
    )
    
    # 根據選擇決定 SQL 中的價格欄位
    if price_calc == "收盤價 (實戰版)":
        price_field = "year_close"
        price_label = "收盤價"
        st.info("使用年度收盤價計算，代表實際可實現的報酬")
    else:
        price_field = "year_high"  
        price_label = "最高價"
        st.warning("使用年度最高價計算，代表理論最大潛力漲幅")
    
    growth_range = st.select_slider(
        f"設定{metric_name}爆發區間 (%)", 
        options=[-50, 0, 20, 50, 100, 150, 200, 300, 500, 1000], 
        value=(100, 1000)
    )
    
    st.markdown("---")
    st.markdown("### 📊 分析選項")
    show_advanced = st.checkbox("顯示進階分析", value=True)
    show_multi_year = st.checkbox("顯示前後年度比較", value=False)  # 預設關閉，避免查詢錯誤
    show_expected_value = st.checkbox("計算期望值評分", value=True)

# ========== 7. 計算民國年份（全域變數） ==========
# 在主要程式區域計算民國年份，以便後續查詢使用
minguo_year = int(target_year) - 1911
prev_minguo_year = minguo_year - 1

# 獲取主要數據
df_prob = fetch_prob_data(target_year, study_metric, growth_range[0], growth_range[1], price_field)

if not df_prob.empty:
    # ========== A. 核心數據顯示區 ==========
    st.subheader(f"📊 {target_year}年：{metric_name}達標次數 vs {price_label}年度報酬統計")
    st.caption(f"計算方式：{price_calc} | 使用{price_label}計算年度漲幅")
    
    # 顯示基本統計
    total_stocks = df_prob["股票檔數"].sum()
    st.metric("總樣本股票數", f"{total_stocks} 檔")
    
    # 顯示原始表格
    display_cols = ["爆發次數", "股票檔數", "平均年度漲幅%", "中位數漲幅%", 
                    "勝率(>20%)", "翻倍率(>100%)", "標準差%"]
    
    # 確保所有需要的欄位都存在
    available_cols = [col for col in display_cols if col in df_prob.columns]
    
    st.dataframe(df_prob[available_cols].style.format({
        "平均年度漲幅%": "{:.1f}%",
        "中位數漲幅%": "{:.1f}%",
        "勝率(>20%)": "{:.1f}%", 
        "翻倍率(>100%)": "{:.1f}%",
        "標準差%": "{:.1f}%"
    }), use_container_width=True)
    
    # ========== B. 視覺化分析 ==========
    if show_advanced and len(df_prob) > 1:
        col1, col2 = st.columns(2)
        
        with col1:
            # 爆發次數 vs 平均報酬與中位數
            fig1 = go.Figure()
            fig1.add_trace(go.Bar(
                x=df_prob["爆發次數"],
                y=df_prob["平均年度漲幅%"],
                name='平均年度漲幅%',
                marker_color='lightblue'
            ))
            fig1.add_trace(go.Scatter(
                x=df_prob["爆發次數"],
                y=df_prob["中位數漲幅%"],
                name='中位數漲幅%',
                mode='lines+markers',
                line=dict(color='darkblue', width=2)
            ))
            fig1.update_layout(
                title=f"{metric_name}爆發次數 vs {price_label}年度表現",
                yaxis_title='漲幅 %',
                height=400
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # 平均數 vs 中位數差異
            if '平均年度漲幅%' in df_prob.columns and '中位數漲幅%' in df_prob.columns:
                df_prob['平均-中位差'] = df_prob['平均年度漲幅%'] - df_prob['中位數漲幅%']
                
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(
                    x=df_prob["爆發次數"],
                    y=df_prob["平均-中位差"],
                    name='平均-中位數差異',
                    marker_color='coral',
                    text=df_prob["平均-中位差"].round(1),
                    textposition='outside'
                ))
                fig2.update_layout(
                    title="平均數與中位數差異分析",
                    yaxis_title="差異 %",
                    height=400
                )
                st.plotly_chart(fig2, use_container_width=True)
                
                # 解釋差異
                pos_diff_count = (df_prob['平均-中位差'] > 0).sum()
                pos_diff_percent = pos_diff_count / len(df_prob) * 100
                
                st.info(f"""
                **平均數與中位數差異分析（{price_calc}）**：
                - {pos_diff_count}/{len(df_prob)} 個區間({pos_diff_percent:.1f}%) 平均數 > 中位數
                - **表示多數區間存在右偏分佈**：少數股票漲幅極高，拉高了平均值
                - 當差異越大，代表該爆發次數區間的**右尾效應**越明顯
                """)
    
    # ========== C. 期望值分析 ==========
    if show_expected_value and len(df_prob) > 1:
        st.subheader("🎯 期望值與綜合評分分析")
        
        # 計算期望值指標
        expected_df = calculate_expected_value(df_prob)
        
        if not expected_df.empty:
            # 找出最佳區間
            if '綜合評分' in expected_df.columns and not expected_df.empty:
                best_idx = expected_df["綜合評分"].idxmax()
                best_hits = expected_df.loc[best_idx, "爆發次數"]
                best_score = expected_df.loc[best_idx, "綜合評分"]
                
                col_a, col_b, col_c = st.columns(3)
                col_a.metric("最佳爆發次數", f"{best_hits} 次")
                col_b.metric("綜合評分", f"{best_score:.2f}")
                col_c.metric("該區間樣本數", f"{int(expected_df.loc[best_idx, '股票檔數'])} 檔")
                
                # 顯示期望值表格
                display_expected_cols = ["爆發次數", "股票檔數", "平均年度漲幅%", "中位數漲幅%", 
                                        "平均-中位差", "勝率(>20%)", "翻倍率(>100%)", "綜合評分"]
                
                available_expected_cols = [col for col in display_expected_cols if col in expected_df.columns]
                
                st.dataframe(expected_df[available_expected_cols].style.format({
                    "平均年度漲幅%": "{:.1f}",
                    "中位數漲幅%": "{:.1f}",
                    "平均-中位差": "{:.1f}",
                    "綜合評分": "{:.2f}"
                }).highlight_max(subset=["綜合評分"], color='lightgreen'), 
                use_container_width=True)
    
    # ========== D. AI 分析助手區 ==========
    st.markdown("---")
    st.subheader("🤖 AI 深度策略診斷")
    
    # 建構Markdown表格
    table_data = df_prob.head(20)  # 限制行數避免過長
    if not table_data.empty:
        header = "| " + " | ".join(table_data.columns) + " |"
        sep = "| " + " | ".join(["---"] * len(table_data.columns)) + " |"
        rows = ["| " + " | ".join(map(str, row.values)) + " |" for _, row in table_data.iterrows()]
        table_md = "\n".join([header, sep] + rows)
    else:
        table_md = "無數據"
    
    # 建構完整的提示詞
    prompt_text = f"""
# {target_year}年台股營收爆發次數與年度報酬關聯分析

## 研究設定
- **分析年度**: {target_year}年
- **研究指標**: {metric_name}
- **股價計算方式**: {price_calc} (使用{price_label}計算漲幅)
- **爆發門檻**: {growth_range[0]}% 至 {growth_range[1]}%
- **研究期間**: 前一年12月到{target_year}年11月（12個月份）
- **股價計算**: {target_year}年度漲跌幅（年K線，使用{price_label}計算）

## 價格計算方式說明
- **{price_calc}**: {price_label}漲幅 = (({price_label} - 年開盤價) / 年開盤價) × 100%
- 如果是「最高價 (極限版)」：代表年度最大潛在漲幅（理論最大值）
- 如果是「收盤價 (實戰版)」：代表實際年度報酬（可實現報酬）

## 統計數據摘要
{table_md}

## 分析問題
請以專業量化分析師的角度，針對以上數據回答以下問題：

### 1. 計算方式影響分析
- **{price_calc}的特性**：使用{price_label}計算有什麼優點和缺點？
- **實務意義**：如果是「最高價」計算，代表什麼意義？如果是「收盤價」計算，又代表什麼意義？

### 2. 相關性分析
- 「爆發次數」與「平均年度漲幅」、「中位數漲幅」、「勝率(>20%)」之間是否存在正相關？
- 從哪些數據點可以支持你的結論？

### 3. 平均數與中位數差異分析
- 哪些爆發次數區間的「平均-中位數」差異最大？這代表什麼意義？
- 右尾效應（平均>中位）最明顯的區間是哪個？對投資策略有何啟示？

### 4. 投資策略建議（考慮計算方式）
- 根據期望值（兼顧樣本數與漲幅），哪個「爆發次數區間」是最佳投資標的？
- 對於不同風險偏好的投資者，你會建議關注哪個爆發次數區間？
- **計算方式影響**：{price_calc}的結果應該如何應用在實際投資中？

### 5. 實務操作建議
- 投資人應該如何利用這個統計規律來制定交易策略？
- 需要搭配哪些其他指標或條件來提高勝率？
"""
    
    col_prompt, col_link = st.columns([2, 1])
    with col_prompt:
        st.write("📋 **AI分析指令（已包含完整參數）**")
        st.code(prompt_text, language="text", height=400)
    
    with col_link:
        st.write("🚀 **AI分析平台**")
        encoded_prompt = urllib.parse.quote(prompt_text)
        
        st.link_button(
            "🔥 ChatGPT 分析", 
            f"https://chatgpt.com/?q={encoded_prompt}",
            help="自動帶入完整分析指令"
        )
        
        st.link_button(
            "🔍 DeepSeek 分析", 
            "https://chat.deepseek.com/",
            help="請複製上方指令貼上使用"
        )
    
    # ========== E. 前後年度比較分析 ==========
    if show_multi_year:
        st.markdown("---")
        st.subheader("📈 前後年度表現比較分析")
        
        st.warning("⚠️ 此功能需要查詢多年度數據，可能會影響效能。")
        
        # 獲取詳細股票名單
        list_query = f"""
        WITH hit_table AS (
            SELECT stock_id, COUNT(*) as hits 
            FROM monthly_revenue 
            WHERE (
                report_month = '{prev_minguo_year}_12' 
                OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
            )
            AND {study_metric} >= {growth_range[0]} AND {study_metric} < {growth_range[1]}
            GROUP BY stock_id
        )
        SELECT h.stock_id as stock_id, h.hits as hits
        FROM hit_table h
        LIMIT 100  -- 限制數量避免查詢過大
        """
        
        try:
            with get_engine().connect() as conn:
                stock_list_df = pd.read_sql_query(text(list_query), conn)
            
            if not stock_list_df.empty:
                # 獲取前後年度數據
                multi_year_df = fetch_multi_year_data(stock_list_df['stock_id'].tolist(), target_year, price_field)
                
                if not multi_year_df.empty:
                    # 按爆發次數分組分析
                    merged_df = pd.merge(stock_list_df, multi_year_df, on='stock_id')
                    
                    # 計算各爆發次數的前後年度表現
                    year_stats = []
                    
                    for hits, group in merged_df.groupby('hits'):
                        for year, year_group in group.groupby('year'):
                            year_stats.append({
                                '爆發次數': hits,
                                '年度': year,
                                '平均報酬%': round(year_group['annual_return'].mean(), 1),
                                '中位數報酬%': round(year_group['annual_return'].median(), 1),
                                '樣本數': len(year_group)
                            })
                    
                    year_stats_df = pd.DataFrame(year_stats)
                    
                    if not year_stats_df.empty:
                        # 轉換為寬格式
                        pivot_mean = year_stats_df.pivot_table(
                            index='爆發次數', 
                            columns='年度', 
                            values='平均報酬%',
                            aggfunc='first'
                        ).round(1)
                        
                        pivot_median = year_stats_df.pivot_table(
                            index='爆發次數', 
                            columns='年度', 
                            values='中位數報酬%',
                            aggfunc='first'
                        ).round(1)
                        
                        # 合併顯示
                        st.write("### 前後年度平均報酬 (%)")
                        st.dataframe(pivot_mean, use_container_width=True)
                        
                        st.write("### 前後年度中位數報酬 (%)")
                        st.dataframe(pivot_median, use_container_width=True)
                        
        except Exception as e:
            st.error(f"前後年度數據查詢失敗: {str(e)}")
    
    # ========== F. 區間名單點名功能 ==========
    st.markdown("---")
    st.subheader("🔍 詳細名單分析")
    
    hit_options = df_prob["爆發次數"].tolist()
    if hit_options:
        selected_hits = st.selectbox("選擇『爆發次數』查看具體股票名單：", hit_options, key="hits_selector")
        
        # 獲取詳細名單
        detail_query = f"""
        WITH hit_table AS (
            SELECT stock_id, COUNT(*) as hits 
            FROM monthly_revenue 
            WHERE (
                report_month = '{prev_minguo_year}_12' 
                OR (report_month LIKE '{minguo_year}_%' AND report_month <= '{minguo_year}_11')
            )
            AND {study_metric} >= {growth_range[0]} AND {study_metric} < {growth_range[1]}
            GROUP BY stock_id
        )
        SELECT h.stock_id as "股票代號", 
               COALESCE(m.stock_name, 'N/A') as "股票名稱",
               h.hits as "爆發次數",
               ROUND(((k.{price_field} - k.year_open)/k.year_open*100)::numeric, 1) as "年度漲幅%",
               ROUND(AVG(m.{study_metric})::numeric, 1) as "平均增長%",
               STRING_AGG(DISTINCT CASE WHEN m.remark <> '-' AND m.remark <> '' THEN m.remark END, ' | ') as "關鍵備註"
        FROM hit_table h
        LEFT JOIN stock_annual_k k ON h.stock_id = SPLIT_PART(k.symbol, '.', 1) AND k.year = '{target_year}'
        LEFT JOIN monthly_revenue m ON h.stock_id = m.stock_id 
          AND (m.report_month LIKE '{minguo_year}_%' OR m.report_month = '{prev_minguo_year}_12')
        WHERE h.hits = {selected_hits}
        GROUP BY h.stock_id, m.stock_name, k.{price_field}, k.year_open, h.hits
        ORDER BY "年度漲幅%" DESC NULLS LAST
        LIMIT 100;
        """
        
        try:
            with get_engine().connect() as conn:
                detail_df = pd.read_sql_query(text(detail_query), conn)
            
            if not detail_df.empty:
                st.write(f"### 🏆 {target_year}年『營收爆發 {selected_hits} 次』股票清單（共{len(detail_df)}檔）")
                st.caption(f"計算方式：{price_calc} | 使用{price_label}計算年度漲幅")
                
                # 名單統計
                if len(detail_df) > 0:
                    avg_return = detail_df["年度漲幅%"].mean()
                    median_return = detail_df["年度漲幅%"].median()
                    positive_count = (detail_df["年度漲幅%"] > 0).sum()
                    positive_rate = positive_count / len(detail_df) * 100
                    
                    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                    col_s1.metric("平均年度漲幅", f"{avg_return:.1f}%")
                    col_s2.metric("中位數漲幅", f"{median_return:.1f}%")
                    col_s3.metric("上漲檔數", f"{positive_count}檔")
                    col_s4.metric("上漲比例", f"{positive_rate:.1f}%")
                
                st.dataframe(detail_df, use_container_width=True)
                
                # 下載按鈕
                st.download_button(
                    label="📊 下載名單CSV",
                    data=detail_df.to_csv(index=False).encode('utf-8'),
                    file_name=f'burst_{selected_hits}_stocks_{target_year}_{price_label}.csv',
                    mime='text/csv'
                )
        except Exception as e:
            st.error(f"詳細名單查詢失敗: {str(e)}")

else:
    st.warning(f"⚠️ 在 {target_year} 年及設定條件下，沒有符合條件的樣本。")
    st.info("""
    💡 **調整建議**：
    1. 降低爆發門檻值
    2. 更換分析年度  
    3. 嘗試不同的增長指標
    4. 放寬增長範圍
    """)


# ========== 8. 頁尾資訊 ==========
st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns(3)
with footer_col1:
    st.markdown(f"**版本**：機率研究室 2.0 ({price_calc})")
with footer_col2:
    st.markdown(f"**數據週期**：2019-2025")
with footer_col3:
    st.markdown(f"**計算方式**：{price_label}漲幅")

# ========== 9. 快速資源連結 ==========
st.divider()
st.markdown("### 🔗 快速資源連結")

# 使用 markdown 創建您想要的格式
st.markdown("""
<div style="text-align: center;">
    <table style="margin: 0 auto; border-collapse: separate; border-spacing: 30px 0;">
        <tr>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">🛠️</div>
                <a href="https://vocus.cc/article/695636c3fd89780001d873bd" target="_blank" style="text-decoration: none;">
                    <b>⚙️ 環境與 AI 設定教學</b>
                </a>
            </td>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">📊</div>
                <a href="https://vocus.cc/salon/grissomlin/room/695636ee0c0c0689d1e2aa9f" target="_blank" style="text-decoration: none;">
                    <b>📖 儀表板功能詳解</b>
                </a>
            </td>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">🐙</div>
                <a href="https://github.com/grissomlin/StockRevenueLab" target="_blank" style="text-decoration: none;">
                    <b>💻 GitHub 專案原始碼</b>
                </a>
            </td>
        </tr>
    </table>
</div>
""", unsafe_allow_html=True)

# 隱藏Streamlit預設元素
hide_st_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
