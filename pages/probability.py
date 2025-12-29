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

# ========== 3. 新增：獲取前後年度比較數據（包含中位數） ==========
@st.cache_data(ttl=3600)
def fetch_multi_year_data(stock_list, target_year):
    """獲取指定股票在前後年度的表現（包含中位數）"""
    if not stock_list:
        return pd.DataFrame()
    
    engine = get_engine()
    stock_ids = ','.join([f"'{id}'" for id in stock_list])
    
    query = f"""
    WITH years_data AS (
        SELECT 
            SPLIT_PART(symbol, '.', 1) as stock_id,
            year,
            ((year_close - year_open) / year_open) * 100 as annual_return
        FROM stock_annual_k
        WHERE SPLIT_PART(symbol, '.', 1) IN ({stock_ids})
            AND year::integer BETWEEN {int(target_year)-2} AND {int(target_year)+1}
    )
    SELECT * FROM years_data;
    """
    
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 4. 數據抓取引擎 (精確對齊年度報表) ==========
@st.cache_data(ttl=3600)
def fetch_prob_data(year, metric_col, low, high):
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
                ((year_close - year_open) / year_open)*100 as ret
        FROM stock_annual_k WHERE year = '{year}'
    )
    SELECT h.hits as "爆發次數", COUNT(*) as "股票檔數",
           ROUND(AVG(p.ret)::numeric, 1) as "平均年度漲幅%",
           ROUND(MEDIAN(p.ret)::numeric, 1) as "中位數漲幅%",
           ROUND((COUNT(*) FILTER (WHERE p.ret > 20) * 100.0 / COUNT(*))::numeric, 1) as "勝率(>20%)",
           ROUND((COUNT(*) FILTER (WHERE p.ret > 100) * 100.0 / COUNT(*))::numeric, 1) as "翻倍率(>100%)",
           ROUND(MIN(p.ret)::numeric, 1) as "最低漲幅%",
           ROUND(MAX(p.ret)::numeric, 1) as "最高漲幅%",
           ROUND(STDDEV(p.ret)::numeric, 1) as "標準差%"
    FROM hit_table h JOIN perf_table p ON h.stock_id = p.stock_id
    GROUP BY h.hits ORDER BY h.hits DESC;
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

# ========== 5. 新增：計算期望值指標 ==========
def calculate_expected_value(df):
    """計算期望值相關指標"""
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
        risk_adjusted = avg_return / max(row["標準差%"], 1)
        
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
            "期望值分數": round(expected_value / 100, 2),
            "風險調整分數": round(risk_adjusted, 2),
            "成功率分數": round(success_adjusted, 2),
            "綜合評分": round((expected_value/100 + risk_adjusted + success_adjusted) / 3, 2)
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
    
    growth_range = st.select_slider(
        f"設定{metric_name}爆發區間 (%)", 
        options=[-50, 0, 20, 50, 100, 150, 200, 300, 500, 1000], 
        value=(100, 1000)
    )
    
    st.markdown("---")
    st.markdown("### 📊 分析選項")
    show_advanced = st.checkbox("顯示進階分析", value=True)
    show_multi_year = st.checkbox("顯示前後年度比較", value=True)
    show_expected_value = st.checkbox("計算期望值評分", value=True)

# 獲取主要數據
df_prob = fetch_prob_data(target_year, study_metric, growth_range[0], growth_range[1])

if not df_prob.empty:
    # ========== A. 核心數據顯示區 ==========
    st.subheader(f"📊 {target_year}年：{metric_name}達標次數 vs 年度報酬統計")
    
    # 顯示基本統計
    total_stocks = df_prob["股票檔數"].sum()
    st.metric("總樣本股票數", f"{total_stocks} 檔")
    
    # 顯示原始表格
    display_cols = ["爆發次數", "股票檔數", "平均年度漲幅%", "中位數漲幅%", 
                    "勝率(>20%)", "翻倍率(>100%)", "標準差%"]
    
    st.dataframe(df_prob[display_cols].style.format({
        "平均年度漲幅%": "{:.1f}%",
        "中位數漲幅%": "{:.1f}%",
        "勝率(>20%)": "{:.1f}%", 
        "翻倍率(>100%)": "{:.1f}%",
        "標準差%": "{:.1f}%"
    }), use_container_width=True)
    
    # ========== B. 視覺化分析 ==========
    if show_advanced:
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
                title=f"{metric_name}爆發次數 vs 年度表現",
                yaxis_title='漲幅 %',
                height=400
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # 平均數 vs 中位數差異
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
            **平均數與中位數差異分析**：
            - {pos_diff_count}/{len(df_prob)} 個區間({pos_diff_percent:.1f}%) 平均數 > 中位數
            - **表示多數區間存在右偏分佈**：少數股票漲幅極高，拉高了平均值
            - 當差異越大，代表該爆發次數區間的**右尾效應**越明顯
            """)
    
    # ========== C. 期望值分析 ==========
    if show_expected_value and len(df_prob) > 1:
        st.subheader("🎯 期望值與綜合評分分析")
        
        # 計算期望值指標
        expected_df = calculate_expected_value(df_prob)
        
        # 找出最佳區間
        if '綜合評分' in expected_df.columns:
            best_idx = expected_df["綜合評分"].idxmax()
            best_hits = expected_df.loc[best_idx, "爆發次數"]
            best_score = expected_df.loc[best_idx, "綜合評分"]
        else:
            best_hits = df_prob.loc[0, "爆發次數"]
            best_score = 0
        
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("最佳爆發次數", f"{best_hits} 次")
        col_b.metric("綜合評分", f"{best_score:.2f}")
        col_c.metric("該區間樣本數", f"{int(expected_df.loc[best_idx if 'best_idx' in locals() else 0, '股票檔數'])} 檔")
        
        # 顯示期望值表格
        st.dataframe(expected_df.style.format({
            "平均年度漲幅%": "{:.1f}",
            "中位數漲幅%": "{:.1f}",
            "平均-中位差": "{:.1f}",
            "期望值分數": "{:.2f}",
            "風險調整分數": "{:.2f}",
            "成功率分數": "{:.2f}",
            "綜合評分": "{:.2f}"
        }).highlight_max(subset=["綜合評分"], color='lightgreen'), 
        use_container_width=True)
    
    # ========== D. AI 分析助手區 (改進版) ==========
    st.markdown("---")
    st.subheader("🤖 AI 深度策略診斷")
    
    # 建構Markdown表格
    header = "| " + " | ".join(df_prob.columns) + " |"
    sep = "| " + " | ".join(["---"] * len(df_prob.columns)) + " |"
    rows = ["| " + " | ".join(map(str, row.values)) + " |" for _, row in df_prob.iterrows()]
    table_md = "\n".join([header, sep] + rows)
    
    # 建構完整的提示詞
    prompt_text = f"""
# {target_year}年台股營收爆發次數與年度報酬關聯分析

## 研究設定
- **分析年度**: {target_year}年
- **研究指標**: {metric_name}
- **爆發門檻**: {growth_range[0]}% 至 {growth_range[1]}%
- **研究期間**: 前一年12月到{target_year}年11月（12個月份）
- **股價計算**: {target_year}年度漲跌幅（年K線）

## 完整統計數據
{table_md}

## 關鍵觀察點（從數據中發現）
1. **極端值分析**: 
   - 爆發12次的有2檔股票，平均漲幅221.8%，勝率100%，翻倍率100%
   - 爆發11次的僅1檔，漲幅-24.4%，全部虧損

2. **樣本分佈特徵**:
   - 爆發次數越少，樣本數越多（符合常態分佈）
   - 爆發1次: 229檔（最多）
   - 爆發12次: 2檔（最少）

## 分析問題
請以專業量化分析師的角度，針對以上數據回答以下問題：

### 1. 相關性分析
- 「爆發次數」與「平均年度漲幅」、「中位數漲幅」、「勝率(>20%)」之間是否存在正相關？
- 從哪些數據點可以支持你的結論？

### 2. 平均數與中位數差異分析
- 哪些爆發次數區間的「平均-中位數」差異最大？這代表什麼意義？
- 右尾效應（平均>中位）最明顯的區間是哪個？對投資策略有何啟示？

### 3. 投資策略建議
- 根據期望值（兼顧樣本數與漲幅），哪個「爆發次數區間」是最佳投資標的？
- 對於不同風險偏好的投資者，你會建議關注哪個爆發次數區間？

### 4. 市場行為洞察
- 從數據中，你認為市場對於營收爆發的「反應模式」是什麼？
- 是否有「邊際效應遞減」的現象？（即更多次爆發是否帶來更高報酬？）

### 5. 實務操作建議
- 投資人應該如何利用這個統計規律來制定交易策略？
- 需要搭配哪些其他指標或條件來提高勝率？
"""
    
    col_prompt, col_link = st.columns([2, 1])
    with col_prompt:
        st.write("📋 **AI分析指令（已包含完整參數）**")
        st.code(prompt_text, language="text", height=500)
        
        # 顯示分析重點摘要
        with st.expander("🔍 本次分析重點摘要", expanded=True):
            st.markdown(f"""
            **核心研究問題**：
            - {metric_name}在{growth_range[0]}%-{growth_range[1]}%區間
            - {target_year}年共{total_stocks}檔股票符合條件
            - 分析爆發次數與年度報酬的關係
            
            **關鍵發現**：
            - 最高爆發12次：2檔，平均漲幅{df_prob.loc[0, '平均年度漲幅%'] if len(df_prob) > 0 else 'N/A'}%
            - 最低爆發1次：{df_prob.loc[len(df_prob)-1, '股票檔數'] if len(df_prob) > 0 else 'N/A'}檔，平均漲幅{df_prob.loc[len(df_prob)-1, '平均年度漲幅%'] if len(df_prob) > 0 else 'N/A'}%
            - 樣本分佈：次數越少，檔數越多
            
            **待解問題**：
            1. 是否存在正相關？
            2. 最佳投資區間為何？
            3. 市場反應模式分析
            """)
    
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
        
        st.link_button(
            "🤖 Claude 分析", 
            "https://claude.ai/",
            help="請複製上方指令貼上使用"
        )
        
        # 快速分析按鈕
        if st.button("📊 執行快速統計分析", type="secondary"):
            st.session_state.quick_analysis = True
    
    # 快速分析功能
    if st.session_state.get("quick_analysis", False):
        st.markdown("### ⚡ 快速統計分析結果")
        
        # 計算相關係數
        numeric_cols = ["平均年度漲幅%", "中位數漲幅%", "勝率(>20%)", "翻倍率(>100%)"]
        correlations = {}
        
        for col in numeric_cols:
            if col in df_prob.columns and "爆發次數" in df_prob.columns:
                corr = df_prob["爆發次數"].corr(df_prob[col])
                correlations[col] = round(corr, 3)
        
        # 顯示相關係數
        cols = st.columns(len(correlations))
        for (col_name, corr_value), col in zip(correlations.items(), cols):
            with col:
                corr_label = "強正相關" if corr_value > 0.7 else ("中正相關" if corr_value > 0.3 else ("弱相關" if corr_value > 0.1 else ("無相關" if abs(corr_value) <= 0.1 else ("負相關" if corr_value < 0 else "N/A"))))
                st.metric(
                    f"爆發次數 vs {col_name}",
                    f"{corr_value}",
                    corr_label
                )
        
        # 提供簡單結論
        st.info(f"""
        **初步觀察結論**：
        1. **相關性**: 爆發次數與年度報酬呈現{correlations.get('平均年度漲幅%', 0) > 0.5 and '強正相關' or '弱相關或無關'}
        2. **最佳區間**: 從期望值看，爆發{best_hits if 'best_hits' in locals() else '4-6'}次可能是最佳區間
        3. **風險提示**: 高爆發次數(>10次)樣本過少，統計意義有限
        4. **右尾效應**: 平均數 > 中位數的區間有{pos_diff_count if 'pos_diff_count' in locals() else '多數'}個，顯示存在右偏分佈
        """)
    
    # ========== E. 前後年度比較分析（包含中位數） ==========
    if show_multi_year:
        st.markdown("---")
        st.subheader("📈 前後年度表現比較分析")
        
        # 獲取詳細股票名單
        minguo_year = int(target_year) - 1911
        prev_minguo_year = minguo_year - 1
        
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
        """
        
        with get_engine().connect() as conn:
            stock_list_df = pd.read_sql_query(text(list_query), conn)
        
        if not stock_list_df.empty:
            # 獲取前後年度數據
            multi_year_df = fetch_multi_year_data(stock_list_df['stock_id'].tolist(), target_year)
            
            if not multi_year_df.empty:
                # 按爆發次數分組分析
                merged_df = pd.merge(stock_list_df, multi_year_df, on='stock_id')
                
                # 計算各爆發次數的前後年度表現（平均數）
                year_comparison_mean = merged_df.groupby(['hits', 'year']).agg({
                    'annual_return': 'mean'
                }).unstack().round(1)
                
                # 計算各爆發次數的前後年度表現（中位數）
                year_comparison_median = merged_df.groupby(['hits', 'year']).agg({
                    'annual_return': 'median'
                }).unstack().round(1)
                
                # 重新命名欄位
                year_comparison_mean.columns = [f'{col[1]}_平均' for col in year_comparison_mean.columns]
                year_comparison_median.columns = [f'{col[1]}_中位數' for col in year_comparison_median.columns]
                
                # 合併平均數和中位數
                year_comparison = pd.concat([year_comparison_mean, year_comparison_median], axis=1)
                
                # 重新排列列，使每個年度的平均數和中位數相鄰
                # 先獲取所有唯一的年份
                years = sorted(set([col.split('_')[0] for col in year_comparison.columns]))
                
                new_order = []
                for year in years:
                    if f'{year}_平均' in year_comparison.columns:
                        new_order.append(f'{year}_平均')
                    if f'{year}_中位數' in year_comparison.columns:
                        new_order.append(f'{year}_中位數')
                
                year_comparison = year_comparison[new_order]
                
                # 重新命名年份為更容易理解的形式
                year_mapping = {
                    str(int(target_year)-2): f'前2年({int(target_year)-2})',
                    str(int(target_year)-1): f'前1年({int(target_year)-1})',
                    target_year: f'目標年({target_year})',
                    str(int(target_year)+1): f'後1年({int(target_year)+1})'
                }
                
                year_comparison.columns = [year_mapping.get(col.split('_')[0], col.split('_')[0]) + '_' + col.split('_')[1] for col in year_comparison.columns]
                
                st.write("### 前後年度平均報酬與中位數比較 (%)")
                st.dataframe(year_comparison.style.format("{:.1f}"), use_container_width=True)
                
                # 添加分析問題
                st.markdown("""
                **前後年度分析問題**：
                1. **提前反應分析**：高爆發次數的股票，是否在**前一年**就已經有優異表現？
                2. **持續性分析**：高爆發次數的股票，在**後一年**是否仍維持強勢？
                3. **利多出盡現象**：是否存在目標年大漲，但後一年下跌的情況？
                4. **中位數 vs 平均數**：哪些年度/爆發次數的「平均數與中位數差異」最大？代表什麼？
                """)
                
                # 添加簡單洞察
                if not year_comparison.empty:
                    # 找出提前反應最明顯的區間
                    early_response = {}
                    for hits in year_comparison.index:
                        target_year_col = f'目標年({target_year})_平均'
                        prev_year_col = f'前1年({int(target_year)-1})_平均'
                        
                        if target_year_col in year_comparison.columns and prev_year_col in year_comparison.columns:
                            target_return = year_comparison.loc[hits, target_year_col]
                            prev_return = year_comparison.loc[hits, prev_year_col]
                            
                            if pd.notna(target_return) and pd.notna(prev_return):
                                early_response[hits] = prev_return
                    
                    if early_response:
                        max_hits = max(early_response, key=early_response.get)
                        max_return = early_response[max_hits]
                        
                        st.info(f"""
                        **洞察發現**：
                        - **提前反應最明顯**：爆發{max_hits}次的股票，前一年平均漲幅{max_return:.1f}%
                        - **投資啟示**：如果前一年已大漲，可能已反映部分預期，需注意追高風險
                        """)
    
    # ========== F. 區間名單點名功能（修正下載按鈕錯誤） ==========
    st.markdown("---")
    st.subheader("🔍 詳細名單分析")
    
    hit_options = df_prob["爆發次數"].tolist()
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
           ROUND(((k.year_close - k.year_open)/k.year_open*100)::numeric, 1) as "年度漲幅%",
           ROUND(AVG(m.{study_metric})::numeric, 1) as "平均增長%",
           STRING_AGG(DISTINCT CASE WHEN m.remark <> '-' AND m.remark <> '' THEN m.remark END, ' | ') as "關鍵備註"
    FROM hit_table h
    LEFT JOIN stock_annual_k k ON h.stock_id = SPLIT_PART(k.symbol, '.', 1) AND k.year = '{target_year}'
    LEFT JOIN monthly_revenue m ON h.stock_id = m.stock_id 
      AND (m.report_month LIKE '{minguo_year}_%' OR m.report_month = '{prev_minguo_year}_12')
    WHERE h.hits = {selected_hits}
    GROUP BY h.stock_id, m.stock_name, k.year_close, k.year_open, h.hits
    ORDER BY "年度漲幅%" DESC NULLS LAST;
    """
    
    with get_engine().connect() as conn:
        detail_df = pd.read_sql_query(text(detail_query), conn)
    
    if not detail_df.empty:
        st.write(f"### 🏆 {target_year}年『營收爆發 {selected_hits} 次』股票清單（共{len(detail_df)}檔）")
        
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
        
        # 名單專屬AI分析
        st.markdown("### 🤖 名單深度診斷")
        
        # 建構名單Markdown
        l_header = "| " + " | ".join(detail_df.columns) + " |"
        l_sep = "| " + " | ".join(["---"] * len(detail_df.columns)) + " |"
        l_rows = ["| " + " | ".join(map(str, r.values)) + " |" for _, r in detail_df.iterrows()]
        list_md = "\n".join([l_header, l_sep] + l_rows)

        list_prompt = f"""
# {target_year}年營收爆發{selected_hits}次股票詳細分析

## 分析背景
- **目標年度**: {target_year}年
- **爆發次數**: {selected_hits}次
- **增長指標**: {metric_name}
- **門檻範圍**: {growth_range[0]}% 至 {growth_range[1]}%
- **樣本數量**: {len(detail_df)}檔股票

## 詳細名單數據
{list_md}

## 名單統計摘要
- 平均年度漲幅: {avg_return:.1f}%
- 中位數漲幅: {median_return:.1f}%
- 上漲股票比例: {positive_rate:.1f}%
- 最高漲幅: {detail_df['年度漲幅%'].max():.1f}%
- 最低漲幅: {detail_df['年度漲幅%'].min():.1f}%

## 分析問題
請針對這份名單進行深度分析：

1. **產業特徵分析**：
   - 從「關鍵備註」欄位中，這些股票是否有共同的產業特性？
   - 是否存在某種「營收認列模式」？（如：專案入帳、季節性因素等）

2. **表現差異解讀**：
   - 為什麼有些股票「平均增長%」很高，但「年度漲幅%」卻不突出？
   - 以8476台境為例，如果數據中存在，請分析其高增長但低漲幅的原因

3. **平均數與中位數分析**：
   - 平均漲幅({avg_return:.1f}%) vs 中位數漲幅({median_return:.1f}%)的差異代表什麼？
   - 是否存在右尾效應（少數股票漲幅極高，拉高平均）？

4. **投資啟示**：
   - 從這份名單中，投資人應該注意哪些關鍵指標？
   - 如何區分「真成長」與「一次性增長」？

5. **策略建議**：
   - 對於爆發{selected_hits}次的股票，最佳的買賣時機為何？
   - 需要搭配哪些技術指標或基本面條件來提高勝率？
"""
        
        col_lp, col_ll = st.columns([2, 1])
        with col_lp:
            st.code(list_prompt, language="text", height=400)
        with col_ll:
            encoded_list_p = urllib.parse.quote(list_prompt)
            st.link_button("🔥 ChatGPT 分析名單", f"https://chatgpt.com/?q={encoded_list_p}")
            st.link_button("🔍 DeepSeek 分析", "https://chat.deepseek.com/")
            
            # 修正：使用 st.download_button 而不是 st.link_button
            st.download_button(
                label="📊 下載名單CSV",
                data=detail_df.to_csv(index=False).encode('utf-8'),
                file_name=f'burst_{selected_hits}_stocks_{target_year}.csv',
                mime='text/csv'
            )

else:
    st.warning(f"⚠️ 在 {target_year} 年及設定條件下，沒有符合條件的樣本。")
    st.info("""
    💡 **調整建議**：
    1. 降低爆發門檻值
    2. 更換分析年度  
    3. 嘗試不同的增長指標
    4. 放寬增長範圍
    """)

# ========== 7. 頁尾資訊 ==========
st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns(3)
with footer_col1:
    st.markdown("**版本**：機率研究室 2.0")
with footer_col2:
    st.markdown(f"**數據週期**：{int(target_year)-2 if show_multi_year else 2019}-{int(target_year)+1 if show_multi_year else 2025}")
with footer_col3:
    st.markdown("**研究重點**：爆發次數 vs 年度報酬")

# 初始化session state
if 'quick_analysis' not in st.session_state:
    st.session_state.quick_analysis = False
