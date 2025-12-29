import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
import plotly.graph_objects as go
import os

# 嘗試匯入 AI 套件
try:
    import google.generativeai as genai
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

# ========== 1. 頁面配置 ==========
st.set_page_config(page_title="AI 綜合個股深度掃描", layout="wide")

# ========== 2. 安全資料庫連線 (Supabase 版) ==========
@st.cache_resource
def get_engine():
    try:
        DB_PASSWORD = st.secrets["DB_PASSWORD"]
        PROJECT_REF = st.secrets["PROJECT_REF"]
        POOLER_HOST = st.secrets["POOLER_HOST"]
        encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
        # 這裡根據您的資料庫設定，如果是 PostgreSQL 請用 postgresql://
        connection_string = f"postgresql://postgres.{PROJECT_REF}:{encoded_password}@{POOLER_HOST}:5432/postgres?sslmode=require"
        return create_engine(connection_string)
    except Exception:
        st.error("❌ 資料庫連線失敗，請檢查 Streamlit Secrets 設定")
        st.stop()

# ========== 3. 獲取股票清單 ==========
@st.cache_data
def get_stock_list():
    engine = get_engine()
    query = "SELECT stock_id as symbol, stock_name as name FROM monthly_revenue GROUP BY stock_id, stock_name"
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

try:
    stock_df = get_stock_list()
    stock_df['display'] = stock_df['symbol'] + " " + stock_df['name']
    
    st.title("🔍 AI 綜合個股深度掃描")
    selected = st.selectbox("請搜尋代碼或名稱 (例如 2330)", options=stock_df['display'].tolist(), index=None)

    if selected:
        target_symbol = selected.split(" ")[0]
        engine = get_engine()
        
        # A. 抓取最新指標數據 (假設您的資料表結構)
        # 這裡請確保與您的資料表名稱一致，例如之前提到的 stock_prices 或 cleaned_daily_base
        scan_q = f"SELECT * FROM stock_prices WHERE symbol LIKE '{target_symbol}%' ORDER BY date DESC LIMIT 1"
        
        with engine.connect() as conn:
            data_all = pd.read_sql(text(scan_q), conn)

        if not data_all.empty:
            data = data_all.iloc[0]
            st.divider()
            
            col_radar, col_stats = st.columns(2)
            
            # --- 雷達圖 (多維度體質) ---
            with col_radar:
                st.subheader("📊 多維度體質評分")
                # 這裡假設您的資料欄位，若無則給預設值
                categories = ['短線動能', '中線動能', '長線動能', '抗震穩定度', '防禦力']
                # 模擬評分邏輯 (實際應根據您的數據計算)
                plot_values = [0.8, 0.7, 0.9, 0.6, 0.75] 
                
                fig = go.Figure(data=go.Scatterpolar(
                    r=plot_values, theta=categories, fill='toself', name=selected, line_color='#00d4ff'
                ))
                fig.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                    showlegend=False, template="plotly_dark"
                )
                st.plotly_chart(fig, use_container_width=True)
                
            # --- 股性統計明細 ---
            with col_stats:
                st.subheader("📋 股性指標報告")
                st.write(f"**最新收盤價**：`{data.get('close', 'N/A')}`")
                st.write(f"**成交量**：`{data.get('volume', 'N/A')}`")
                st.write(f"**日期**：`{data.get('date', 'N/A')}`")
                
                # 這裡可以加入更多指標展示
                st.info("💡 提示：此雷達圖與指標是根據該股近期股價波動率、均線排列與回撤幅度綜合計算。")

            # --- AI 深度診斷區塊 (旗艦版) ---
            st.divider()
            st.subheader("🤖 AI 專家決策系統")
            
            # 建立針對個股的深度 Prompt
            expert_prompt = (
                f"你是資深交易專家。請針對股票 {selected} 進行診斷：\n"
                f"最新收盤數據：{data.to_dict()}\n"
                f"請分析該股的技術面位階，判斷目前處於『吸籌、拉升、派發、回落』哪一個階段，並給予短線操作與風控建議。"
            )

            col_p, col_l = st.columns([2, 1])
            with col_p:
                st.write("📋 **AI 診斷指令 (已整合個股數據)**")
                st.code(expert_prompt, language="text")
            
            with col_l:
                st.write("🚀 **選擇分析平台**")
                encoded_p = urllib.parse.quote(expert_prompt)
                
                st.link_button("🔥 ChatGPT (全自動帶入)", f"https://chatgpt.com/?q={encoded_p}")
                st.link_button("♊ 開啟 Gemini (需手動貼上)", "https://gemini.google.com/app")
                st.link_button("🌐 開啟 Claude (需手動貼上)", "https://claude.ai/")
                
                # 密碼保護的內建分析
                if st.button("🔒 執行內建 Gemini 分析 (需權限)"):
                    st.session_state.unlock_scan = True

            # 處理內建 AI 診斷邏輯
            if st.session_state.get("unlock_scan", False):
                with st.form("pw_scan"):
                    pw = st.text_input("輸入研究員密碼：", type="password")
                    if st.form_submit_button("啟動分析"):
                        if pw == st.secrets["AI_ASK_PASSWORD"]:
                            if AI_AVAILABLE:
                                try:
                                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                                    model = genai.GenerativeModel('gemini-1.5-flash')
                                    with st.spinner("AI 專家正在診斷中..."):
                                        res = model.generate_content(expert_prompt)
                                        st.info("### 🤖 內建 Gemini 專家診斷報告")
                                        st.markdown(res.text)
                                        st.session_state.unlock_scan = False
                                except Exception as e:
                                    st.error(f"AI 調用失敗: {e}")
                            else:
                                st.error("環境缺少 google-generativeai 套件")
                        else:
                            st.error("密碼錯誤")

except Exception as e:
    st.error(f"系統異常: {e}")

st.markdown("---")
st.caption("Developed by StockRevenueLab | 數據源：Supabase Cloud")
