import streamlit as st
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import google.generativeai as genai
import os
import urllib.parse

# 1. 頁面配置
st.set_page_config(page_title="AI 綜合個股深度掃描", layout="wide")

# 2. 市場資料庫配置
market_option = st.sidebar.selectbox("🚩 選擇市場", ("TW", "JP", "CN", "US", "HK", "KR"), key="scan_market")
db_map = {
    "TW": "tw_stock_warehouse.db", 
    "JP": "jp_stock_warehouse.db", 
    "CN": "cn_stock_warehouse.db", 
    "US": "us_stock_warehouse.db", 
    "HK": "hk_stock_warehouse.db", 
    "KR": "kr_stock_warehouse.db"
}
target_db = db_map[market_option]

url_templates = {
    "TW": "https://www.wantgoo.com/stock/{s}/technical-chart",
    "US": "https://www.tradingview.com/symbols/{s}/",
    "JP": "https://jp.tradingview.com/symbols/TSE-{s}/",
    "CN": "https://panyi.eastmoney.com/pc_sc_kline.html?s={s}",
    "HK": "https://www.tradingview.com/symbols/HKEX-{s}/",
    "KR": "https://www.tradingview.com/symbols/KRX-{s}/"
}
current_url_base = url_templates.get(market_option, "https://google.com/search?q={s}")

if not os.path.exists(target_db):
    st.error(f"請先回到首頁同步 {market_option} 數據庫")
    st.stop()

@st.cache_data
def get_full_stock_info(_db_path):
    conn = sqlite3.connect(_db_path)
    try:
        df = pd.read_sql("SELECT symbol, name, sector FROM stock_info", conn)
    except:
        df = pd.DataFrame(columns=['symbol', 'name', 'sector'])
    conn.close()
    return df

try:
    stock_df = get_full_stock_info(target_db)
    stock_df['display'] = stock_df['symbol'] + " " + stock_df['name']
    
    st.title("🔍 AI 綜合個股深度掃描")
    selected = st.selectbox("請搜尋代碼或名稱 (例如 2330)", options=stock_df['display'].tolist(), index=None)

    if selected:
        target_symbol = selected.split(" ")[0]
        conn = sqlite3.connect(target_db)
        
        # A. 抓取最新指標數據
        scan_q = f"SELECT * FROM cleaned_daily_base WHERE StockID = '{target_symbol}' ORDER BY 日期 DESC LIMIT 1"
        data_all = pd.read_sql(scan_q, conn)
        
        # B. 歷史股性統計 (2023 至今)
        hist_q = f"""
        SELECT COUNT(*) as t, SUM(is_limit_up) as lu, 
        SUM(CASE WHEN Prev_LU = 0 AND is_limit_up = 0 AND Ret_High > 0.095 THEN 1 ELSE 0 END) as failed_lu,
        AVG(CASE WHEN Prev_LU=1 THEN Overnight_Alpha END) as ov,
        AVG(CASE WHEN Prev_LU=1 THEN Next_1D_Max END) as nxt
        FROM cleaned_daily_base WHERE StockID = '{target_symbol}'
        """
        hist = pd.read_sql(hist_q, conn).iloc[0]

        # C. 獲取產業與同業
        temp_info_q = f"SELECT sector FROM stock_info WHERE symbol = '{target_symbol}'"
        sector_res = pd.read_sql(temp_info_q, conn)
        sector_name = sector_res.iloc[0,0] if not sector_res.empty else "未知"
        
        peer_q = f"SELECT symbol, name FROM stock_info WHERE sector = '{sector_name}' AND symbol != '{target_symbol}' LIMIT 8"
        peers_df = pd.read_sql(peer_q, conn)
        conn.close()

        if not data_all.empty:
            data = data_all.iloc[0]
            st.divider()
            
            col_radar, col_stats = st.columns(2)
            
            # --- 雷達圖 ---
            with col_radar:
                st.subheader("📊 多維度體質評分")
                r5 = data.get('Ret_5D', 0) or 0
                r20 = data.get('Ret_20D', 0) or 0
                r200 = data.get('Ret_200D', 0) or 0
                vol = data.get('volatility_20d', 0) or 0
                dd = data.get('drawdown_after_high_20d', 0) or 0

                categories = ['短線動能', '中線動能', '長線動能', '抗震穩定度', '防禦力']
                plot_values = [
                    min(max(r5 * 5 + 0.5, 0.1), 1),
                    min(max(r20 * 2 + 0.5, 0.1), 1),
                    min(max(r200 + 0.5, 0.1), 1),
                    max(1 - vol * 2, 0.1),
                    max(1 + dd, 0.1)
                ]
                
                fig = go.Figure(data=go.Scatterpolar(
                    r=plot_values, theta=categories, fill='toself', name=selected, line_color='#00d4ff'
                ))
                fig.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                    showlegend=False, template="plotly_dark"
                )
                st.plotly_chart(fig, use_container_width=True)
                
            # --- 行為統計 ---
            with col_stats:
                st.subheader("📋 股性統計 (2023~至今)")
                m1, m2 = st.columns(2)
                m1.metric("成功漲停次數", f"{int(hist['lu'] or 0)} 次")
                m2.metric("衝板失敗(炸板)", f"{int(hist['failed_lu'] or 0)} 次")
                
                st.write(f"**最新收盤價**：`{data['收盤']}`")
                st.write(f"**所屬產業**：`{sector_name}`")
                st.write(f"**漲停隔日溢價均值**：{(hist['ov'] or 0)*100:.2f}%")
                
                if not peers_df.empty:
                    st.write("**🔗 同產業參考**：")
                    links = [f"[{row['symbol']}]({current_url_base.replace('{s}', row['symbol'].split('.')[0])})" for _, row in peers_df.iterrows()]
                    st.caption(" ".join(links))

            # --- AI 深度診斷區塊 (旗艦版更新) ---
            st.divider()
            st.subheader("🤖 AI 專家決策系統 2.0")
            
            # 格式化提示詞 (Prompt Compression)
            expert_prompt = (
                f"你是資深交易專家。請針對股票 {selected} 進行診斷：\n"
                f"數據指標 (2023至今)：\n"
                f"- 成功漲停：{int(hist['lu'])} 次 / 炸板次數：{int(hist['failed_lu'])} 次\n"
                f"- 隔日溢價期望值：{(hist['ov'] or 0)*100:.2f}%\n"
                f"- 20日波動率：{vol*100:.2f}%\n"
                f"請分析該股籌碼壓力與妖性，判斷適不適合隔日沖，並給予短線風控建議。"
            )

            col_p, col_l = st.columns([2, 1])
            with col_p:
                st.write("📋 **AI 專家診斷指令**")
                st.code(expert_prompt, language="text")
            
            with col_l:
                st.write("🚀 **選擇分析平台**")
                encoded_p = urllib.parse.quote(expert_prompt)
                
                # 按鈕群組
                st.link_button("🔥 ChatGPT (全自動帶入)", f"https://chatgpt.com/?q={encoded_p}")
                st.link_button("♊ 開啟 Gemini (需手動貼上)", "https://gemini.google.com/app")
                st.link_button("🌐 開啟 Claude (需手動貼上)", "https://claude.ai/")
                
                # 密碼保護按鈕 (這會觸發內建 Gemini API)
                if st.button("🔒 執行內建 AI 深度分析 (需權限)"):
                    st.session_state.show_pw_scan = True

            # 密碼彈窗邏輯
            if st.session_state.get("show_pw_scan", False):
                with st.form("pw_scan_form"):
                    user_pw = st.text_input("請輸入研究員密碼：", type="password")
                    if st.form_submit_button("驗證並分析"):
                        if user_pw == st.secrets["AI_ASK_PASSWORD"]:
                            api_key = st.secrets.get("GEMINI_API_KEY")
                            if api_key:
                                try:
                                    genai.configure(api_key=api_key)
                                    model = genai.GenerativeModel('gemini-1.5-flash') # 使用 flash 加速
                                    with st.spinner("AI 專家正在閱卷中..."):
                                        response = model.generate_content(expert_prompt)
                                        st.session_state.ai_report = response.text
                                        st.session_state.show_pw_scan = False
                                except Exception as e:
                                    st.error(f"API 調用失敗: {e}")
                            else:
                                st.warning("Secrets 中缺少 GEMINI_API_KEY")
                        else:
                            st.error("密碼錯誤！")

            # 顯示分析結果
            if "ai_report" in st.session_state:
                st.info("### 🤖 內建 AI 專家診斷報告")
                st.markdown(st.session_state.ai_report)
                if st.button("🗑️ 清除報告"):
                    del st.session_state.ai_report
                    st.rerun()

except Exception as e:
    st.error(f"系統異常: {e}")

# --- 3. 底部快速連結 (Footer) ---
st.divider()
st.markdown("### 🔗 快速資源連結")
col_link1, col_link2, col_link3 = st.columns(3)
with col_link1:
    st.page_link("https://vocus.cc/article/694f813afd8978000101e75a", label="⚙️ 環境與 AI 設定教學", icon="🛠️")
with col_link2:
    st.page_link("https://vocus.cc/article/694f88bdfd89780001042d74", label="📖 儀表板功能詳解", icon="📊")
with col_link3:
    st.page_link("https://github.com/grissomlin/Alpha-Data-Cleaning-Lab", label="💻 GitHub 專案原始碼", icon="🐙")
