import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# 1. 頁面基本設定
st.set_page_config(page_title="主力早知道 | StockRevenueLab", layout="wide")

# 2. 資料庫連接 (從 Secrets 讀取配置)
@st.cache_resource
def get_engine():
    try:
        DB_PASSWORD = st.secrets["DB_PASSWORD"]
        PROJECT_REF = st.secrets["PROJECT_REF"]
        POOLER_HOST = st.secrets["POOLER_HOST"]
        connection_string = f"postgresql://postgres.{PROJECT_REF}:{urllib.parse.quote_plus(DB_PASSWORD)}@{POOLER_HOST}:5432/postgres?sslmode=require"
        return create_engine(connection_string)
    except Exception as e:
        st.error(f"資料庫連接設定錯誤，請檢查 st.secrets: {e}")
        return None

# 3. 標題與研究導論
st.title("🕵️ 主力早知道？營收爆發前後的股價行為")
st.markdown("""
本研究分析 **「第一次營收爆發」** 時，市場的反應規律。這能幫助我們辨識市場是否具備「資訊不對稱」的特徵。
* **主力預跑 (Month T)**：報表尚未公佈（例如 11 月營收要到 12/10 才公佈），但 11 月股價已經先行發動。
* **利多追價 (Month T+1)**：報表正式公佈後的月份，市場散戶與機構是否持續跟進。
""")

# 4. 側邊欄：參數設定與說明
with st.sidebar:
    st.header("⚙️ 分析參數")
    threshold = st.slider("設定爆發門檻 (YoY %)", 20, 300, 100)
    st.divider()
    st.markdown("""
    ### 📖 指標定義說明
    - **預跑率**：在營收月份當月，股價漲幅即超過 5%。
    - **追價率**：營收公佈後的次月，股價持續上漲超過 5%。
    - **利多出盡**：營收公佈後的次月，股價下跌超過 5%。
    """)

# 5. SQL 核心查詢 (修正 DataError 與日期邏輯)
# 確保 report_month 格式符合 'YYYY_MM'
query = text(f"""
WITH first_events AS (
    SELECT stock_id, report_month, yoy_pct,
           LAG(yoy_pct) OVER(PARTITION BY stock_id ORDER BY report_month) as prev_yoy
    FROM monthly_revenue
    WHERE yoy_pct >= :threshold
),
filtered_first AS (
    SELECT * FROM first_events WHERE prev_yoy IS NULL OR prev_yoy < :threshold
),
price_behavior AS (
    SELECT 
        f.stock_id, f.report_month, f.yoy_pct,
        ((p1.m_close - p1.m_open)/p1.m_open * 100) as pre_run_ret,
        ((p2.m_close - p2.m_open)/p2.m_open * 100) as post_run_ret
    FROM filtered_first f
    JOIN stock_monthly_k p1 ON f.stock_id = SPLIT_PART(p1.symbol, '.', 1) AND f.report_month = p1.report_month
    LEFT JOIN stock_monthly_k p2 ON p1.symbol = p2.symbol 
      AND p2.report_month = TO_CHAR(
          (TO_DATE(f.report_month, 'YYYY_MM') + INTERVAL '1 month'), 
          'YYYY_MM'
      )
)
SELECT 
    COUNT(*) as "總事件樣本",
    ROUND(AVG(pre_run_ret)::numeric, 1) as "預跑平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE pre_run_ret > 5) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 1) as "主力預跑率(漲幅>5%)",
    ROUND(AVG(post_run_ret)::numeric, 1) as "公佈後平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE post_run_ret > 5) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 1) as "公佈後追價率(漲幅>5%)",
    ROUND((COUNT(*) FILTER (WHERE post_run_ret < -5) * 100.0 / NULLIF(COUNT(*), 0))::numeric, 1) as "利多出盡機率(跌幅>5%)"
FROM price_behavior
""")

# 6. 資料執行與結果顯示
engine = get_engine()
if engine:
    with engine.connect() as conn:
        try:
            res = pd.read_sql_query(query, conn, params={"threshold": threshold})
            
            if not res.empty and res["總事件樣本"].iloc[0] > 0:
                st.subheader("📊 全市場大數據分析結果")
                st.table(res)
                st.info("💡 註：『主力預跑』指營收月份當月。例如 11 月營收 12/10 才公佈，但 11 月股價就先漲了。")
                
                # --- 功能區：AI 分析助手 ---
                st.divider()
                col1, col2 = st.columns(2)
                
                stats = res.iloc[0].to_dict()
                prompt_text = (
                    f"我正在分析台股營收爆發後的股價行為。當 YoY 門檻設為 {threshold}% 時，數據如下：\n"
                    f"- 樣本總數：{stats['總事件樣本']} 件\n"
                    f"- 主力預跑率 (漲幅>5%)：{stats['主力預跑率(漲幅>5%)']}%\n"
                    f"- 預跑平均漲幅：{stats['預跑平均漲幅%']}%\n"
                    f"- 營收公佈後追價率：{stats['公佈後追價率(漲幅>5%)']}%\n"
                    f"- 利多出盡機率 (跌幅>5%)：{stats['利多出盡機率(跌幅>5%)']}%\n\n"
                    "請分析這代表市場對營收消息的反應是『領先反應』還是『落後補漲』？並針對此數據結果給予投資策略建議。"
                )

                with col1:
                    st.subheader("🤖 產生 AI 提示詞")
                    st.code(prompt_text, language="text")
                    st.caption("點擊右上角複製圖示，貼上至 AI 模型進行深度診斷。")

                with col2:
                    st.subheader("🚀 直接詢問 AI")
                    encoded_prompt = urllib.parse.quote(prompt_text)
                    
                    st.link_button("🔥 開啟 ChatGPT 分析 (自動帶入)", f"https://chatgpt.com/?q={encoded_prompt}")
                    st.link_button("Ⓜ️ 開啟 Microsoft Copilot (需手動貼上)", f"https://www.bing.com/chat?q={encoded_prompt}")
                    st.link_button("🌐 開啟 Claude.ai (需手動貼上)", "https://claude.ai/")
                    
                    st.warning("提醒：Copilot 與 ChatGPT 支援自動填入；Claude 建議複製左側代碼。")
            else:
                st.warning("⚠️ 目前設定的門檻過高，查無符合的事件樣本，請嘗試調低 YoY 門檻。")
        except Exception as sql_err:
            st.error(f"SQL 查詢執行失敗。可能原因：資料表格式不符。詳細錯誤：{sql_err}")
else:
    st.error("無法建立資料庫連線，請確認 Streamlit Secrets 設定。")
