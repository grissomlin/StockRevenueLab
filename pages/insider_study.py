import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# 1. 頁面基本設定
st.set_page_config(page_title="主力早知道 | StockRevenueLab", layout="wide")

# 2. 資料庫連接
@st.cache_resource
def get_engine():
    try:
        DB_PASSWORD = st.secrets["DB_PASSWORD"]
        PROJECT_REF = st.secrets["PROJECT_REF"]
        POOLER_HOST = st.secrets["POOLER_HOST"]
        connection_string = f"postgresql://postgres.{PROJECT_REF}:{urllib.parse.quote_plus(DB_PASSWORD)}@{POOLER_HOST}:5432/postgres?sslmode=require"
        return create_engine(connection_string)
    except Exception as e:
        st.error(f"資料庫連接設定錯誤，請檢查 secrets: {e}")
        return None

# 3. 標題與導論
st.title("🕵️ 主力早知道？營收爆發前後的股價行為")
st.markdown("""
本研究分析 **「第一次營收爆發」** 時，市場的反應規律。這能幫助我們辨識市場是否具備「資訊不對稱」的特徵。
* **主力預跑 (Month T)**：報表尚未公佈（例如 11 月營收要到 12/10 才公佈），但 11 月股價已經先行發動。
* **利多追價 (Month T+1)**：報表正式公佈後的月份，市場散戶與機構是否持續跟進。
""")

# 4. 側邊欄：參數設定
with st.sidebar:
    st.header("⚙️ 分析參數")
    threshold = st.slider("設定爆發門檻 (YoY %)", 20, 300, 100)
    st.divider()
    st.markdown("""
    ### 📖 指標定義
    - **預跑率**：營收當月漲幅 > 5%
    - **追價率**：公佈次月漲幅 > 5%
    - **利多出盡**：公佈次月跌幅 > 5%
    """)

# 5. SQL 核心查詢 (修正 DataError 與日期邏輯)
# 註：假設 report_month 格式為 'YYYY_MM'
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

# 6. 資料執行與顯示
engine = get_engine()
if engine:
    with engine.connect() as conn:
        try:
            res = pd.read_sql_query(query, conn, params={"threshold": threshold})
            
            if not res.empty and res["總事件樣本"].iloc[0] > 0:
                st.subheader("📊 全市場大數據分析結果")
                st.table(res)
                st.info("💡 註：『主力預跑』指營收月份當月。例如 11 月營收 12/10 才公佈，但 11 月股價就先漲了。")
                
                # --- 功能增加：AI 輔助區 ---
                st.divider()
                col1, col2 = st.columns(2)
                
                stats = res.iloc[0].to_dict()
                prompt_text = (
                    f"我正在分析台股營收爆發後的股價行為。當門檻設為 {threshold}% 時：\n"
                    f"- 總樣本：{stats['總事件樣本']} 件\n"
                    f"- 主力預跑率：{stats['主力預跑率(漲幅>5%)']}%\n"
                    f"- 公佈後平均漲幅：{stats['公佈後平均漲幅%']}%\n"
                    f"- 利多出盡機率：{stats['利多出盡機率(跌幅>5%)']}%\n"
                    "請分析這代表市場對營收消息的反應是『領先反應』還是『落後補漲』？並給予操作策略建議。"
                )

                with col1:
                    st.subheader("🤖 產生 AI 提示詞")
                    st.markdown("複製下方文字到 AI 進行深度分析：")
                    st.code(prompt_text, language="text")
                    if st.button("補充說明：如何使用此提示詞"):
                        st.write("> **使用方式**：將上方框內文字複製，貼上至 ChatGPT 或 Claude。AI 會根據樣本數與勝率，分析目前市場是否已經過熱或仍有肉吃。")

                with col2:
                    st.subheader("🚀 直接詢問 AI")
                    st.write("點擊按鈕直接帶著數據開啟對話：")
                    encoded_prompt = urllib.parse.quote(prompt_text)
                    st.link_button("👉 在 ChatGPT 中分析", f"https://chatgpt.com/?q={encoded_prompt}")
                    st.link_button("👉 搜尋 Claude (手動貼上)", "https://claude.ai/")
            else:
                st.warning("⚠️ 查無符合條件的數據，請調低門檻或確認資料庫日期格式。")
        except Exception as sql_err:
            st.error(f"SQL 執行錯誤：{sql_err}")
else:
    st.error("無法建立資料庫連線。")
