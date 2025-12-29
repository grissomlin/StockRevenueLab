import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# 1. 頁面基本設定
st.set_page_config(page_title="主力早知道 | StockRevenueLab", layout="wide")

# 2. 資料庫連接 (保留原始邏輯，不精簡)
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

# 3. 標題與導論
st.title("🕵️ 主力早知道？營收爆發前後的股價行為")
st.markdown("""
本研究分析 **「第一次營收爆發」** 時，市場的反應規律。這能幫助我們辨識市場是否具備「資訊不對稱」的特徵。
* **主力預跑 (Month T)**：報表尚未公佈（例如 11 月營收要到 12/10 才公佈），但 11 月股價已經先行發動。
* **利利多追價 (Month T+1)**：報表正式公佈後的月份，市場散戶與機構是否持續跟進。
""")

# 4. 側邊欄：參數設定與補充說明
with st.sidebar:
    st.header("⚙️ 分析參數")
    threshold = st.slider("設定爆發門檻 (YoY %)", 20, 300, 100)
    
    st.divider()
    st.markdown("""
    ### 📖 指標定義說明
    - **第一次爆發**：指該股票 YoY 達到門檻，且上個月未達標（排除連續高成長的雜訊）。
    - **預跑率**：在營收公佈前（當月）漲幅就超過 5% 的比例。
    - **利多出盡**：營收公佈後（次月）跌幅超過 5% 的比例。
    """)

# 5. SQL 核心查詢邏輯
query = f"""
WITH first_events AS (
    SELECT stock_id, report_month, yoy_pct,
           LAG(yoy_pct) OVER(PARTITION BY stock_id ORDER BY report_month) as prev_yoy
    FROM monthly_revenue
    WHERE yoy_pct >= {threshold}
),
filtered_first AS (
    SELECT * FROM first_events WHERE prev_yoy IS NULL OR prev_yoy < {threshold}
),
price_behavior AS (
    SELECT 
        f.stock_id, f.report_month, f.yoy_pct,
        ((p1.m_close - p1.m_open)/p1.m_open * 100) as pre_run_ret,
        ((p2.m_close - p2.m_open)/p2.m_open * 100) as post_run_ret
    FROM filtered_first f
    JOIN stock_monthly_k p1 ON f.stock_id = SPLIT_PART(p1.symbol, '.', 1) AND f.report_month = p1.report_month
    LEFT JOIN stock_monthly_k p2 ON p1.symbol = p2.symbol 
      AND p2.report_month = (
          CASE WHEN RIGHT(p1.report_month, 2) = '12' 
          THEN (LEFT(p1.report_month, 4)::int + 1)::text || '-01' -- 修正原始代碼可能存在的格式微調
          ELSE LEFT(p1.report_month, 5) || LPAD((RIGHT(p1.report_month, 2)::int + 1)::text, 2, '0')
          END
      )
)
SELECT 
    COUNT(*) as "總事件樣本",
    ROUND(AVG(pre_run_ret)::numeric, 1) as "預跑平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE pre_run_ret > 5) * 100.0 / COUNT(*))::numeric, 1) as "主力預跑率(漲幅>5%)",
    ROUND(AVG(post_run_ret)::numeric, 1) as "公佈後平均漲幅%",
    ROUND((COUNT(*) FILTER (WHERE post_run_ret > 5) * 100.0 / COUNT(*))::numeric, 1) as "公佈後追價率(漲幅>5%)",
    ROUND((COUNT(*) FILTER (WHERE post_run_ret < -5) * 100.0 / COUNT(*))::numeric, 1) as "利多出盡機率(跌幅>5%)"
FROM price_behavior
"""

# 6. 資料執行與顯示
engine = get_engine()
if engine:
    with engine.connect() as conn:
        res = pd.read_sql_query(text(query), conn)
        
        if not res.empty and res["總事件樣本"].iloc[0] > 0:
            st.subheader("📊 全市場大數據分析結果")
            st.table(res)
            st.info("💡 註：『主力預跑』指營收月份當月。例如 11 月營收 12/10 才公佈，但 11 月股價就先漲了。")
            
            # --- 新增功能區 ---
            st.divider()
            col1, col2 = st.columns(2)
            
            # 準備提示詞內容
            stats = res.iloc[0].to_dict()
            prompt_text = f"""
我正在分析台灣股市的「營收爆發與股價先行關係」。
當設定營收年增率(YoY)門檻為 {threshold}% 時，分析結果如下：
- 樣本總數：{stats['總事件樣本']} 件
- 主力預跑平均漲幅：{stats['預跑平均漲幅%']}%
- 主力預跑率(漲幅>5%)：{stats['主力預跑率(漲幅>5%)']}%
- 營收公佈後平均漲幅：{stats['公佈後平均漲幅%']}%
- 利多出盡機率(跌幅>5%)：{stats['利多出盡機率(跌幅>5%)']}%

請以專業量化分析師的角度，解讀這些數據代表的市場意義，並給予投資策略建議。
"""

            with col1:
                st.subheader("🤖 產生 AI 分析提示詞")
                st.text_area("複製下方文字至 ChatGPT / Claude：", value=prompt_text, height=200)
                if st.button("📋 點擊產生提示詞說明"):
                    st.success("提示詞已產生！這段文字結合了目前的即時數據，能讓 AI 提供更精準的策略建議。")

            with col2:
                st.subheader("🚀 直接詢問 AI")
                st.write("點擊下方連結，快速開啟 AI 對話框：")
                
                # 建立 URL 轉義後的連結 (以 ChatGPT 為例)
                encoded_prompt = urllib.parse.quote(prompt_text)
                chatgpt_url = f"https://chatgpt.com/?q={encoded_prompt}"
                claude_url = f"https://claude.ai/" # Claude 目前不支援直接帶入 URL query
                
                st.link_button("👉 前往 ChatGPT 進行分析", chatgpt_url)
                st.caption("註：ChatGPT 支援直接帶入提示詞；Claude 則需手動貼上。")
                
                with st.expander("如何利用 AI 深度分析？"):
                    st.write("""
                    1. **觀察預跑率**：若預跑率極高，代表市場資訊非常靈敏，追高營收公佈後的股票風險較大。
                    2. **比較漲幅差**：若預跑漲幅遠大於公佈後漲幅，建議尋找「營收即將爆發」的領先指標。
                    3. **防範出盡**：注意「利多出盡機率」，若此數值超過 30%，代表市場傾向獲利了結。
                    """)
        else:
            st.warning("⚠️ 查無符合條件的數據，請調整門檻或確認資料庫內容。")
else:
    st.error("無法建立資料庫連線。")
