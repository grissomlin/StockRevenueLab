# StockRevenueLab
本專案致力於研究台股全市場（上市、上櫃、興櫃、ETF）於 2020 至 2025 年間的股價變動規律，並結合財報營收（年增率、月增率）進行多因子量化分析。專案內含完整的數據精煉過程，旨在提供開發者與投資者一套乾淨、精準的歷史數據與分析範本。
# 🧪 StockRevenueLab: 台股量化財報研究室

StockRevenueLab 是一個整合「自動化數據爬蟲」、「雲端資料庫同步」與「AI 智能量化分析」的台股研究專案。本專案透過分析超過 16 萬筆真實數據，揭開企業業績與股價漲幅之間的神秘面紗。

## 🌐 專案資源導覽

* **🏠 分析沙龍中心**: [StockRevenueLab 數據觀測站](https://vocus.cc/salon/grissomlin/room/695636ee0c0c0689d1e2aa9f)
* **📈 線上互動儀表板**: [立即開啟 Streamlit App](https://stockrevenuelab-jlfqljhuy5q9appppwy2bg8a.streamlit.app/)
* **🐍 自動化爬蟲工具**: [Google Colab 一鍵執行](https://colab.research.google.com/github/grissomlin/StockRevenueLab/blob/main/%E3%80%8C%E8%B2%A1%E5%A0%B1%E5%84%80%E8%A1%A8%E6%9D%BF%E7%9B%B8%E9%97%9C%E7%A8%8B%E5%BC%8F%E7%A2%BC%E3%80%8D_github.ipynb)

---

## 🛠️ 三大核心儀表板功能說明

### 1. 📊 趨勢觀測站：飆股基因對帳單
**用途**：利用「漲幅分組（Binning）」技術，回溯歷史數據，找出營收成長與股價漲幅之間的真實相關性。
* **核心技術**：將下跌設為 10% 一級，上漲設為 100% 一級，觀察不同漲幅區間的營收 YoY/MoM 表現。
* **特色功能**：內建 AI 智能助手，自動診斷年度業績王特徵。
* **📖 詳細教學**: [從 16 萬筆數據看透飆股基因](https://vocus.cc/article/69563da2fd89780001da3c94)
* <img width="1903" height="913" alt="image" src="https://github.com/user-attachments/assets/8de1acf8-768b-4d2c-bf9f-604caef1ddd2" />


### 2. 🎲 機率研究室 2.0：營收爆發機率統計
**用途**：找出統計勝率。分析營收爆發次數（MoM 或 YoY 達標）與年度報酬率的邏輯關係。
* **核心技術**：使用 `PERCENTILE_CONT` 計算漲幅中位數，排除極端妖股干擾。
* **特色功能**：提供「綜合評分」模型，兼顧賺多、賠少、機率高三大投資關鍵。
* **📖 詳細教學**: [營收爆發幾次，股價翻倍機率最高？](https://vocus.cc/article/695640b8fd89780001db0f44)

<img width="1903" height="879" alt="image" src="https://github.com/user-attachments/assets/239fff22-5250-44d6-a79a-1164362e17ca" />
<img width="1585" height="782" alt="image" src="https://github.com/user-attachments/assets/83c48af7-0b60-4726-b414-0a185de8535a" />
<img width="1474" height="785" alt="image" src="https://github.com/user-attachments/assets/1a4e7839-84c3-4db2-b7da-561d06703ee7" />


### 3. ⏱️ 公告行為研究室 4.3：公告效應量化診斷
**用途**：探討營收利多公告後，市場呈現的是「追價動能」還是「利多出盡」。
* **核心技術**：採用「事件研究法」，分析公告日 (T) 前後一個月的股價分佈（偏度、峰度、變異係數）。
* **特色功能**：偵測「資訊不對稱」現象，觀察大戶是否提前佈局或偷跑。
* **📖 詳細教學**: [營收利多公告後，該追還是該賣？](https://vocus.cc/article/695642d5fd89780001dba290)

<img width="1917" height="911" alt="image" src="https://github.com/user-attachments/assets/9325c5bb-93c9-498e-93ac-6a72eab0daa6" />
<img width="1545" height="820" alt="image" src="https://github.com/user-attachments/assets/32bed836-a865-4cea-9fd8-a966ee078374" />
<img width="1562" height="828" alt="image" src="https://github.com/user-attachments/assets/62063b6d-0e8e-4768-98cc-934a687106e2" />
<img width="1540" height="891" alt="image" src="https://github.com/user-attachments/assets/11ba0de5-6ce4-4912-9c47-513e467b4aa3" />

---

## 🚀 技術架構與教學

本專案採用的技術棧包括 Python, Streamlit, PostgreSQL (Supabase), 以及 yfinance。

* **數據抓取與同步**: 如何自動抓取月營收與股價並同步至雲端資料庫。
  * **📖 詳細教學**: [打造個人台股財報儀表板](https://vocus.cc/article/695636c3fd89780001d873bd)
* **AI 診斷引擎**: 整合 LLM 提示詞工程，自動將統計數據轉化為量化診斷報告。

---

## 🧋 支持與鼓勵

如果你覺得這個專案對你的投資研究有幫助，歡迎請我喝杯珍奶，補充創作靈感！

👉 **[Boba Boost! 贊助連結](https://vocus.cc/pay/donate/606146a3fd89780001ba32e9?donateSourceType=article&donateSourceRefID=695640b8fd89780001db0f44)**

---
Developed by **StockRevenueLab** | 讓數據說真話，揭開台股漲幅的神秘面紗。
