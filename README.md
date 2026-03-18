# Shelf_Life_Analysis_Food_And_Beverages

# 🥫 Food & Beverage Supply Chain & Shelf-Life Analytics Platform

An end-to-end, AI-powered supply chain management and shelf-life tracking system. This platform provides real-time visibility into the lifecycle of perishable goods (Food & Beverage), tracking batches from factory manufacturing to retailer stock. It leverages Machine Learning for demand forecasting  for strategic anomaly resolution and supply chain insights.

## ✨ Key Features

* **End-to-End Batch Traceability:** Tracks product batches across 6 critical stages: `Factory Manufacturing` ➡️ `Factory Dispatch` ➡️ `Dealer Receipt` ➡️ `Dealer Dispatch` ➡️ `Retailer Receipt` ➡️ `Retailer Stock`.
* **Dynamic Shelf-Life Monitoring:** Automatically calculates product expiration based on product categories (e.g., milk, biscuits, chocolates). Flags inventory statuses as *Fresh*, *Moderate*, *Critical*, or *Expired*.
* **AI-Powered Insights :** Generates strategic recommendations for transit bottlenecks, inventory holding costs, and anomaly resolutions. Includes an interactive Supply Chain Chatbot for custom queries.
* **Advanced Demand Forecasting:** Uses **Holt-Winters Exponential Smoothing** and **Linear Regression** to predict future dealer and retailer demand, analyzing seasonality and historical sales trends.
* **Automated Anomaly Detection:** Identifies transit-time delays, expired/stagnant stock (e.g., shelf life > 30 days), and data consistencies across the pipeline.
* **Live Tracking:** Integrates with **Traccar** for live telemetry of shipments via WebSockets.

## 🛠️ Tech Stack

* **Backend & API:** Python 3, Flask, Flask-SocketIO
* **Data Processing & Analytics:** Pandas, NumPy
* **Machine Learning (Forecasting):** Scikit-Learn, Statsmodels (Time-Series Analysis)
* **AI Integration:** OpenAI Python SDK (Via OpenRouter)
* **Tracking:** Traccar API Webhooks
* **Database:** SQLite (for local anomaly logging and telemetry storage)

## 📦 Required Datasets

To run the full end-to-end flow, the system expects CSV datasets matching the following supply chain stages:
1. `Factory_Manufacturing.csv`
2. `Factory_Dispatch.csv`
3. `Dealer_Receipt.csv`
4. `Dealer_Dispatch.csv`
5. `Retailer_Receipt.csv`
6. `Retailer_Stock.csv`

*(Note: Data should contain identifiers like `Batch_ID`, `Dealer_Name`, `Retailer_Name`, dates, and quantities).*

## 🚀 Installation & Setup

### 1. Clone the repository
```bash



## Environment Variables:
# AI Integration
OPENROUTER_API_KEY=your_openrouter_api_key_here

# Traccar GPS Integration
TRACCAR_API_KEY=your_traccar_api_key_here
TRACCAR_URL=http://localhost:8082
TRACCAR_USER=your_traccar_username
TRACCAR_PASS=your_traccar_password
git clone [https://github.com/Pranit-repo/shelf-life-analytics.git](https://github.com/yourusername/shelf-life-analytics.git)
cd shelf-life-analytics
