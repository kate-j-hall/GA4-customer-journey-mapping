# 🛒 GA4 Customer Journey Case Study

This repository showcases a customer journey mapping project for an eCommerce retail site using Google Analytics 4 (GA4) sample data. It integrates SQL, Looker Studio visualizations, and actionable insights to understand and optimize discovery, engagement, and conversion.

---

## 📦 Project Overview

- **Dataset:** GA4 public sample data (Google Merchandise Store)
- **Goal:** Map and analyze customer journeys across key discovery methods (navigation, search, promotions, PDP, PLP)
- **Approach:**
  - Wrote advanced SQL using CTEs and window functions
  - Grouped customer journeys across discovery actions and funnel steps
  - Visualized insights in Looker Studio
  - Summarized key business recommendations

👉 **Looker Studio Report:**  
[View Report](https://lookerstudio.google.com/reporting/79f405f0-5605-44bb-8825-52d669994779)

---

## 🛠 Repository Contents

`sql/`:
  - sankey_grouped.sql # Builds grouped dataset for Sankey diagrams
  - customer_journey.sql # Builds structured customer journey dataset

`reports/`
  - summary_report.md # One-pager with key findings and recommendations

README.md # Project overview and documentation

---

## 💡 Key Insights

✅ **Navigation** is the main driver of volume, but  
✅ **Search** delivers the strongest conversion rate and highest revenue per unit.  
✅ **Product Detail Page (PDP)** landings show small volume but high value per purchase, suggesting untapped opportunity.  
✅ **Promotions** drive the third-highest PDP volume but have the weakest conversion — indicating areas for deeper investigation.

---

## 📊 Recommendations

- Enhance and promote **site search** to capture high-converting intent.
- Improve **navigation structure** to better guide product discovery.
- Optimize **PDPs** with recommendations and cross-sells to increase conversion.
- Audit **promotions** to identify underperforming campaigns.
- Perform a **data quality audit** to address collection gaps (impressions, missing SKUs, etc.).

---

## 📈 Next Steps

- Drill down to **product-level** and **category-level** performance.
- Explore **channel-level drivers** for PDP and PLP landings.
- Refine **personalization strategies** based on customer journey mapping.

---

## 👩‍💻 How to Use

1️⃣ Run the SQL scripts (`/sql`) in BigQuery against the GA4 sample dataset: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`

2️⃣ Connect the resulting tables to Looker Studio or another BI tool.

3️⃣ Review the `summary_report.md` for top-level business recommendations.

---

## 🧩 About This Project

This project was built to demonstrate:

- Customer journey mapping with SQL
- Behavioral funnel analysis
- Data storytelling with BI tools
- Translating data insights into business actions

---

**Created by:** Kate Hall (July 2025)
