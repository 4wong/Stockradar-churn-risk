# StockRadar

## Project Overview

This project focuses on **StockRadar**, which is a Shopify app that allows merchants to manage stock by notifying them through "Back in Stock" alerts via email and SMS.

After the app launched its **$29/month Pro Tier**, a trend came about where a significant number of users were unsubscribing between day 60 and 90. This system was built to identify the reasoning behind this churn and create an "early warning" dashboard to flag at-risk customers before they cancel.

## The Business Problem

The app was experiencing a "leaky bucket" problem. Despite constant new signups, the churn rate in the Pro Tier was decreasing monthly recurring revenue. My analysis focused on two main failure modes:

1. **Technical Friction:**  
   Integration errors that disallowed SMS alerts from correctly sending.

2. **Low Value Realisation:**  
   Merchants not seeing enough "Recovered Sales" to justify the $29 per month subscription cost.

## 🛠 My Process

1. **Data Modeling:**  
   Incorporated billing data with raw app activity logs into a "Customer 360" view.

2. **Feature Engineering:**  
   Calculated behavioural metrics such as `sms_fail_rate_30d` and `days_since_last_dashboard`.

3. **Risk Scoring:**  
   Developed a classification model to assign a churn probability to each merchant.

4. **Insight Generation:**  
   Identified key patterns to predict churn to help prioritise technical fixes and customer outreach.

## Results

A baseline churn risk model was trained on Pro-tier merchants using leakage-safe, point-in-time features that were derived from subscription events, application usage, and recovered revenue.

**Model performance (holdout set):**
- **ROC-AUC:** 0.82  
- **Precision @ Top 10% risk:** 70%  
- **Lift @ Top 10%:** 2.34× over random selection  

By focusing on the top 10% highest-risk merchants, the model is able to identify approximately 70% of true churners in advance, which enables efficient and relevant targeted retention outreach.

**Key churn drivers identified:**
- High SMS failure rate and integration errors (technical friction)
- Low engagement recency (days since last dashboard interaction)
- Lower realised ROI relative to the subscription cost

A logistic regression model was selected to prioritise interpretability and practical decision-making. The resulting coefficients clearly showcase whether churn risk is driven by technical issues or low engagement, allowing teams to respond with appropriate interventions.

## ⚙️ Tech Stack

- **SQL:** Data cleaning, deduplication, and monthly usage aggregations.
- **Python:** Feature engineering and machine learning.
- **Data Visualisation:** Churn analysis and feature importance rankings.

---

**Note on Data Privacy:**  
To comply with PII (Personally Identifiable Information) standards and protect data sensitive to the business, the datasets provided in this repository are sanitised based on real-world SaaS logic. The underlying schema and statistical distributions have been preserved to reflect actual production challenges.
