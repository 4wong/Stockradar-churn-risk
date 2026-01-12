# \# StockRadar

# 

# \### Project Overview

# This project focuses on \*\*StockRadar\*\*, which is a Shopify app that allows sellers to manage stock by notifying them through "Back in Stock" alerts via email and SMS. 

# 

# After the app launched its \*\*$29/month Pro Tier\*\*, a trend came about where a significant number of users were unsubscribing between day 60 and 90. This system was built to identify the reasoning behind this churn and create an "early warning" dashboard to flag at-risk customers before they cancel.

# 

# \### The Business Problem

# The app was experiencing a "leaky bucket" problem. Despite constant new signups, the churn rate in the Pro Tier was decreasing monthly recurring revenue. My analysis focused on two main failure modes:

# 1\. \*\*Technical Friction:\*\* Integration errors that disallowed SMS alerts from correctly sending.

# 2\. \*\*Low Value Realisation:\*\* Sellers not seeing enough "Recovered Sales" to justify the $29 per month subscription cost.

# 

# \### 🛠 My Process

# 1\. \*\*Data Modeling:\*\* Incorporated billing data with raw app activity logs into a "Customer 360" view.

# 2\. \*\*Feature Engineering:\*\* Calculated behavioural metrics such as `sms\_success\_rate` and `days\_since\_last\_login`.

# 3\. \*\*Risk Scoring:\*\* Developed a classification model to assign a churn probability to each merchant.

# 4\. \*\*Insight Generation:\*\* Identified key patterns to predict churn to help prioritise technical fixes and customer outreach.

# 

# \### ⚙️ Tech Stack

# \* \*\*SQL:\*\* Data cleaning, deduplication, and monthly usage aggregations.

# \* \*\*Python:\*\* Feature engineering and machine learning (XGBoost/Scikit-Learn).

# \* \*\*Data Visualisation:\*\* Churn analysis and feature importance rankings.

# 

# ---

# \*\*Note on Data Privacy:\*\* To comply with PII (Personally Identifiable Information) standards and protect data sensitive to the business, the datasets provided in this repository are a mockup based on real-world SaaS logic. The underlying schema and statistical distributions have been preserved to reflect actual production challenges.

