# Retail Analytics Power BI Dashboard

## Overview

This Power BI dashboard provides an analytical layer for the
Retail Data Engineering Pipeline. Power BI connects to the Gold
dimensional model created in Snowflake.

## Dashboard Pages

### Executive Dashboard

![Executive Dashboard](screenshots/Retail_Executive_Dashboard.png)

Provides an overview of business performance including:

- Total Revenue
- Total Profit
- Total Orders
- Average Order Value
- Profit Margin
- Revenue trends

### Product Dashboard

![Product Dashboard](screenshots/Retail_Product_Dashboard.png)

Analyzes product performance including:

- Revenue by category
- Profit by category
- Product performance
- Revenue vs. profit margin
- Units sold

### Customer Dashboard

![Customer Dashboard](screenshots/Retail_Customer_Dashboard.png)

Analyzes customer behavior without exposing unnecessary PII.

Includes:

- Total Customers
- Active vs. Inactive Customers
- Customer segments
- Revenue by customer segment
- Customer activity

## Data Model

Power BI uses the Snowflake Gold dimensional model:

FACT_SALES
DIM_CUSTOMER
DIM_PRODUCT
DIM_DATE

## Key DAX Measures

- Total Revenue
- Total Profit
- Total Orders
- Total Units Sold
- Average Order Value
- Profit Margin %
- Total Customers
