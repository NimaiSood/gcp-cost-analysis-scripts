#!/usr/bin/env python3
"""
Production Hyperdisk Balanced Cost Analysis
===========================================

This is the production version that works with real Google Cloud Billing Export data.
Update the BILLING_PROJECT_ID to point to your project that contains the billing export.

Usage Instructions:
1. Ensure you have billing export enabled and data flowing to BigQuery
2. Update BILLING_PROJECT_ID and BILLING_DATASET_ID below
3. Run: python3 hyperdisk_balanced_production.py

Author: GitHub Copilot
Date: August 25, 2025
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from google.cloud import bigquery
import numpy as np

# Configuration - UPDATE THESE FOR YOUR ENVIRONMENT
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
BILLING_PROJECT_ID = "your-billing-project-id"  # UPDATE THIS
BILLING_DATASET_ID = "billing_export"  # UPDATE THIS
BILLING_TABLE_ID = f"gcp_billing_export_v1_{BILLING_ACCOUNT_ID.replace('-', '_')}"  # Usually this pattern

ANALYSIS_DAYS = 90  # Analyze last 90 days
MIN_COST_THRESHOLD = 1.0  # Minimum cost to include (USD)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function for production analysis"""
    logger.info("🚀 Starting Production Hyperdisk Balanced Storage Cost Analysis")
    logger.info("=" * 60)
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        
        # Build the full table reference
        billing_table = f"{BILLING_PROJECT_ID}.{BILLING_DATASET_ID}.{BILLING_TABLE_ID}"
        logger.info(f"Using billing table: {billing_table}")
        
        # Test table access
        try:
            table_ref = client.get_table(billing_table)
            logger.info(f"✅ Successfully accessed billing table with {table_ref.num_rows} rows")
        except Exception as e:
            logger.error(f"❌ Cannot access billing table: {e}")
            logger.error("Please check:")
            logger.error("1. BILLING_PROJECT_ID is correct")
            logger.error("2. BILLING_DATASET_ID is correct") 
            logger.error("3. Billing export is enabled and data is flowing")
            logger.error("4. You have BigQuery permissions")
            return
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=ANALYSIS_DAYS)
        
        logger.info(f"Analyzing costs from {start_date.date()} to {end_date.date()}")
        
        # Main analysis query
        query = f"""
        WITH hyperdisk_costs AS (
            SELECT 
                project.id as project_id,
                project.name as project_name,
                service.description as service_name,
                sku.description as sku_description,
                sku.id as sku_id,
                EXTRACT(DATE FROM usage_start_time) as usage_date,
                SUM(cost) as daily_cost,
                SUM(usage.amount) as usage_amount,
                usage.unit as usage_unit,
                location.location as location,
                currency as currency_code
            FROM `{billing_table}`
            WHERE 
                -- Filter for Hyperdisk Balanced storage
                (
                    LOWER(sku.description) LIKE '%hyperdisk%balanced%'
                    OR LOWER(sku.description) LIKE '%hyperdisk balanced%'
                    OR LOWER(sku.description) LIKE '%pd-balanced%'
                    OR LOWER(sku.description) LIKE '%balanced persistent disk%'
                )
                -- Associate with Compute Engine
                AND (
                    LOWER(service.description) LIKE '%compute%'
                    OR LOWER(service.description) LIKE '%engine%'
                )
                -- Date filter
                AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ANALYSIS_DAYS} DAY)
                AND usage_start_time < CURRENT_TIMESTAMP()
                -- Cost filter
                AND cost > {MIN_COST_THRESHOLD}
            GROUP BY 
                project.id, project.name, service.description,
                sku.description, sku.id, usage_date, usage.unit, 
                location.location, currency
        ),
        project_summaries AS (
            SELECT 
                project_id,
                project_name,
                COUNT(DISTINCT usage_date) as days_with_costs,
                MIN(usage_date) as first_cost_date,
                MAX(usage_date) as last_cost_date,
                SUM(daily_cost) as total_cost,
                AVG(daily_cost) as avg_daily_cost,
                MAX(daily_cost) as max_daily_cost,
                MIN(daily_cost) as min_daily_cost,
                STDDEV(daily_cost) as cost_stddev,
                STRING_AGG(DISTINCT sku_description, '; ' LIMIT 5) as sku_types,
                SUM(usage_amount) as total_usage,
                STRING_AGG(DISTINCT usage_unit, ', ') as usage_units,
                STRING_AGG(DISTINCT location, ', ') as locations,
                COUNT(DISTINCT sku_id) as unique_sku_count,
                COUNT(DISTINCT location) as location_count
            FROM hyperdisk_costs
            GROUP BY project_id, project_name
        ),
        cost_trends AS (
            SELECT 
                project_id,
                -- Calculate cost trend (last 30 days vs previous 30 days)
                SUM(CASE 
                    WHEN usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
                    THEN daily_cost 
                    ELSE 0 
                END) as cost_last_30_days,
                SUM(CASE 
                    WHEN usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) 
                         AND usage_date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    THEN daily_cost 
                    ELSE 0 
                END) as cost_prev_30_days,
                -- Weekly breakdown
                SUM(CASE 
                    WHEN usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                    THEN daily_cost 
                    ELSE 0 
                END) as cost_last_7_days,
                SUM(CASE 
                    WHEN usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
                         AND usage_date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    THEN daily_cost 
                    ELSE 0 
                END) as cost_prev_7_days
            FROM hyperdisk_costs
            GROUP BY project_id
        )
        SELECT 
            ps.*,
            ct.cost_last_30_days,
            ct.cost_prev_30_days,
            ct.cost_last_7_days,
            ct.cost_prev_7_days,
            CASE 
                WHEN ct.cost_prev_30_days > 0 THEN 
                    ROUND(((ct.cost_last_30_days - ct.cost_prev_30_days) / ct.cost_prev_30_days) * 100, 2)
                ELSE NULL 
            END as cost_change_percent_30d,
            CASE 
                WHEN ct.cost_prev_7_days > 0 THEN 
                    ROUND(((ct.cost_last_7_days - ct.cost_prev_7_days) / ct.cost_prev_7_days) * 100, 2)
                ELSE NULL 
            END as cost_change_percent_7d,
            DATE_DIFF(CURRENT_DATE(), ps.first_cost_date, DAY) as duration_days,
            CASE 
                WHEN ps.total_cost > 1000 THEN 'HIGH'
                WHEN ps.total_cost > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END as cost_category,
            ROUND(ps.total_cost * (30.0 / ps.days_with_costs), 2) as monthly_cost_estimate,
            ROUND(ps.total_cost / ps.days_with_costs, 2) as cost_per_day
        FROM project_summaries ps
        LEFT JOIN cost_trends ct ON ps.project_id = ct.project_id
        ORDER BY ps.total_cost ASC  -- Ascending order as requested
        """
        
        logger.info("🔍 Executing Hyperdisk Balanced cost analysis query...")
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to DataFrame
        df = results.to_dataframe()
        
        if df.empty:
            logger.warning("❌ No Hyperdisk Balanced costs found")
            print("\\n❌ No Hyperdisk Balanced storage costs found for the analysis period")
            print("\\nPossible reasons:")
            print("1. No Hyperdisk Balanced disks are being used")
            print("2. Costs are below the minimum threshold")
            print("3. Billing data is not yet available")
            return
        
        logger.info(f"✅ Found {len(df)} projects with Hyperdisk Balanced costs")
        
        # Generate timestamp for files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save detailed CSV
        csv_filename = f"hyperdisk_balanced_analysis_{timestamp}.csv"
        df.to_csv(csv_filename, index=False)
        logger.info(f"📄 Detailed CSV saved: {csv_filename}")
        
        # Generate Excel report
        excel_filename = f"hyperdisk_balanced_report_{timestamp}.xlsx"
        generate_excel_report(df, excel_filename, client, billing_table)
        
        # Print summary to console
        print_analysis_summary(df)
        
        logger.info(f"\\n📋 Excel report: {excel_filename}")
        logger.info(f"📄 CSV data: {csv_filename}")
        logger.info("\\n🎉 Analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        raise

def generate_excel_report(df: pd.DataFrame, filename: str, client: bigquery.Client, billing_table: str):
    """Generate comprehensive Excel report"""
    logger.info(f"📊 Generating Excel report: {filename}")
    
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1
        })
        
        money_format = workbook.add_format({'num_format': '$#,##0.00'})
        number_format = workbook.add_format({'num_format': '#,##0'})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        # 1. Executive Summary
        summary_data = {
            'Metric': [
                'Analysis Date',
                'Analysis Period (Days)',
                'Total Projects with Hyperdisk Balanced',
                'Total Hyperdisk Balanced Cost',
                'Average Cost per Project',
                'Highest Cost Project',
                'Highest Daily Cost',
                'Total Usage Days',
                'Average Duration (Days)',
                'High Cost Projects (>$1000)',
                'Medium Cost Projects ($100-$1000)',
                'Low Cost Projects (<$100)',
                'Projects with Cost Increase (30d)',
                'Projects with Cost Decrease (30d)',
                'Average Monthly Cost Estimate'
            ],
            'Value': [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ANALYSIS_DAYS,
                len(df),
                f"${df['total_cost'].sum():.2f}",
                f"${df['total_cost'].mean():.2f}",
                df.iloc[-1]['project_id'] if len(df) > 0 else 'N/A',  # Last in ascending order = highest cost
                f"${df['max_daily_cost'].max():.2f}",
                df['days_with_costs'].sum(),
                f"{df['duration_days'].mean():.1f}",
                len(df[df['cost_category'] == 'HIGH']),
                len(df[df['cost_category'] == 'MEDIUM']),
                len(df[df['cost_category'] == 'LOW']),
                len(df[df['cost_change_percent_30d'] > 0]),
                len(df[df['cost_change_percent_30d'] < 0]),
                f"${df['monthly_cost_estimate'].mean():.2f}"
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Executive Summary', index=False)
        
        # 2. Projects by Cost (Ascending Order)
        main_columns = [
            'project_id', 'project_name', 'total_cost', 'monthly_cost_estimate',
            'days_with_costs', 'duration_days', 'cost_last_30_days', 
            'cost_change_percent_30d', 'cost_category', 'avg_daily_cost', 
            'max_daily_cost', 'unique_sku_count', 'location_count',
            'first_cost_date', 'last_cost_date', 'locations'
        ]
        
        projects_df = df[main_columns].copy()
        projects_df.to_excel(writer, sheet_name='Projects by Cost (Ascending)', index=False)
        
        # 3. High Cost Projects (for easier identification)
        high_cost_df = df[df['cost_category'] == 'HIGH'].copy()
        if not high_cost_df.empty:
            # Sort high cost projects by cost descending to highlight the worst
            high_cost_df = high_cost_df.sort_values('total_cost', ascending=False)
            high_cost_df.to_excel(writer, sheet_name='High Cost Projects', index=False)
        
        # 4. Cost Trends Analysis
        trend_columns = [
            'project_id', 'total_cost', 'cost_last_30_days', 'cost_prev_30_days',
            'cost_change_percent_30d', 'cost_last_7_days', 'cost_prev_7_days',
            'cost_change_percent_7d', 'cost_category'
        ]
        trends_df = df[trend_columns].copy()
        trends_df = trends_df.sort_values('cost_change_percent_30d', ascending=False, na_position='last')
        trends_df.to_excel(writer, sheet_name='Cost Trends', index=False)
        
        # 5. Usage Details
        usage_columns = [
            'project_id', 'total_usage', 'usage_units', 'sku_types',
            'unique_sku_count', 'locations', 'location_count'
        ]
        usage_df = df[usage_columns].copy()
        usage_df.to_excel(writer, sheet_name='Usage Details', index=False)
    
    logger.info(f"✅ Excel report generated: {filename}")

def print_analysis_summary(df: pd.DataFrame):
    """Print comprehensive analysis summary"""
    print(f"\\n🔍 HYPERDISK BALANCED STORAGE ANALYSIS")
    print("=" * 70)
    print(f"📅 Analysis Period: Last {ANALYSIS_DAYS} days")
    print(f"💰 Minimum Cost Threshold: ${MIN_COST_THRESHOLD}")
    print(f"📊 Total Projects Found: {len(df)}")
    print(f"💵 Total Cost: ${df['total_cost'].sum():.2f}")
    print(f"📈 Average Cost per Project: ${df['total_cost'].mean():.2f}")
    print(f"📅 Average Duration: {df['duration_days'].mean():.1f} days")
    print("")
    
    # Cost distribution
    high_cost = len(df[df['cost_category'] == 'HIGH'])
    medium_cost = len(df[df['cost_category'] == 'MEDIUM'])
    low_cost = len(df[df['cost_category'] == 'LOW'])
    
    print("📈 COST DISTRIBUTION:")
    print(f"   🔴 High Cost Projects (>$1000): {high_cost}")
    print(f"   🟡 Medium Cost Projects ($100-$1000): {medium_cost}")
    print(f"   🟢 Low Cost Projects (<$100): {low_cost}")
    print("")
    
    # Top 10 highest cost projects (sorted descending for highlighting)
    df_desc = df.sort_values('total_cost', ascending=False)
    print("🏆 TOP 10 HIGHEST COST PROJECTS:")
    print("-" * 80)
    print(f"{'Rank':<4} {'Project ID':<25} {'Total Cost':<12} {'Duration':<12} {'Trend':<8}")
    print("-" * 80)
    
    for i, (_, row) in enumerate(df_desc.head(10).iterrows(), 1):
        duration_str = f"{row['duration_days']:.0f} days"
        trend = "↗️" if pd.notna(row['cost_change_percent_30d']) and row['cost_change_percent_30d'] > 5 else "↘️" if pd.notna(row['cost_change_percent_30d']) and row['cost_change_percent_30d'] < -5 else "→"
        print(f"{i:<4} {row['project_id']:<25} ${row['total_cost']:<11.2f} {duration_str:<12} {trend:<8}")
    
    print("")
    
    # Cost trends analysis
    increasing_costs = df[df['cost_change_percent_30d'] > 5]
    decreasing_costs = df[df['cost_change_percent_30d'] < -5]
    
    print("📊 COST TRENDS (30-day comparison):")
    print(f"   📈 Projects with increasing costs (>5%): {len(increasing_costs)}")
    print(f"   📉 Projects with decreasing costs (<-5%): {len(decreasing_costs)}")
    
    if len(increasing_costs) > 0:
        print(f"   🚨 Highest cost increase: {increasing_costs['cost_change_percent_30d'].max():.1f}%")
    if len(decreasing_costs) > 0:
        print(f"   💰 Biggest cost decrease: {decreasing_costs['cost_change_percent_30d'].min():.1f}%")
    
    print("")
    
    # Show all projects in ascending order as requested
    print("📊 ALL PROJECTS (ASCENDING ORDER BY COST):")
    print("-" * 90)
    print(f"{'Project ID':<30} {'Cost':<12} {'Duration':<15} {'30d Change':<12} {'Status':<8}")
    print("-" * 90)
    
    for _, row in df.iterrows():
        duration_str = f"{row['duration_days']:.0f} days"
        change_str = f"{row['cost_change_percent_30d']:+.1f}%" if pd.notna(row['cost_change_percent_30d']) else "N/A"
        status = "🔴" if row['cost_category'] == 'HIGH' else "🟡" if row['cost_category'] == 'MEDIUM' else "🟢"
        print(f"{row['project_id']:<30} ${row['total_cost']:<11.2f} {duration_str:<15} {change_str:<12} {status}")

if __name__ == "__main__":
    print("\\n" + "="*60)
    print("🔧 PRODUCTION HYPERDISK BALANCED ANALYSIS")
    print("="*60)
    print("\\n⚠️  CONFIGURATION REQUIRED:")
    print("1. Update BILLING_PROJECT_ID in the script")
    print("2. Update BILLING_DATASET_ID if different")
    print("3. Ensure billing export is enabled")
    print("4. Verify BigQuery permissions")
    print("\\n" + "="*60)
    
    # Check if configuration looks updated
    if BILLING_PROJECT_ID == "your-billing-project-id":
        print("\\n❌ Please update BILLING_PROJECT_ID in the script before running!")
        print("   Look for: BILLING_PROJECT_ID = 'your-billing-project-id'")
        print("   Update to: BILLING_PROJECT_ID = 'your-actual-project-id'")
        exit(1)
    
    main()
