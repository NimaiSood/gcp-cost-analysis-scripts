#!/usr/bin/env python3
"""
Simple Hyperdisk Balanced Cost Analysis with Real BigQuery Connection
====================================================================

This script demonstrates connecting to real BigQuery billing data.
Configure your billing project and dataset below to connect to real data.

Usage:
1. Update BILLING_PROJECT_ID and BILLING_DATASET_ID below
2. Run: python3 hyperdisk_simple_connection.py

Author: GitHub Copilot
Date: August 25, 2025
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import sys

# Configuration - UPDATE THESE FOR YOUR ENVIRONMENT
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"

# Option 1: Manual Configuration (most reliable)
BILLING_PROJECT_ID = "netapp-g1p-billing"  # Set to your billing project ID
BILLING_DATASET_ID = "g1p_cloud_billing_data"  # Set to your billing dataset ID

# Option 2: Try specific billing projects we found
KNOWN_BILLING_PROJECTS = [
    "vsa-billing-09", "vsa-billing-08", "vsa-billing-07", 
    "vsa-billing-06", "vsa-billing-05", "vsa-billing-04"
]

# Standard billing table naming
BILLING_TABLE_ID = f"gcp_billing_export_resource_v1_{BILLING_ACCOUNT_ID.replace('-', '_')}"

ANALYSIS_DAYS = 90
MIN_COST_THRESHOLD = 1.0

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_manual_configuration():
    """Check if manual configuration is provided, if not try auto-discovery"""
    try:
        if BILLING_PROJECT_ID and BILLING_DATASET_ID:
            logger.info("✅ Using manual configuration")
            return True, BILLING_PROJECT_ID, BILLING_DATASET_ID
        
        logger.info("🔍 No manual configuration - trying auto-discovery...")
        
        # Try to discover billing tables in known projects
        try:
            client = bigquery.Client()
        except Exception as e:
            logger.error(f"❌ Failed to initialize BigQuery client: {e}")
            logger.error("Please check:")
            logger.error("1. Google Cloud SDK is installed and configured")
            logger.error("2. Application Default Credentials are set")
            logger.error("3. You have appropriate permissions")
            return False, None, None
        
        for project_id in KNOWN_BILLING_PROJECTS:
            logger.info(f"🔍 Checking project: {project_id}")
            
            # Try common dataset names
            for dataset_id in ["billing_export", "billing", "gcp_billing_export"]:
                try:
                    full_table_id = f"{project_id}.{dataset_id}.{BILLING_TABLE_ID}"
                    table = client.get_table(full_table_id)
                    logger.info(f"✅ Found billing table: {full_table_id}")
                    return True, project_id, dataset_id
                except Exception as e:
                    logger.debug(f"Table {full_table_id} not accessible: {e}")
                    continue
                    
            # Try to list datasets in the project
            try:
                datasets = list(client.list_datasets(project=project_id))
                for dataset in datasets:
                    if any(keyword in dataset.dataset_id.lower() for keyword in ['billing', 'export']):
                        try:
                            full_table_id = f"{project_id}.{dataset.dataset_id}.{BILLING_TABLE_ID}"
                            table = client.get_table(full_table_id)
                            logger.info(f"✅ Found billing table: {full_table_id}")
                            return True, project_id, dataset.dataset_id
                        except Exception as e:
                            logger.debug(f"Table {full_table_id} not accessible: {e}")
                            continue
            except Exception as e:
                logger.debug(f"Cannot access project {project_id}: {e}")
                continue
        
        logger.error("❌ Auto-discovery failed!")
        logger.error("Please update the following in the script:")
        logger.error(f"  BILLING_PROJECT_ID = 'your-actual-project-id'")
        logger.error(f"  BILLING_DATASET_ID = 'your-actual-dataset'")
        logger.error("\nExample:")
        logger.error(f"  BILLING_PROJECT_ID = 'my-billing-project'")
        logger.error(f"  BILLING_DATASET_ID = 'billing_export'")
        return False, None, None
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in configuration check: {e}")
        return False, None, None

def test_billing_table_connection(project_id, dataset_id):
    """Test connection to billing table with comprehensive error handling"""
    try:
        # Initialize BigQuery client with error handling
        try:
            client = bigquery.Client()
        except Exception as e:
            logger.error(f"❌ Failed to initialize BigQuery client: {e}")
            logger.error("Troubleshooting steps:")
            logger.error("1. Install Google Cloud SDK: gcloud auth application-default login")
            logger.error("2. Set project: gcloud config set project YOUR_PROJECT_ID")
            logger.error("3. Check permissions for BigQuery")
            return None, None
        
        # Validate input parameters
        if not project_id or not dataset_id:
            logger.error("❌ Invalid project or dataset ID")
            return None, None
        
        # Build table reference
        full_table_id = f"{project_id}.{dataset_id}.{BILLING_TABLE_ID}"
        
        logger.info(f"🔍 Testing connection to: {full_table_id}")
        
        # Try to get table metadata with timeout
        try:
            table = client.get_table(full_table_id)
        except Exception as e:
            if "404" in str(e):
                logger.error(f"❌ Table not found: {full_table_id}")
                logger.error("Possible issues:")
                logger.error(f"1. Project '{project_id}' doesn't exist or not accessible")
                logger.error(f"2. Dataset '{dataset_id}' doesn't exist")
                logger.error(f"3. Table '{BILLING_TABLE_ID}' doesn't exist")
                logger.error("4. Billing export not configured for this billing account")
            elif "403" in str(e):
                logger.error(f"❌ Access denied to: {full_table_id}")
                logger.error("Required permissions:")
                logger.error("1. BigQuery Data Viewer role")
                logger.error("2. Access to the billing project")
                logger.error("3. Billing Account Viewer role (if applicable)")
            else:
                logger.error(f"❌ Connection failed: {e}")
            return None, None
        
        logger.info(f"✅ Successfully connected to billing table!")
        logger.info(f"   Table: {full_table_id}")
        logger.info(f"   Rows: {table.num_rows:,}")
        logger.info(f"   Size: {table.num_bytes / (1024**3):.2f} GB")
        logger.info(f"   Created: {table.created}")
        logger.info(f"   Last Modified: {table.modified}")
        
        # Test data query with error handling
        test_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN 1 END) as recent_rows,
            MIN(usage_start_time) as earliest_date,
            MAX(usage_start_time) as latest_date,
            COUNT(DISTINCT project.id) as unique_projects
        FROM `{full_table_id}`
        LIMIT 1
        """
        
        logger.info("🔍 Testing data accessibility...")
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.dry_run = True  # First test with dry run
            job_config.use_query_cache = False
            
            dry_run_job = client.query(test_query, job_config=job_config)
            logger.debug("✅ Query syntax validation passed")
            
            # Now run the actual query with timeout
            job_config.dry_run = False
            job_config.maximum_bytes_billed = 10 * 1024**3  # Limit to 10GB
            
            query_job = client.query(test_query, job_config=job_config)
            result = list(query_job.result(timeout=60))  # 60 second timeout
            
            if not result:
                logger.warning("⚠️ Query returned no results")
                return None, None
                
            row = result[0]
            
        except Exception as e:
            if "timeout" in str(e).lower():
                logger.error("❌ Query timeout - table might be very large")
                logger.error("Try reducing the analysis period or contact your admin")
            elif "exceeded" in str(e).lower():
                logger.error("❌ Query would process too much data")
                logger.error("Consider filtering or using a smaller time window")
            else:
                logger.error(f"❌ Data access test failed: {e}")
            return None, None
        
        logger.info(f"📊 Data Summary:")
        logger.info(f"   Total Records: {row.total_rows:,}")
        logger.info(f"   Recent Records (30d): {row.recent_rows:,}")
        logger.info(f"   Date Range: {row.earliest_date.date()} to {row.latest_date.date()}")
        logger.info(f"   Unique Projects: {row.unique_projects:,}")
        
        # Validate data freshness
        from datetime import datetime, timedelta
        if row.latest_date.date() < (datetime.now().date() - timedelta(days=2)):
            logger.warning(f"⚠️ Data might be stale - last update: {row.latest_date.date()}")
        
        return client, full_table_id
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in connection test: {e}")
        logger.error("\nGeneral troubleshooting steps:")
        logger.error("1. Verify BILLING_PROJECT_ID is correct")
        logger.error("2. Verify BILLING_DATASET_ID is correct")  
        logger.error("3. Check that billing export is enabled")
        logger.error("4. Ensure you have BigQuery permissions")
        logger.error("5. Verify the billing account ID is correct")
        return None, None

def search_for_hyperdisk_balanced(client, manual_config):
    """Search for Hyperdisk Balanced storage costs."""
    try:
        start_time = time.time()
        logger.info("🔍 Searching for Hyperdisk Balanced storage costs...")
        
        # First, let's sample the data to understand the structure
        sample_query = """
        SELECT 
            sku.description,
            service.description,
            usage_start_time,
            project.id as project_id,
            cost
        FROM `{project_id}.{dataset_id}.{table_name}`
        WHERE 
            sku.description LIKE '%Hyperdisk%'
        LIMIT 10
        """.format(
            project_id=manual_config['billing_project_id'],
            dataset_id=manual_config['dataset_id'],
            table_name=manual_config['table_name']
        )
        
        logger.info("📊 Running sample query to understand data structure...")
        
        # Configure job with safe limits
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=1 * 1024**3,  # 1GB limit for sample
            use_query_cache=True,
            dry_run=False
        )
        
        try:
            sample_job = client.query(sample_query, job_config=job_config, timeout=30)
            sample_results = sample_job.result(timeout=30)
            
            logger.info("✅ Sample query successful! Found example records:")
            for row in sample_results:
                logger.info(f"   SKU: {row.sku_description}")
                logger.info(f"   Service: {row.service_description}")
                logger.info(f"   Project: {row.project_id}")
                logger.info(f"   Cost: ${row.cost:.4f}")
                logger.info(f"   Date: {row.usage_start_time}")
                logger.info("   ---")
            
            # Now run the actual analysis query with partitioning
            main_query = """
            SELECT 
                project.id as project_id,
                project.name as project_name,
                location.location as location,
                SUM(cost) as total_cost,
                SUM(usage.amount) as total_usage_gb,
                COUNT(DISTINCT resource.name) as resource_count
            FROM `{project_id}.{dataset_id}.{table_name}`
            WHERE 
                sku.description LIKE '%Hyperdisk Balanced%'
                AND export_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
                AND cost > 0
                AND service.description = 'Compute Engine'
            GROUP BY project_id, project_name, location
            HAVING total_cost > 0.01
            ORDER BY total_cost DESC
            LIMIT 50
            """.format(
                project_id=manual_config['billing_project_id'],
                dataset_id=manual_config['dataset_id'],
                table_name=manual_config['table_name']
            )
        
        try:
            # Set up job configuration with safety limits
            job_config = bigquery.QueryJobConfig()
            job_config.maximum_bytes_billed = 50 * 1024**3  # Limit to 50GB
            job_config.use_query_cache = True
            
            query_job = client.query(test_query, job_config=job_config)
            result = list(query_job.result(timeout=120))  # 2 minute timeout
            
            if not result:
                logger.warning("❌ Search query returned no results")
                return None
                
            row = result[0]
            
        except Exception as e:
            if "timeout" in str(e).lower():
                logger.error("❌ Search query timeout")
                logger.error("The billing table is very large. Try:")
                logger.error("1. Reducing ANALYSIS_DAYS (currently {ANALYSIS_DAYS})")
                logger.error("2. Increasing MIN_COST_THRESHOLD (currently ${MIN_COST_THRESHOLD})")
                logger.error("3. Running during off-peak hours")
            elif "exceeded" in str(e).lower():
                logger.error("❌ Query would process too much data")
                logger.error("Try reducing the analysis period or filtering criteria")
            elif "quota" in str(e).lower():
                logger.error("❌ BigQuery quota exceeded")
                logger.error("Wait a few minutes and try again, or contact your admin")
            else:
                logger.error(f"❌ Search query failed: {e}")
            return None
        
        # Validate results
        if not hasattr(row, 'hyperdisk_records') or row.hyperdisk_records is None:
            logger.error("❌ Invalid query results structure")
            return None
        
        if row.hyperdisk_records > 0:
            logger.info(f"✅ Found Hyperdisk Balanced data!")
            logger.info(f"   Records: {row.hyperdisk_records:,}")
            logger.info(f"   Projects: {row.projects_with_hyperdisk}")
            logger.info(f"   Total Cost: ${row.total_hyperdisk_cost:.2f}")
            logger.info(f"   Sample SKUs: {row.sample_skus}")
            
            # Validate cost amount
            if row.total_hyperdisk_cost < 0:
                logger.warning("⚠️ Negative cost detected - please verify billing data")
            elif row.total_hyperdisk_cost > 1000000:  # > $1M
                logger.warning("⚠️ Very high cost detected - please verify results")
            
            # Run full analysis
            return run_full_analysis(client, billing_table)
        else:
            logger.warning("❌ No Hyperdisk Balanced storage costs found")
            logger.warning("Possible reasons:")
            logger.warning("1. No Hyperdisk Balanced disks in use during analysis period")
            logger.warning(f"2. Costs below minimum threshold (${MIN_COST_THRESHOLD})")
            logger.warning(f"3. Outside analysis time window ({ANALYSIS_DAYS} days)")
            logger.warning("4. Different SKU naming conventions")
            
            # Suggest broader search
            logger.info("💡 Try broadening the search:")
            logger.info("1. Increase ANALYSIS_DAYS")
            logger.info("2. Decrease MIN_COST_THRESHOLD")
            logger.info("3. Check for different disk types")
            return None
            
    except Exception as e:
        logger.error(f"❌ Unexpected error in Hyperdisk search: {e}")
        logger.error("This might indicate:")
        logger.error("1. Billing table schema changes")
        logger.error("2. Network connectivity issues")
        logger.error("3. BigQuery service issues")
        return None

def run_full_analysis(client, billing_table):
    """Run the complete Hyperdisk Balanced analysis with comprehensive error handling"""
    try:
        logger.info("🚀 Running full Hyperdisk Balanced analysis...")
        
        # Validate inputs
        if not client or not billing_table:
            logger.error("❌ Invalid client or billing table for analysis")
            return None
        
        analysis_query = f"""
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
                location.location as location_name,
                -- Extract resource information
                resource.name as resource_name
            FROM `{billing_table}`
            WHERE 
                (
                    LOWER(sku.description) LIKE '%hyperdisk%balanced%'
                    OR LOWER(sku.description) LIKE '%hyperdisk balanced%'
                    OR LOWER(sku.description) LIKE '%pd-balanced%'
                    OR LOWER(sku.description) LIKE '%balanced persistent disk%'
                )
                AND (
                    LOWER(service.description) LIKE '%compute%'
                    OR LOWER(service.description) LIKE '%engine%'
                )
                AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ANALYSIS_DAYS} DAY)
                AND cost > {MIN_COST_THRESHOLD}
            GROUP BY 
                project.id, project.name, service.description,
                sku.description, sku.id, usage_date, usage.unit, location_name,
                resource.name
        ),
        resource_counts AS (
            SELECT 
                project_id,
                COUNT(DISTINCT resource_name) as unique_resources,
                COUNT(DISTINCT CASE WHEN LOWER(sku_description) LIKE '%capacity%' THEN resource_name END) as capacity_resources,
                COUNT(DISTINCT CASE WHEN LOWER(sku_description) LIKE '%iops%' THEN resource_name END) as iops_resources,
                COUNT(DISTINCT CASE WHEN LOWER(sku_description) LIKE '%throughput%' THEN resource_name END) as throughput_resources,
                COUNT(DISTINCT CASE WHEN LOWER(sku_description) LIKE '%storage pool%' THEN resource_name END) as storage_pool_resources,
                COUNT(DISTINCT sku_id) as unique_skus,
                STRING_AGG(DISTINCT sku_description, '; ' LIMIT 5) as sku_types_detailed,
                -- Calculate total usage by resource type
                SUM(CASE WHEN LOWER(sku_description) LIKE '%capacity%' THEN usage_amount ELSE 0 END) as total_capacity_gb,
                SUM(CASE WHEN LOWER(sku_description) LIKE '%iops%' THEN usage_amount ELSE 0 END) as total_iops_provisioned
            FROM hyperdisk_costs
            GROUP BY project_id
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
                STRING_AGG(DISTINCT sku_description, '; ' LIMIT 3) as sku_types,
                SUM(usage_amount) as total_usage,
                STRING_AGG(DISTINCT usage_unit, ', ') as usage_units,
                STRING_AGG(DISTINCT location_name, ', ') as locations
            FROM hyperdisk_costs
            GROUP BY project_id, project_name
        )
        SELECT 
            ps.*,
            rc.unique_resources,
            rc.capacity_resources,
            rc.iops_resources,
            rc.throughput_resources,
            rc.storage_pool_resources,
            rc.unique_skus,
            rc.sku_types_detailed,
            rc.total_capacity_gb,
            rc.total_iops_provisioned,
            DATE_DIFF(CURRENT_DATE(), ps.first_cost_date, DAY) as duration_days,
            CASE 
                WHEN ps.total_cost > 1000 THEN 'HIGH'
                WHEN ps.total_cost > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END as cost_category,
            ROUND(ps.total_cost / NULLIF(rc.unique_resources, 0), 2) as cost_per_resource,
            ROUND(rc.total_capacity_gb / NULLIF(rc.unique_resources, 0), 2) as avg_capacity_per_resource
        FROM project_summaries ps
        LEFT JOIN resource_counts rc ON ps.project_id = rc.project_id
        ORDER BY ps.total_cost ASC  -- Ascending order as requested
        """
        
        logger.info("📊 Executing analysis query...")
        
        try:
            # Set up job configuration with safety limits
            job_config = bigquery.QueryJobConfig()
            job_config.maximum_bytes_billed = 100 * 1024**3  # Limit to 100GB
            job_config.use_query_cache = True
            job_config.priority = bigquery.QueryPriority.INTERACTIVE
            
            query_job = client.query(analysis_query, job_config=job_config)
            
            # Monitor query progress
            logger.info("⏳ Query is running...")
            df = query_job.result(timeout=300).to_dataframe()  # 5 minute timeout
            
        except Exception as e:
            if "timeout" in str(e).lower():
                logger.error("❌ Analysis query timeout")
                logger.error("The query is taking too long. Try:")
                logger.error(f"1. Reducing ANALYSIS_DAYS from {ANALYSIS_DAYS} to 30 or 60")
                logger.error(f"2. Increasing MIN_COST_THRESHOLD from ${MIN_COST_THRESHOLD}")
                logger.error("3. Running during off-peak hours")
                logger.error("4. Contacting your BigQuery admin for quota increases")
            elif "exceeded" in str(e).lower():
                logger.error("❌ Query processing limit exceeded")
                logger.error("Try reducing the scope of analysis")
            elif "quota" in str(e).lower():
                logger.error("❌ BigQuery quota exceeded")
                logger.error("Wait and try again, or contact your admin")
            else:
                logger.error(f"❌ Analysis query failed: {e}")
            return None
        
        # Validate results
        if df is None:
            logger.error("❌ Query returned None")
            return None
            
        if df.empty:
            logger.warning("❌ Analysis returned no results")
            logger.warning("This could mean:")
            logger.warning("1. No Hyperdisk Balanced resources found")
            logger.warning("2. All costs filtered out by thresholds")
            logger.warning("3. Data quality issues")
            return None
        
        # Validate data quality
        try:
            # Check for required columns
            required_columns = ['project_id', 'total_cost', 'unique_resources']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"❌ Missing required columns: {missing_columns}")
                return None
            
            # Check for data anomalies
            if df['total_cost'].sum() <= 0:
                logger.warning("⚠️ Total cost is zero or negative")
            
            if df['unique_resources'].sum() <= 0:
                logger.warning("⚠️ No resources found")
            
            # Check for null values in critical columns
            critical_nulls = df[required_columns].isnull().sum()
            if critical_nulls.any():
                logger.warning(f"⚠️ Null values found in critical columns: {critical_nulls[critical_nulls > 0]}")
            
        except Exception as e:
            logger.warning(f"⚠️ Data validation failed: {e}")
        
        # Display results
        try:
            print_analysis_results(df)
        except Exception as e:
            logger.error(f"❌ Failed to display results: {e}")
            logger.info("Raw data summary:")
            logger.info(f"Projects: {len(df)}")
            logger.info(f"Total cost: ${df['total_cost'].sum():.2f}")
            logger.info(f"Total resources: {df['unique_resources'].sum()}")
        
        # Save to Excel with error handling
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_filename = f"hyperdisk_real_data_analysis_{timestamp}.xlsx"
            df.to_excel(excel_filename, index=False)
            logger.info(f"✅ Results saved to: {excel_filename}")
        except Exception as e:
            logger.error(f"❌ Failed to save Excel file: {e}")
            try:
                # Fallback to CSV
                csv_filename = f"hyperdisk_real_data_analysis_{timestamp}.csv"
                df.to_csv(csv_filename, index=False)
                logger.info(f"✅ Results saved to CSV: {csv_filename}")
            except Exception as csv_e:
                logger.error(f"❌ Failed to save CSV file: {csv_e}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in analysis: {e}")
        logger.error("Full analysis failed. This might indicate:")
        logger.error("1. BigQuery service issues")
        logger.error("2. Network connectivity problems")
        logger.error("3. Insufficient permissions")
        logger.error("4. Billing table schema changes")
        return None

def print_analysis_results(df):
    """Print analysis results with resource details"""
    print(f"\n🎯 HYPERDISK BALANCED ANALYSIS RESULTS")
    print("=" * 80)
    print(f"📅 Analysis Period: Last {ANALYSIS_DAYS} days")
    print(f"📊 Total Projects: {len(df)}")
    print(f"💵 Total Cost: ${df['total_cost'].sum():.2f}")
    print(f"📈 Average Cost: ${df['total_cost'].mean():.2f}")
    print(f"🔧 Total Hyperdisk Resources: {df['unique_resources'].sum()}")
    print(f"📦 Average Resources per Project: {df['unique_resources'].mean():.1f}")
    
    # Resource summary
    total_capacity = df['capacity_resources'].sum()
    total_iops = df['iops_resources'].sum()
    total_throughput = df['throughput_resources'].sum()
    total_storage_pool = df['storage_pool_resources'].sum()
    
    print(f"\n📊 RESOURCE TYPE BREAKDOWN:")
    print(f"   💾 Capacity Resources: {total_capacity}")
    print(f"   ⚡ IOPS Resources: {total_iops}")
    print(f"   🚀 Throughput Resources: {total_throughput}")
    print(f"   🏊 Storage Pool Resources: {total_storage_pool}")
    
    # Usage summary
    total_capacity_gb = df['total_capacity_gb'].sum()
    total_iops_provisioned = df['total_iops_provisioned'].sum()
    
    print(f"\n📈 USAGE SUMMARY:")
    print(f"   💾 Total Capacity: {total_capacity_gb:,.0f} GB-hours")
    print(f"   ⚡ Total IOPS Provisioned: {total_iops_provisioned:,.0f} IOPS-hours")
    
    print(f"\n📊 ALL PROJECTS (ASCENDING ORDER BY COST):")
    print("-" * 110)
    print(f"{'Project ID':<30} {'Cost':<12} {'Resources':<10} {'Cost/Res':<10} {'Duration':<12} {'Category':<10}")
    print("-" * 110)
    
    for _, row in df.iterrows():
        duration_str = f"{row['duration_days']:.0f} days"
        category_icon = "🔴" if row['cost_category'] == 'HIGH' else "🟡" if row['cost_category'] == 'MEDIUM' else "🟢"
        cost_per_res = f"${row['cost_per_resource']:.2f}" if pd.notna(row['cost_per_resource']) else "N/A"
        resources = f"{row['unique_resources']:.0f}" if pd.notna(row['unique_resources']) else "0"
        
        print(f"{row['project_id']:<30} ${row['total_cost']:<11.2f} {resources:<10} {cost_per_res:<10} {duration_str:<12} {category_icon} {row['cost_category']}")
    
    print(f"\n🔍 DETAILED RESOURCE BREAKDOWN:")
    print("-" * 140)
    print(f"{'Project ID':<30} {'Total':<8} {'Capacity':<10} {'IOPS':<8} {'Throughput':<12} {'Storage Pool':<12} {'SKU Types':<8}")
    print("-" * 140)
    
    for _, row in df.iterrows():
        total_res = f"{row['unique_resources']:.0f}" if pd.notna(row['unique_resources']) else "0"
        capacity_res = f"{row['capacity_resources']:.0f}" if pd.notna(row['capacity_resources']) else "0"
        iops_res = f"{row['iops_resources']:.0f}" if pd.notna(row['iops_resources']) else "0"
        throughput_res = f"{row['throughput_resources']:.0f}" if pd.notna(row['throughput_resources']) else "0"
        storage_pool_res = f"{row['storage_pool_resources']:.0f}" if pd.notna(row['storage_pool_resources']) else "0"
        sku_count = f"{row['unique_skus']:.0f}" if pd.notna(row['unique_skus']) else "0"
        
        print(f"{row['project_id']:<30} {total_res:<8} {capacity_res:<10} {iops_res:<8} {throughput_res:<12} {storage_pool_res:<12} {sku_count:<8}")
    
    # Top resource users
    print(f"\n🏆 TOP RESOURCE USERS:")
    print("-" * 80)
    df_by_resources = df.sort_values('unique_resources', ascending=False)
    print(f"{'Rank':<6} {'Project ID':<30} {'Resources':<12} {'Total Cost':<12}")
    print("-" * 80)
    
    for i, (_, row) in enumerate(df_by_resources.head(5).iterrows(), 1):
        resources = f"{row['unique_resources']:.0f}" if pd.notna(row['unique_resources']) else "0"
        print(f"{i:<6} {row['project_id']:<30} {resources:<12} ${row['total_cost']:<11.2f}")
    
    # Cost efficiency analysis
    print(f"\n💰 COST EFFICIENCY ANALYSIS:")
    print("-" * 80)
    df_by_efficiency = df[df['cost_per_resource'].notna()].sort_values('cost_per_resource', ascending=True)
    print(f"{'Rank':<6} {'Project ID':<30} {'Cost per Resource':<18} {'Total Resources':<12}")
    print("-" * 80)
    
    for i, (_, row) in enumerate(df_by_efficiency.head(5).iterrows(), 1):
        cost_per_res = f"${row['cost_per_resource']:.2f}"
        resources = f"{row['unique_resources']:.0f}"
        print(f"{i:<6} {row['project_id']:<30} {cost_per_res:<18} {resources:<12}")
        
    if len(df_by_efficiency) > 5:
        print("\n   Most Expensive per Resource:")
        for i, (_, row) in enumerate(df_by_efficiency.tail(3).iterrows(), 1):
            cost_per_res = f"${row['cost_per_resource']:.2f}"
            resources = f"{row['unique_resources']:.0f}"
            rank = len(df_by_efficiency) - 3 + i
            print(f"{rank:<6} {row['project_id']:<30} {cost_per_res:<18} {resources:<12}")

def main():
    """Main function with comprehensive error handling"""
    print("\n" + "="*60)
    print("🔧 HYPERDISK BALANCED REAL DATA CONNECTION TEST")
    print("="*60)
    
    try:
        # Check configuration with error handling
        try:
            config_ok, project_id, dataset_id = check_manual_configuration()
            if not config_ok:
                print("\n💡 To connect to real data:")
                print("1. Enable billing export in Google Cloud Console")
                print("2. Update BILLING_PROJECT_ID and BILLING_DATASET_ID in this script")
                print("3. Ensure you have BigQuery Data Viewer permissions")
                print("\n🔗 Useful links:")
                print("- Billing export setup: https://cloud.google.com/billing/docs/how-to/export-data-bigquery")
                print("- BigQuery permissions: https://cloud.google.com/bigquery/docs/access-control")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Configuration check failed: {e}")
            sys.exit(1)
        
        # Test billing table connection
        try:
            client, billing_table = test_billing_table_connection(project_id, dataset_id)
            if not client:
                print("\n💡 Connection troubleshooting:")
                print("1. Verify your GCP credentials: gcloud auth list")
                print("2. Check project access: gcloud projects get-iam-policy PROJECT_ID")
                print("3. Test BigQuery access: bq ls PROJECT_ID:DATASET_ID")
                print("4. Verify billing export is working: check Cloud Console > Billing > Billing export")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Connection test failed unexpectedly: {e}")
            sys.exit(1)
        
        # Search for Hyperdisk Balanced data
        try:
            results = search_for_hyperdisk_balanced(client, billing_table)
            
            if results is not None:
                print(f"\n🎉 Analysis completed successfully!")
                print(f"📊 Projects analyzed: {len(results)}")
                print(f"💰 Total cost found: ${results['total_cost'].sum():.2f}")
                print(f"🔧 Total resources: {results['unique_resources'].sum()}")
                
                # Performance metrics
                logger.info("📈 Performance Summary:")
                logger.info(f"   Query processing time: {datetime.now().strftime('%H:%M:%S')}")
                logger.info(f"   Data points processed: {len(results) * results.shape[1]:,}")
                
            else:
                print(f"\n⚠️  No Hyperdisk Balanced costs found in the specified time period")
                print(f"\n💡 Next steps:")
                print(f"1. Check if you have Hyperdisk Balanced disks in use")
                print(f"2. Try increasing the analysis period (currently {ANALYSIS_DAYS} days)")
                print(f"3. Lower the cost threshold (currently ${MIN_COST_THRESHOLD})")
                print(f"4. Verify the billing account ID: {BILLING_ACCOUNT_ID}")
                
        except KeyboardInterrupt:
            logger.warning("⚠️ Analysis interrupted by user")
            print("\n⚠️ Analysis was interrupted. To resume:")
            print("1. Re-run the script")
            print("2. Consider reducing the analysis scope if it's taking too long")
            sys.exit(130)
        except Exception as e:
            logger.error(f"❌ Analysis failed unexpectedly: {e}")
            print(f"\n💡 If this error persists:")
            print(f"1. Check BigQuery service status")
            print(f"2. Verify your permissions haven't changed")
            print(f"3. Try reducing the analysis scope")
            print(f"4. Contact your GCP administrator")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"❌ Critical error in main function: {e}")
        print(f"\n💥 Critical error occurred. Please:")
        print(f"1. Check the log output above for details")
        print(f"2. Verify your GCP setup and permissions")
        print(f"3. Contact support if the issue persists")
        sys.exit(1)
    
    except KeyboardInterrupt:
        print(f"\n👋 Script interrupted by user. Goodbye!")
        sys.exit(130)

if __name__ == "__main__":
    main()
