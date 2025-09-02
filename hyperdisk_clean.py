#!/usr/bin/env python3
"""
Hyperdisk Balanced Cost Analysis - Clean Production Version
Analyzes Hyperdisk Balanced storage costs from BigQuery billing data
"""

import logging
import time
import os
from datetime import datetime, timedelta

# BigQuery imports
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    import pandas as pd
    import db_dtypes
    import pyarrow
except ImportError as e:
    print(f"Missing required packages: {e}")
    print("Install with: pip install google-cloud-bigquery pandas db-dtypes pyarrow xlsxwriter")
    exit(1)

# Configuration
BILLING_PROJECT_ID = "netapp-g1p-billing"
BILLING_DATASET_ID = "g1p_cloud_billing_data"
BILLING_TABLE_ID = "gcp_billing_export_resource_v1_01227B_3F83E7_AC2416"
ANALYSIS_DAYS = 1  # Reduced to 1 day for this massive dataset

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_bigquery_client():
    """Create BigQuery client with error handling"""
    try:
        client = bigquery.Client(project=BILLING_PROJECT_ID)
        logger.info("✅ BigQuery client created successfully")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to create BigQuery client: {e}")
        logger.error("Check: gcloud auth application-default login")
        return None

def test_table_access(client):
    """Test access to the billing table"""
    try:
        full_table_id = f"{BILLING_PROJECT_ID}.{BILLING_DATASET_ID}.{BILLING_TABLE_ID}"
        logger.info(f"🔍 Testing access to: {full_table_id}")
        
        # Get table metadata
        table = client.get_table(full_table_id)
        
        logger.info(f"✅ Table accessible!")
        logger.info(f"   Rows: {table.num_rows:,}")
        logger.info(f"   Size: {table.num_bytes / (1024**3):.2f} GB")
        logger.info(f"   Last Modified: {table.modified}")
        
        return full_table_id
        
    except Exception as e:
        logger.error(f"❌ Table access failed: {e}")
        return None

def find_hyperdisk_sample(client, table_id):
    """Find sample Hyperdisk records to understand data structure"""
    try:
        logger.info("📊 Looking for Hyperdisk sample data...")
        
        # Use a more targeted sample with stricter filtering
        sample_query = f"""
        SELECT 
            sku.description as sku_desc,
            service.description as service_desc,
            project.id as project_id,
            cost,
            usage_start_time
        FROM `{table_id}`
        WHERE 
            LOWER(sku.description) LIKE '%hyperdisk%'
            AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
            AND cost > 0
        LIMIT 3
        """
        
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=500 * 1024**2,  # 500MB limit 
            use_query_cache=True
        )
        
        job = client.query(sample_query, job_config=job_config)
        results = job.result(timeout=20)
        
        sample_found = False
        for row in results:
            sample_found = True
            logger.info(f"   Found: {row.sku_desc}")
            logger.info(f"   Service: {row.service_desc}")
            logger.info(f"   Project: {row.project_id}")
            logger.info(f"   Cost: ${row.cost:.4f}")
            logger.info("   ---")
        
        if not sample_found:
            logger.warning("⚠️ No Hyperdisk records found in last 6 hours")
            logger.info("ℹ️  This is normal for large datasets - proceeding with main analysis")
            
        return True  # Always proceed with main analysis
        
    except Exception as e:
        logger.warning(f"⚠️ Sample query failed: {e}")
        logger.info("ℹ️  Proceeding with main analysis anyway")
        return True  # Don't block main analysis

def analyze_hyperdisk_balanced(client, table_id):
    """Analyze Hyperdisk Balanced costs"""
    try:
        logger.info("🚀 Running Hyperdisk Balanced analysis...")
        
        analysis_query = f"""
        SELECT 
            project.id as project_id,
            project.name as project_name,
            location.location as location,
            SUM(cost) as total_cost,
            SUM(usage.amount) as total_usage_gb,
            COUNT(DISTINCT resource.name) as resource_count,
            COUNT(DISTINCT EXTRACT(DATE FROM usage_start_time)) as days_with_usage,
            STRING_AGG(DISTINCT sku.description, '; ' LIMIT 3) as sku_types
        FROM `{table_id}`
        WHERE 
            -- Use partition pruning with export_time for better performance
            export_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ANALYSIS_DAYS} DAY)
            AND LOWER(sku.description) LIKE '%hyperdisk%balanced%'
            AND LOWER(service.description) LIKE '%compute%'
            AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ANALYSIS_DAYS} DAY)
            AND cost > 0.01
        GROUP BY 
            1, 2, 3
        HAVING 
            SUM(cost) > 0.10
        ORDER BY 
            4 DESC
        LIMIT 50
        """
        
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=10 * 1024**3,  # 10GB limit
            use_query_cache=True
        )
        
        start_time = time.time()
        job = client.query(analysis_query, job_config=job_config)
        results = job.result(timeout=120)
        
        # Process results
        hyperdisk_data = []
        total_cost = 0
        total_resources = 0
        
        for row in results:
            data = {
                'project_id': row.project_id,
                'project_name': row.project_name or 'Unknown',
                'location': row.location or 'Unknown',
                'total_cost': float(row.total_cost),
                'total_usage_gb': float(row.total_usage_gb or 0),
                'resource_count': int(row.resource_count or 0),
                'days_with_usage': int(row.days_with_usage or 0),
                'sku_types': row.sku_types or 'Unknown'
            }
            hyperdisk_data.append(data)
            total_cost += data['total_cost']
            total_resources += data['resource_count']
        
        query_time = time.time() - start_time
        
        # Log results
        logger.info(f"✅ Analysis complete!")
        logger.info(f"   Projects with Hyperdisk Balanced: {len(hyperdisk_data)}")
        logger.info(f"   Total cost ({ANALYSIS_DAYS} days): ${total_cost:,.2f}")
        logger.info(f"   Total resources: {total_resources:,}")
        logger.info(f"   Query time: {query_time:.1f} seconds")
        logger.info(f"   Data processed: {job.total_bytes_processed / (1024**3):.2f} GB")
        
        return hyperdisk_data
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        if "exceeded" in str(e).lower():
            logger.error("Query processing limit exceeded - try reducing ANALYSIS_DAYS")
        elif "timeout" in str(e).lower():
            logger.error("Query timeout - table is very large")
        elif "quota" in str(e).lower():
            logger.error("BigQuery quota exceeded - wait and retry")
        return []

def save_results(data, filename_prefix="hyperdisk_analysis"):
    """Save results to Excel and CSV"""
    try:
        if not data:
            logger.warning("⚠️ No data to save")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Save Excel
        excel_file = f"{filename_prefix}_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Hyperdisk_Analysis', index=False)
            
            # Add summary sheet
            summary_data = {
                'Metric': ['Projects', 'Total Cost', 'Total Resources', 'Avg Cost per Project'],
                'Value': [
                    len(data),
                    f"${sum(d['total_cost'] for d in data):,.2f}",
                    sum(d['resource_count'] for d in data),
                    f"${sum(d['total_cost'] for d in data) / len(data):,.2f}"
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        # Save CSV
        csv_file = f"{filename_prefix}_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        
        logger.info(f"📊 Results saved:")
        logger.info(f"   Excel: {excel_file}")
        logger.info(f"   CSV: {csv_file}")
        
    except Exception as e:
        logger.error(f"❌ Failed to save results: {e}")

def main():
    """Main execution function"""
    try:
        print("="*60)
        print("🔧 HYPERDISK BALANCED COST ANALYSIS")
        print("="*60)
        
        # Create BigQuery client
        client = create_bigquery_client()
        if not client:
            return
        
        # Test table access
        table_id = test_table_access(client)
        if not table_id:
            return
        
        # Find sample data
        if not find_hyperdisk_sample(client, table_id):
            logger.warning("⚠️ No recent Hyperdisk data found - trying broader search...")
        
        # Run analysis
        results = analyze_hyperdisk_balanced(client, table_id)
        
        if results:
            # Save results
            save_results(results)
            
            # Display top results
            print("\n📋 TOP PROJECTS BY HYPERDISK BALANCED COST:")
            print("-" * 80)
            for i, project in enumerate(results[:10], 1):
                print(f"{i:2d}. {project['project_id']:<30} ${project['total_cost']:>8.2f} ({project['resource_count']:>3} resources)")
        else:
            logger.warning("❌ No Hyperdisk Balanced data found")
            logger.info("💡 Suggestions:")
            logger.info("   1. Increase ANALYSIS_DAYS in the script")
            logger.info("   2. Check if you have Hyperdisk Balanced disks")
            logger.info("   3. Verify billing export is working")
        
        print("\n✅ Analysis complete!")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Analysis interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        logger.error("🔧 Troubleshooting:")
        logger.error("   1. Check GCP authentication: gcloud auth list")
        logger.error("   2. Verify project access permissions")
        logger.error("   3. Ensure BigQuery API is enabled")

if __name__ == "__main__":
    main()
