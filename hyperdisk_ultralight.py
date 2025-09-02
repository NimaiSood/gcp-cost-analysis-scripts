#!/usr/bin/env python3
"""
Hyperdisk Balanced Cost Analysis - Ultra-Light Version
For extremely large billing datasets (2.6TB+)
"""

import logging
import time
from datetime import datetime

# BigQuery imports
try:
    from google.cloud import bigquery
    import pandas as pd
except ImportError as e:
    print(f"Missing required packages: {e}")
    print("Install with: pip install google-cloud-bigquery pandas xlsxwriter")
    exit(1)

# Configuration
BILLING_PROJECT_ID = "netapp-g1p-billing"
BILLING_DATASET_ID = "g1p_cloud_billing_data"
BILLING_TABLE_ID = "gcp_billing_export_resource_v1_01227B_3F83E7_AC2416"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Ultra-light analysis using summary approach"""
    try:
        print("="*60)
        print("🔧 HYPERDISK BALANCED COST ANALYSIS - ULTRA LIGHT")
        print("="*60)
        
        # Create client
        client = bigquery.Client(project=BILLING_PROJECT_ID)
        logger.info("✅ BigQuery client created")
        
        full_table_id = f"{BILLING_PROJECT_ID}.{BILLING_DATASET_ID}.{BILLING_TABLE_ID}"
        
        # Test table access
        table = client.get_table(full_table_id)
        logger.info(f"✅ Table accessible: {table.num_rows:,} rows, {table.num_bytes / (1024**3):.1f} GB")
        
        # Ultra-targeted query - only look at very recent data and specific patterns
        ultra_query = f"""
        SELECT 
            COUNT(*) as hyperdisk_records,
            COUNT(DISTINCT project.id) as unique_projects,
            SUM(cost) as total_cost,
            STRING_AGG(DISTINCT sku.description, '; ' LIMIT 5) as sample_skus
        FROM `{full_table_id}`
        WHERE 
            -- Very recent data only (last 6 hours)
            export_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
            AND (
                LOWER(sku.description) LIKE '%hyperdisk%balanced%'
                OR LOWER(sku.description) LIKE '%hyperdisk balanced%'
            )
            AND cost > 0
        """
        
        logger.info("🔍 Running ultra-light Hyperdisk analysis (last 6 hours)...")
        
        # Very conservative limits
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=2 * 1024**3,  # 2GB limit
            use_query_cache=True,
            dry_run=False
        )
        
        start_time = time.time()
        job = client.query(ultra_query, job_config=job_config)
        results = job.result(timeout=60)
        
        for row in results:
            query_time = time.time() - start_time
            
            logger.info(f"✅ Analysis complete!")
            logger.info(f"   Hyperdisk records (6h): {row.hyperdisk_records:,}")
            logger.info(f"   Projects with Hyperdisk: {row.unique_projects}")
            logger.info(f"   Total cost (6h): ${row.total_cost:.4f}")
            logger.info(f"   Sample SKUs: {row.sample_skus}")
            logger.info(f"   Query time: {query_time:.1f}s")
            logger.info(f"   Data processed: {job.total_bytes_processed / (1024**3):.2f} GB")
            
            if row.hyperdisk_records > 0:
                # Extrapolate to longer periods
                daily_estimate = row.total_cost * 4  # 6h * 4 = 24h
                weekly_estimate = daily_estimate * 7
                monthly_estimate = daily_estimate * 30
                
                print("\n📊 COST PROJECTIONS (based on 6-hour sample):")
                print(f"   Daily estimate: ${daily_estimate:.2f}")
                print(f"   Weekly estimate: ${weekly_estimate:.2f}")
                print(f"   Monthly estimate: ${monthly_estimate:.2f}")
                
                print(f"\n📋 PROJECTS WITH HYPERDISK BALANCED:")
                print(f"   {row.unique_projects} projects found with Hyperdisk usage")
                
                # Save minimal results
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                summary_file = f"hyperdisk_summary_{timestamp}.txt"
                
                with open(summary_file, 'w') as f:
                    f.write(f"Hyperdisk Balanced Analysis Summary\n")
                    f.write(f"Generated: {datetime.now()}\n")
                    f.write(f"Analysis Period: Last 6 hours\n\n")
                    f.write(f"Records Found: {row.hyperdisk_records:,}\n")
                    f.write(f"Projects: {row.unique_projects}\n")
                    f.write(f"6-Hour Cost: ${row.total_cost:.4f}\n")
                    f.write(f"Daily Estimate: ${daily_estimate:.2f}\n")
                    f.write(f"Weekly Estimate: ${weekly_estimate:.2f}\n")
                    f.write(f"Monthly Estimate: ${monthly_estimate:.2f}\n")
                    f.write(f"Sample SKUs: {row.sample_skus}\n")
                
                logger.info(f"📄 Summary saved to: {summary_file}")
                
            else:
                logger.warning("❌ No Hyperdisk Balanced usage found in last 6 hours")
                logger.info("💡 This might mean:")
                logger.info("   1. No Hyperdisk Balanced disks currently in use")
                logger.info("   2. Usage billing is delayed")
                logger.info("   3. Different SKU naming conventions")
        
        print("\n✅ Ultra-light analysis complete!")
        
    except Exception as e:
        if "exceeded" in str(e).lower():
            logger.error("❌ Even the ultra-light query is too large for this dataset")
            logger.error("💡 Recommendations:")
            logger.error("   1. Contact your GCP admin to increase BigQuery limits")
            logger.error("   2. Use BigQuery console with smaller time windows")
            logger.error("   3. Consider using Cloud Asset API instead")
        else:
            logger.error(f"❌ Analysis failed: {e}")

if __name__ == "__main__":
    main()
