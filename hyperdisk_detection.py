#!/usr/bin/env python3
"""
Hyperdisk Balanced Detection Script - Minimal Sampling
For massive billing datasets (2.6TB+) - uses TABLESAMPLE for efficiency
"""

import logging
import time
from datetime import datetime

# BigQuery imports
try:
    from google.cloud import bigquery
except ImportError as e:
    print(f"Missing required packages: {e}")
    print("Install with: pip install google-cloud-bigquery")
    exit(1)

# Configuration
BILLING_PROJECT_ID = "netapp-g1p-billing"
BILLING_DATASET_ID = "g1p_cloud_billing_data"
BILLING_TABLE_ID = "gcp_billing_export_resource_v1_01227B_3F83E7_AC2416"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Minimal sampling approach for massive datasets"""
    try:
        print("="*60)
        print("🔧 HYPERDISK DETECTION - MINIMAL SAMPLING")
        print("="*60)
        
        # Create client
        client = bigquery.Client(project=BILLING_PROJECT_ID)
        logger.info("✅ BigQuery client created")
        
        full_table_id = f"{BILLING_PROJECT_ID}.{BILLING_DATASET_ID}.{BILLING_TABLE_ID}"
        
        # Test table access
        table = client.get_table(full_table_id)
        logger.info(f"✅ Table accessible: {table.num_rows:,} rows, {table.num_bytes / (1024**3):.1f} GB")
        
        # Minimal sampling query - just check if Hyperdisk exists
        sample_query = f"""
        SELECT 
            COUNT(*) as total_sample,
            COUNTIF(LOWER(sku.description) LIKE '%hyperdisk%') as hyperdisk_count,
            COUNTIF(LOWER(sku.description) LIKE '%hyperdisk%balanced%') as hyperdisk_balanced_count,
            STRING_AGG(DISTINCT 
                CASE WHEN LOWER(sku.description) LIKE '%hyperdisk%' 
                THEN sku.description END, '; ' LIMIT 5) as hyperdisk_skus
        FROM `{full_table_id}` TABLESAMPLE SYSTEM (0.01 PERCENT)
        """
        
        logger.info("🔍 Running minimal sample (0.01% of table)...")
        
        # Ultra-conservative limits  
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=100 * 1024**2,  # 100MB limit
            use_query_cache=True
        )
        
        start_time = time.time()
        job = client.query(sample_query, job_config=job_config)
        results = job.result(timeout=30)
        
        for row in results:
            query_time = time.time() - start_time
            
            logger.info(f"✅ Sample analysis complete!")
            logger.info(f"   Sample size: {row.total_sample:,} records")
            logger.info(f"   Hyperdisk records: {row.hyperdisk_count}")
            logger.info(f"   Hyperdisk Balanced: {row.hyperdisk_balanced_count}")
            logger.info(f"   Query time: {query_time:.1f}s")
            logger.info(f"   Data processed: {job.total_bytes_processed / (1024**2):.1f} MB")
            
            if row.hyperdisk_count > 0:
                print(f"\n✅ HYPERDISK DETECTED!")
                print(f"   Found {row.hyperdisk_count} Hyperdisk records in sample")
                print(f"   Hyperdisk Balanced: {row.hyperdisk_balanced_count}")
                print(f"   Sample SKUs: {row.hyperdisk_skus}")
                
                # Estimate full table
                sample_ratio = row.total_sample / table.num_rows if row.total_sample > 0 else 0
                if sample_ratio > 0:
                    estimated_hyperdisk = int(row.hyperdisk_count / sample_ratio)
                    estimated_balanced = int(row.hyperdisk_balanced_count / sample_ratio)
                    
                    print(f"\n📊 ESTIMATED FULL TABLE:")
                    print(f"   Total Hyperdisk records: ~{estimated_hyperdisk:,}")
                    print(f"   Hyperdisk Balanced records: ~{estimated_balanced:,}")
                
                # Provide next steps
                print(f"\n💡 NEXT STEPS:")
                print(f"   1. Contact GCP admin to increase BigQuery slot/billing limits")
                print(f"   2. Use BigQuery console for targeted queries")
                print(f"   3. Consider using Views to pre-filter Hyperdisk data")
                print(f"   4. Export subset of data for detailed analysis")
                
            else:
                print(f"\n❌ NO HYPERDISK FOUND IN SAMPLE")
                print(f"   Sampled {row.total_sample:,} records (0.01% of table)")
                print(f"   This suggests either:")
                print(f"   - No Hyperdisk usage in your environment")
                print(f"   - Very low Hyperdisk usage (< 0.01% of billing records)")
                print(f"   - Different SKU naming conventions")
            
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_file = f"hyperdisk_detection_{timestamp}.txt"
            
            with open(results_file, 'w') as f:
                f.write(f"Hyperdisk Detection Results\n")
                f.write(f"Generated: {datetime.now()}\n")
                f.write(f"Method: 0.01% table sample\n\n")
                f.write(f"Table Size: {table.num_rows:,} rows\n")
                f.write(f"Sample Size: {row.total_sample:,} records\n")
                f.write(f"Hyperdisk Records: {row.hyperdisk_count}\n")
                f.write(f"Hyperdisk Balanced: {row.hyperdisk_balanced_count}\n")
                f.write(f"Sample SKUs: {row.hyperdisk_skus}\n")
                if sample_ratio > 0:
                    f.write(f"Estimated Total Hyperdisk: ~{int(row.hyperdisk_count / sample_ratio):,}\n")
                    f.write(f"Estimated Balanced: ~{int(row.hyperdisk_balanced_count / sample_ratio):,}\n")
            
            logger.info(f"📄 Results saved to: {results_file}")
        
        print(f"\n✅ Detection complete!")
        
    except Exception as e:
        if "exceeded" in str(e).lower():
            logger.error("❌ Query limits exceeded even with minimal sampling")
            logger.error("This dataset is exceptionally large - manual investigation needed")
        else:
            logger.error(f"❌ Detection failed: {e}")

if __name__ == "__main__":
    main()
