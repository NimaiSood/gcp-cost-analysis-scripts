# Configuration file for GCP Unused Resources Finder
# You can modify these values to customize the scan behavior

# Billing account ID to scan
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"

# Snapshot age threshold (days) - snapshots older than this are considered outdated
SNAPSHOT_AGE_DAYS = 30

# Bucket inactivity threshold (days) - buckets inactive for this long are flagged
BUCKET_INACTIVE_DAYS = 90

# Maximum number of parallel workers for processing projects
MAX_WORKERS = 10

# Additional scan options
SCAN_UNATTACHED_DISKS = True
SCAN_UNUSED_IPS = True
SCAN_OUTDATED_SNAPSHOTS = True
SCAN_UNACCESSED_BUCKETS = True

# Report settings
INCLUDE_ZERO_COST_RESOURCES = True
DETAILED_BUCKET_ANALYSIS = False  # Set to True for more detailed bucket analysis (slower)
