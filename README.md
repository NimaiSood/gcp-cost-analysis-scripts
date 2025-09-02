# GCP Unused Resources Finder

A comprehensive Python script to identify unused and potentially costly resources across all Google Cloud Platform (GCP) projects under a specific billing account.

## What it finds

🔍 **Unattached Persistent Disks**
- Persistent disks not attached to any VM instances
- Includes disk size, type, zone, and creation date

💻 **Unused Static IP Addresses**
- Reserved static IP addresses not assigned to any resources
- Includes region, address type, and creation date

📸 **Outdated Snapshots**
- Disk snapshots older than a specified threshold (default: 30 days)
- Includes source disk, age, size, and storage costs

🗄️ **Unaccessed Storage Buckets**
- Storage buckets with no recent activity (default: 90 days)
- Includes object count, total size, and last activity date

## Prerequisites

1. **Python 3.7+** installed on your system
2. **Google Cloud CLI (gcloud)** installed and configured
3. **Proper GCP permissions** for the billing account and projects

### Required GCP Permissions

Your account needs the following IAM roles:
- `billing.resourceAssociations.list` (Billing Account User or Viewer)
- `compute.disks.list` (Compute Viewer)
- `compute.addresses.list` (Compute Viewer)
- `compute.snapshots.list` (Compute Viewer)
- `storage.buckets.list` (Storage Object Viewer)

## Quick Start

1. **Clone or download** this script to your local machine

2. **Run the setup script**:
   ```bash
   ./setup.sh
   ```

3. **Authenticate with Google Cloud** (if not already done):
   ```bash
   gcloud auth application-default login
   ```

4. **Run the scanner**:
   ```bash
   python3 "Unused Resources.py"
   ```

## Configuration

Edit `config.py` to customize the scan behavior:

```python
# Billing account to scan
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"

# Age thresholds
SNAPSHOT_AGE_DAYS = 30      # Snapshots older than this are flagged
BUCKET_INACTIVE_DAYS = 90   # Buckets inactive for this long are flagged

# Performance settings
MAX_WORKERS = 10            # Parallel processing workers
```

## Output

The script generates an Excel report with multiple sheets:

- **Summary**: Overview of findings per project
- **Unattached Disks**: Detailed list of unattached persistent disks
- **Unused IPs**: List of unused static IP addresses
- **Outdated Snapshots**: Snapshots older than the threshold
- **Unaccessed Buckets**: Storage buckets with no recent activity

Report filename format: `unused_resources_report_{BILLING_ACCOUNT}_{TIMESTAMP}.xlsx`

## Features

✅ **Multi-project scanning** - Automatically finds all projects under the billing account  
✅ **Parallel processing** - Scans multiple projects simultaneously for faster execution  
✅ **Comprehensive reporting** - Detailed Excel report with multiple worksheets  
✅ **Error handling** - Graceful handling of permission errors and API limits  
✅ **Configurable thresholds** - Customize what constitutes "outdated" or "unused"  
✅ **Resource details** - Includes creation dates, sizes, locations, and costs  

## Manual Installation

If you prefer to install dependencies manually:

```bash
pip3 install google-cloud-billing google-cloud-compute google-cloud-storage google-cloud-monitoring pandas openpyxl
```

## Troubleshooting

### Authentication Issues
```bash
# Re-authenticate with Google Cloud
gcloud auth application-default login

# Verify access to billing account
gcloud billing accounts list
```

### Permission Errors
- Ensure your account has the required IAM roles
- Check that billing is enabled for the projects
- Verify you have access to the specified billing account

### API Limits
- The script includes built-in rate limiting
- For large organizations, consider running during off-peak hours
- Reduce `MAX_WORKERS` in config.py if you encounter quota issues

## Sample Output

```
🔍 GCP Unused Resources Finder
Scanning billing account: 01227B-3F83E7-AC2416

🏗️  Processing 25 projects...
Processing project: my-project-1
  Found 3 unattached disks
  Found 1 unused IPs
  Found 12 outdated snapshots
  Found 2 unaccessed buckets

📊 UNUSED RESOURCES REPORT
==================================================
Billing Account: 01227B-3F83E7-AC2416
Generated: 2025-08-22 13:45:30
Report saved as: unused_resources_report_01227B-3F83E7-AC2416_20250822_134530.xlsx

📈 SUMMARY:
  • Total Projects Processed: 25
  • Unattached Disks: 15
  • Unused Static IPs: 8
  • Outdated Snapshots (>30 days): 45
  • Unaccessed Buckets (>90 days): 12

✅ Scan completed!
```

## Cost Savings Potential

This script can help identify significant cost savings:
- **Unattached disks**: $0.04-$0.17 per GB per month
- **Unused static IPs**: $0.010 per hour (~$7.30/month each)
- **Old snapshots**: $0.026 per GB per month
- **Unused storage**: $0.020-$0.023 per GB per month

## License

This script is provided as-is for educational and operational purposes. Use at your own discretion and ensure you have proper permissions before running against production environments.
