#!/usr/bin/env python3
"""
Script to list the top 100 projects linked to a specific billing account.
Sorted by daily cost in descending order.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from google.cloud import billing_v1
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import (
    GoogleAPIError, 
    RetryError, 
    DeadlineExceeded, 
    PermissionDenied,
    NotFound,
    ServiceUnavailable
)
import pandas as pd

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
MAX_PROJECTS = 100
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Billing export configuration
# You may need to adjust these based on your BigQuery billing export setup
BILLING_PROJECT_ID = None  # Will try to auto-detect or use first available project
BILLING_DATASET_ID = "billing_export"  # Common default
BILLING_TABLE_ID = "gcp_billing_export_v1_01227B_3F83E7_AC2416"  # Based on your billing account
COST_ANALYSIS_DAYS = 7  # Number of days to analyze for cost calculation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('project_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ProjectAnalysisError(Exception):
    """Custom exception for project analysis errors."""
    pass

class BillingAccountError(Exception):
    """Custom exception for billing account related errors."""
    pass

def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """
    Decorator to retry function calls on failure.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ServiceUnavailable, DeadlineExceeded, RetryError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
                        break
                except Exception as e:
                    # For non-retryable exceptions, fail immediately
                    logger.error(f"Non-retryable error in {func.__name__}: {str(e)}")
                    raise
            
            if last_exception:
                raise last_exception
        return wrapper
    return decorator

@retry_on_failure()
def get_projects_under_billing_account(billing_account_id):
    """
    Get all projects linked to a specific billing account.
    
    Args:
        billing_account_id (str): The billing account ID
        
    Returns:
        list: List of project objects
        
    Raises:
        BillingAccountError: If billing account operations fail
        PermissionDenied: If insufficient permissions
        NotFound: If billing account doesn't exist
    """
    if not billing_account_id:
        raise BillingAccountError("Billing account ID cannot be empty")
    
    logger.info(f"🔍 Retrieving projects for billing account: {billing_account_id}...")
    
    try:
        # Initialize the Cloud Billing client
        client = billing_v1.CloudBillingClient()
        
        # Validate billing account exists and is accessible
        billing_account_name = f"billingAccounts/{billing_account_id}"
        
        try:
            # Test access to the billing account
            billing_account = client.get_billing_account(name=billing_account_name)
            if not billing_account:
                raise NotFound(f"Billing account {billing_account_id} not found")
        except NotFound:
            raise BillingAccountError(f"Billing account {billing_account_id} not found or inaccessible")
        except PermissionDenied:
            raise BillingAccountError(f"Insufficient permissions to access billing account {billing_account_id}")
        
        # Get all projects under the billing account
        request = billing_v1.ListProjectBillingInfoRequest(
            name=billing_account_name
        )
        
        projects = []
        page_result = client.list_project_billing_info(request=request)
        
        project_count = 0
        for project_billing_info in page_result:
            try:
                if project_billing_info.billing_enabled:
                    # Extract project ID from the project name (format: projects/PROJECT_ID/billingInfo)
                    project_name_parts = project_billing_info.name.split('/')
                    if len(project_name_parts) >= 3:
                        project_id = project_name_parts[1]  # Get the PROJECT_ID part
                    else:
                        project_id = project_billing_info.name  # Fallback to full name
                    
                    # Validate project ID
                    if not project_id or project_id.strip() == "":
                        logger.warning(f"Empty project ID found, skipping: {project_billing_info.name}")
                        continue
                    
                    projects.append({
                        'project_id': project_id,
                        'project_name': project_billing_info.name,
                        'billing_enabled': project_billing_info.billing_enabled,
                        'billing_account_name': project_billing_info.billing_account_name
                    })
                    project_count += 1
                    
            except Exception as e:
                logger.warning(f"Error processing project {project_billing_info.name}: {str(e)}")
                continue
        
        if not projects:
            logger.warning("No active projects found linked to billing account")
            return []
        
        logger.info(f"✅ Found {len(projects)} active projects linked to billing account.")
        return projects
        
    except PermissionDenied as e:
        error_msg = f"Permission denied accessing billing account {billing_account_id}: {str(e)}"
        logger.error(error_msg)
        raise BillingAccountError(error_msg)
    except NotFound as e:
        error_msg = f"Billing account {billing_account_id} not found: {str(e)}"
        logger.error(error_msg)
        raise BillingAccountError(error_msg)
    except GoogleAPIError as e:
        error_msg = f"Google API error retrieving projects: {str(e)}"
        logger.error(error_msg)
        raise BillingAccountError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error retrieving projects: {str(e)}"
        logger.error(error_msg)
        raise ProjectAnalysisError(error_msg)

def get_project_details(project_id):
    """
    Get basic project details. Since we don't have resource manager,
    we'll use the project ID as display name.
    
    Args:
        project_id (str): The project ID
        
    Returns:
        dict: Project details
        
    Raises:
        ValueError: If project_id is invalid
    """
    if not project_id or not isinstance(project_id, str) or project_id.strip() == "":
        raise ValueError("Project ID must be a non-empty string")
    
    try:
        return {
            'display_name': project_id,  # Use project ID as display name
            'state': 'ACTIVE',  # Assume active since it's in billing
            'create_time': None,
            'labels': {}  # We'll get labels from other sources if needed
        }
    except Exception as e:
        logger.warning(f"Error getting details for project {project_id}: {str(e)}")
        # Return minimal details as fallback
        return {
            'display_name': project_id,
            'state': 'UNKNOWN',
            'create_time': None,
            'labels': {}
        }

def detect_billing_export_table():
    """
    Detect the BigQuery billing export table configuration.
    
    Returns:
        tuple: (project_id, dataset_id, table_id) or (None, None, None) if not found
    """
    try:
        client = bigquery.Client()
        
        # Try to use the default project from credentials
        project_id = client.project
        logger.info(f"Using project: {project_id} for billing data queries")
        
        # Common dataset names for billing exports
        common_datasets = ["billing_export", "billing", "cloud_billing_export"]
        
        for dataset_id in common_datasets:
            try:
                dataset_ref = client.dataset(dataset_id, project=project_id)
                dataset = client.get_dataset(dataset_ref)
                
                # List tables in the dataset
                tables = list(client.list_tables(dataset))
                
                for table in tables:
                    table_id = table.table_id
                    # Look for billing export table patterns
                    if ("billing_export" in table_id.lower() or 
                        "gcp_billing_export" in table_id.lower() or
                        BILLING_ACCOUNT_ID.replace("-", "_") in table_id):
                        
                        logger.info(f"Found billing export table: {project_id}.{dataset_id}.{table_id}")
                        return project_id, dataset_id, table_id
                        
            except Exception as e:
                logger.debug(f"Dataset {dataset_id} not found or accessible: {e}")
                continue
                
        logger.warning("No billing export table found with common patterns")
        return None, None, None
        
    except Exception as e:
        logger.error(f"Error detecting billing export table: {e}")
        return None, None, None

@retry_on_failure()
def get_project_daily_cost_from_bigquery(project_id, days=7):
    """
    Get actual daily cost for a project from BigQuery billing export.
    
    Args:
        project_id (str): The project ID
        days (int): Number of days to look back for cost calculation
        
    Returns:
        float: Average daily cost in USD
    """
    try:
        # Detect billing export table
        billing_project, dataset_id, table_id = detect_billing_export_table()
        
        if not all([billing_project, dataset_id, table_id]):
            logger.warning(f"Billing export table not found for project {project_id}")
            return 0.0
            
        client = bigquery.Client(project=billing_project)
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Query to get cost data for the specific project
        query = f"""
        SELECT 
            SUM(cost) as total_cost,
            COUNT(DISTINCT DATE(usage_start_time)) as active_days
        FROM `{billing_project}.{dataset_id}.{table_id}`
        WHERE project.id = @project_id
            AND DATE(usage_start_time) >= @start_date
            AND DATE(usage_start_time) <= @end_date
            AND cost > 0
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            total_cost = row.total_cost or 0.0
            active_days = row.active_days or days
            
            # Calculate average daily cost
            if active_days > 0:
                daily_cost = total_cost / active_days
                logger.debug(f"Project {project_id}: ${total_cost:.2f} over {active_days} days = ${daily_cost:.2f}/day")
                return daily_cost
            else:
                return 0.0
                
        return 0.0
        
    except Exception as e:
        logger.warning(f"Error getting BigQuery cost data for project {project_id}: {e}")
        return 0.0

@retry_on_failure()
def get_project_daily_cost_from_api(project_id, days=7):
    """
    Alternative method to get cost data using Cloud Billing Budget API or estimates.
    This is a fallback when BigQuery billing export is not available.
    
    Args:
        project_id (str): The project ID
        days (int): Number of days to estimate cost
        
    Returns:
        float: Estimated daily cost in USD
    """
    try:
        # This is a placeholder for Budget API implementation
        # The Budget API doesn't provide historical costs directly,
        # but you could implement resource-based cost estimation here
        
        logger.debug(f"Using fallback cost estimation for project {project_id}")
        
        # For now, return 0 but this could be enhanced with:
        # 1. Resource inventory and pricing API
        # 2. Cloud Asset Inventory
        # 3. Resource usage metrics
        
        return 0.0
        
    except Exception as e:
        logger.warning(f"Error in fallback cost estimation for project {project_id}: {e}")
        return 0.0

def get_project_daily_cost(project_id, days=7):
    """
    Calculate the average daily cost for a project based on the last N days.
    Uses BigQuery billing export as primary source, with fallback methods.
    
    Args:
        project_id (str): The project ID
        days (int): Number of days to look back for cost calculation
        
    Returns:
        float: Average daily cost in USD
        
    Raises:
        ValueError: If invalid parameters provided
    """
    if not project_id or not isinstance(project_id, str):
        raise ValueError("Project ID must be a non-empty string")
    
    if not isinstance(days, int) or days <= 0:
        raise ValueError("Days must be a positive integer")
    
    try:
        logger.debug(f"📊 Getting cost data for project {project_id}...")
        
        # Try BigQuery billing export first (most accurate)
        daily_cost = get_project_daily_cost_from_bigquery(project_id, days)
        
        if daily_cost > 0:
            return daily_cost
            
        # Fallback to API-based estimation if BigQuery data not available
        daily_cost = get_project_daily_cost_from_api(project_id, days)
        
        return daily_cost
        
    except ValueError:
        # Re-raise validation errors
        raise
    except Exception as e:
        logger.warning(f"⚠️ Could not get cost data for project {project_id}: {str(e)}")
        return 0.0

def validate_environment():
    """
    Validate the environment and prerequisites.
    
    Raises:
        ProjectAnalysisError: If environment validation fails
    """
    try:
        # Check if required environment variables or credentials are available
        # This is a placeholder - add actual credential checks as needed
        
        # Validate billing account ID format
        if not BILLING_ACCOUNT_ID or len(BILLING_ACCOUNT_ID.split('-')) != 3:
            raise ProjectAnalysisError(f"Invalid billing account ID format: {BILLING_ACCOUNT_ID}")
        
        # Check if pandas is available for CSV export
        try:
            import pandas as pd
        except ImportError:
            raise ProjectAnalysisError("pandas library is required but not installed")
        
        # Test Google Cloud client initialization
        try:
            client = billing_v1.CloudBillingClient()
        except Exception as e:
            raise ProjectAnalysisError(f"Failed to initialize Google Cloud Billing client: {str(e)}")
        
        logger.info("✅ Environment validation passed")
        
    except ProjectAnalysisError:
        raise
    except Exception as e:
        raise ProjectAnalysisError(f"Environment validation failed: {str(e)}")

def save_results_to_csv(projects_data, billing_account_id):
    """
    Save project data to CSV file with error handling.
    
    Args:
        projects_data (list): List of project dictionaries
        billing_account_id (str): Billing account ID for filename
        
    Returns:
        str: Filename of saved CSV file
        
    Raises:
        ProjectAnalysisError: If CSV saving fails
    """
    if not projects_data:
        raise ProjectAnalysisError("No project data to save")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"top_projects_{billing_account_id.replace('-', '_')}_{timestamp}.csv"
        
        # Ensure the directory is writable
        current_dir = os.getcwd()
        if not os.access(current_dir, os.W_OK):
            raise ProjectAnalysisError(f"No write permission in current directory: {current_dir}")
        
        df = pd.DataFrame(projects_data)
        df.to_csv(csv_filename, index=False)
        
        # Verify file was created and has content
        if not os.path.exists(csv_filename):
            raise ProjectAnalysisError(f"Failed to create CSV file: {csv_filename}")
        
        file_size = os.path.getsize(csv_filename)
        if file_size == 0:
            raise ProjectAnalysisError(f"CSV file created but is empty: {csv_filename}")
        
        logger.info(f"💾 Results saved to: {csv_filename} ({file_size} bytes)")
        return csv_filename
        
    except pd.errors.ParserError as e:
        raise ProjectAnalysisError(f"Error creating DataFrame: {str(e)}")
    except PermissionError as e:
        raise ProjectAnalysisError(f"Permission error saving CSV: {str(e)}")
    except OSError as e:
        raise ProjectAnalysisError(f"OS error saving CSV: {str(e)}")
    except Exception as e:
        raise ProjectAnalysisError(f"Unexpected error saving CSV: {str(e)}")

def main():
    """Main function to list top 100 projects by cost."""
    
    logger.info("🚀 Starting script to list top 100 projects by cost...")
    logger.info(f"📊 Target billing account: {BILLING_ACCOUNT_ID}")
    logger.info(f"🔢 Limiting results to top {MAX_PROJECTS} projects")
    logger.info("=" * 80)
    
    try:
        # Validate environment before proceeding
        validate_environment()
        
        # Get all projects under the billing account
        projects = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
        
        if not projects:
            logger.warning("❌ No projects found. Exiting.")
            return
        
        logger.info(f"\n💰 Analyzing costs for {len(projects)} projects...")
        
        # Enhance project data with costs and details
        enhanced_projects = []
        failed_projects = []
        
        for i, project in enumerate(projects, 1):
            project_id = project.get('project_id')
            if not project_id:
                logger.warning(f"Skipping project with missing ID: {project}")
                failed_projects.append(project)
                continue
                
            logger.info(f"📊 [{i}/{len(projects)}] Analyzing project: {project_id}")
            
            try:
                # Get project details
                details = get_project_details(project_id)
                
                # Get daily cost
                daily_cost = get_project_daily_cost(project_id, COST_ANALYSIS_DAYS)
                
                # Validate cost data
                if not isinstance(daily_cost, (int, float)) or daily_cost < 0:
                    logger.warning(f"Invalid cost data for project {project_id}: {daily_cost}")
                    daily_cost = 0.0
                
                # Combine all information
                enhanced_project = {
                    'project_id': project_id,
                    'display_name': details['display_name'],
                    'daily_cost_usd': daily_cost,
                    'monthly_cost_usd': daily_cost * 30,
                    'state': details['state'],
                    'create_time': details['create_time'],
                    'labels_count': len(details['labels']),
                    'labels': details['labels'],
                    'billing_enabled': project['billing_enabled']
                }
                
                enhanced_projects.append(enhanced_project)
                
            except Exception as e:
                logger.error(f"Failed to process project {project_id}: {str(e)}")
                failed_projects.append(project)
                continue
            
            # Progress indicator
            if i % 10 == 0:
                logger.info(f"✅ Processed {i}/{len(projects)} projects")
        
        if failed_projects:
            logger.warning(f"⚠️ Failed to process {len(failed_projects)} projects")
        
        if not enhanced_projects:
            raise ProjectAnalysisError("No projects were successfully processed")
        
        logger.info("\n🔄 Sorting projects by daily cost...")
        
        # Sort by daily cost (descending)
        enhanced_projects.sort(key=lambda x: x.get('daily_cost_usd', 0), reverse=True)
        
        # Limit to top 100
        top_projects = enhanced_projects[:MAX_PROJECTS]
        
        # Display results
        logger.info("\n" + "=" * 100)
        logger.info(f"📊 TOP {len(top_projects)} PROJECTS BY DAILY COST")
        logger.info("=" * 100)
        
        print(f"{'Rank':<5} {'Project ID':<30} {'Display Name':<25} {'Daily Cost':<12} {'Monthly Est':<12} {'Labels':<8}")
        print("-" * 100)
        
        total_daily_cost = 0.0
        
        for i, project in enumerate(top_projects, 1):
            daily_cost = project.get('daily_cost_usd', 0.0)
            monthly_cost = project.get('monthly_cost_usd', 0.0)
            total_daily_cost += daily_cost
            
            # Truncate long names
            display_name = project.get('display_name', 'N/A')[:24] if len(str(project.get('display_name', ''))) > 24 else project.get('display_name', 'N/A')
            
            print(f"{i:<5} {project.get('project_id', 'N/A'):<30} {display_name:<25} ${daily_cost:<11.2f} ${monthly_cost:<11.2f} {project.get('labels_count', 0):<8}")
        
        print("-" * 100)
        print(f"{'TOTAL':<62} ${total_daily_cost:<11.2f} ${total_daily_cost * 30:<11.2f}")
        print("=" * 100)
        
        # Summary statistics
        logger.info(f"\n📈 SUMMARY STATISTICS:")
        logger.info(f"💰 Total daily cost (top {len(top_projects)}): ${total_daily_cost:.2f}")
        logger.info(f"💰 Estimated monthly cost: ${total_daily_cost * 30:.2f}")
        
        if top_projects:
            avg_cost = total_daily_cost / len(top_projects)
            logger.info(f"📊 Average daily cost per project: ${avg_cost:.2f}")
            
            highest_cost = top_projects[0]
            logger.info(f"🏆 Highest cost project: {highest_cost.get('project_id', 'N/A')} (${highest_cost.get('daily_cost_usd', 0):.2f}/day)")
        
        # Projects with no labels
        unlabeled_projects = [p for p in top_projects if p.get('labels_count', 0) == 0]
        if unlabeled_projects:
            logger.info(f"🏷️ Projects without labels: {len(unlabeled_projects)}/{len(top_projects)}")
            unlabeled_cost = sum(p.get('daily_cost_usd', 0) for p in unlabeled_projects)
            logger.info(f"💸 Daily cost of unlabeled projects: ${unlabeled_cost:.2f}")
        
        # Save to CSV for further analysis
        try:
            csv_filename = save_results_to_csv(top_projects, BILLING_ACCOUNT_ID)
            logger.info(f"✅ Results saved to: {csv_filename}")
        except ProjectAnalysisError as e:
            logger.error(f"Failed to save CSV: {str(e)}")
            # Continue execution even if CSV save fails
        
        logger.info(f"\n🏁 Analysis complete! Listed top {len(top_projects)} projects by cost.")
        
        if failed_projects:
            logger.warning(f"⚠️ Note: {len(failed_projects)} projects failed to process")
            
    except BillingAccountError as e:
        logger.error(f"❌ Billing account error: {str(e)}")
        sys.exit(1)
    except ProjectAnalysisError as e:
        logger.error(f"❌ Project analysis error: {str(e)}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except SystemExit:
        # Re-raise SystemExit to preserve exit codes
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in main execution: {str(e)}")
        logger.exception("Full traceback:")
        sys.exit(1)
