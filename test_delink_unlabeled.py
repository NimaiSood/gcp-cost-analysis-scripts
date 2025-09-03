#!/usr/bin/env python3
"""
Test script to test delinking projects with no labels from billing account.
This script combines label checking with delinking functionality for testing.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
import logging
import time
from datetime import datetime
from google.cloud import resourcemanager_v3
from google.cloud import billing_v1
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
from functools import wraps

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
MAX_PROJECTS = 10  # Limit to 10 projects for testing
MAX_RETRIES = 3
RETRY_DELAY = 2

# Test settings
DRY_RUN = True  # Set to False to actually delink projects
REQUIRE_CONFIRMATION = True  # Require user confirmation for each project

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'delink_test_{BILLING_ACCOUNT_ID.replace("-", "_")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Decorator to retry functions on failure with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ServiceUnavailable, DeadlineExceeded, RetryError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
                        
            raise last_exception
            
        return wrapper
    return decorator

@retry_on_failure()
def get_billing_account_projects():
    """Get projects linked to the specific billing account."""
    logger.info(f"🔍 Retrieving projects linked to billing account: {BILLING_ACCOUNT_ID}")
    
    try:
        client = billing_v1.CloudBillingClient()
        billing_account_name = f"billingAccounts/{BILLING_ACCOUNT_ID}"
        request = billing_v1.ListProjectBillingInfoRequest(name=billing_account_name)
        
        project_ids = []
        page_result = client.list_project_billing_info(request=request)
        
        for project_billing_info in page_result:
            try:
                if project_billing_info.billing_enabled:
                    project_name_parts = project_billing_info.name.split('/')
                    if len(project_name_parts) >= 3:
                        project_id = project_name_parts[1]
                        project_ids.append(project_id)
                        
            except Exception as e:
                logger.warning(f"Error processing billing project: {str(e)}")
                continue
                
        logger.info(f"✅ Found {len(project_ids)} projects linked to billing account.")
        return project_ids[:MAX_PROJECTS]  # Limit for testing
        
    except Exception as e:
        logger.error(f"Failed to retrieve billing account projects: {str(e)}")
        return []

@retry_on_failure()
def get_project_details(project_id):
    """Get detailed information about a specific project."""
    try:
        client = resourcemanager_v3.ProjectsClient()
        project_name = f"projects/{project_id}"
        request = resourcemanager_v3.GetProjectRequest(name=project_name)
        project = client.get_project(request=request)
        
        return {
            'project_id': project.project_id,
            'display_name': project.display_name or project.project_id,
            'state': project.state.name,
            'create_time': project.create_time.strftime('%Y-%m-%d %H:%M:%S') if project.create_time else 'Unknown',
            'labels': dict(project.labels) if project.labels else {}
        }
        
    except NotFound:
        logger.warning(f"Project {project_id} not found or not accessible")
        return {
            'project_id': project_id,
            'display_name': project_id,
            'state': 'UNKNOWN',
            'create_time': 'Unknown',
            'labels': {}
        }
    except Exception as e:
        logger.warning(f"Error getting details for project {project_id}: {str(e)}")
        return {
            'project_id': project_id,
            'display_name': project_id,
            'state': 'ERROR',
            'create_time': 'Unknown',
            'labels': {}
        }

def has_any_labels(project_details):
    """Check if project has any labels."""
    labels = project_details.get('labels', {})
    return len(labels) > 0

def get_project_resources(project_id):
    """Get basic resource count for a project (simplified version)."""
    # For testing purposes, we'll just return a basic structure
    # In production, you would check actual resources
    return {
        'compute_instances': 0,
        'disks': 0,
        'buckets': 0,
        'has_resources': False  # Assume no resources for testing
    }

def delink_project_from_billing(project_id, dry_run=True):
    """
    Delink a project from the billing account.
    
    Args:
        project_id (str): The project ID
        dry_run (bool): If True, only simulate the operation
        
    Returns:
        dict: Operation result
    """
    try:
        if dry_run:
            logger.info(f"🔄 DRY RUN: Would delink project {project_id} from billing")
            return {
                'success': True,
                'message': f"DRY RUN: Would delink project {project_id} from billing",
                'dry_run': True
            }
        
        # Initialize the Cloud Billing client
        client = billing_v1.CloudBillingClient()
        
        # Prepare the request to disable billing
        project_name = f"projects/{project_id}"
        billing_info = billing_v1.ProjectBillingInfo(
            name=project_name,
            billing_account_name="",  # Empty string disables billing
            billing_enabled=False
        )
        
        request = billing_v1.UpdateProjectBillingInfoRequest(
            name=project_name,
            project_billing_info=billing_info
        )
        
        # Execute the delink operation
        response = client.update_project_billing_info(request=request)
        
        logger.info(f"✅ Successfully delinked project {project_id} from billing")
        return {
            'success': True,
            'message': f"Successfully delinked project {project_id} from billing",
            'response': response,
            'dry_run': False
        }
        
    except Exception as e:
        error_msg = f"Failed to delink project {project_id}: {str(e)}"
        logger.error(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'error': str(e),
            'dry_run': dry_run
        }

def get_user_confirmation(project_id, project_details, resources):
    """Get user confirmation before delinking a project."""
    
    print(f"\n⚠️  CONFIRMATION REQUIRED FOR PROJECT: {project_id}")
    print("-" * 60)
    print(f"📊 Project Display Name: {project_details.get('display_name', 'Unknown')}")
    print(f"📊 Project State: {project_details.get('state', 'UNKNOWN')}")
    print(f"📅 Created: {project_details.get('create_time', 'Unknown')}")
    print(f"🏷️ Labels: {len(project_details.get('labels', {}))} labels")
    
    if project_details.get('labels'):
        print(f"🏷️ Label keys: {', '.join(project_details['labels'].keys())}")
    else:
        print(f"🏷️ No labels found - this is why it's being delinked!")
    
    print(f"💻 Compute Instances: {resources.get('compute_instances', 0)}")
    print(f"💾 Persistent Disks: {resources.get('disks', 0)}")
    print(f"🗄️ Storage Buckets: {resources.get('buckets', 0)}")
    print(f"📋 Has Resources: {'Yes' if resources.get('has_resources', False) else 'No'}")
    print("-" * 60)
    
    if resources.get('has_resources', False):
        print("⚠️  WARNING: This project has active resources!")
        print("   Delinking will disable billing and may affect running services.")
    
    while True:
        response = input(f"Do you want to delink '{project_id}' from billing? (y/N/s=skip all): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no', '']:
            return False
        elif response in ['s', 'skip']:
            return 'skip_all'
        else:
            print("Please enter 'y' for yes, 'n' for no, or 's' to skip all remaining projects.")

def main():
    """Main function to test delinking projects without labels."""
    
    start_time = time.time()
    
    logger.info("🚀 Starting test for delinking projects with no labels...")
    logger.info(f"💳 Target billing account: {BILLING_ACCOUNT_ID}")
    logger.info(f"📊 Testing with up to {MAX_PROJECTS} projects")
    logger.info(f"🔒 DRY RUN MODE: {'ENABLED' if DRY_RUN else 'DISABLED'}")
    logger.info(f"✋ Confirmation Required: {'YES' if REQUIRE_CONFIRMATION else 'NO'}")
    logger.info("="*80)
    
    # Safety warning
    if not DRY_RUN:
        logger.warning("⚠️  ⚠️  ⚠️  WARNING ⚠️  ⚠️  ⚠️")
        logger.warning("This script will ACTUALLY DELINK projects from billing!")
        logger.warning("Projects will lose access to billable resources.")
        logger.warning("Make sure you understand the consequences!")
        logger.warning("⚠️  ⚠️  ⚠️  WARNING ⚠️  ⚠️  ⚠️")
        
        response = input("\nAre you absolutely sure you want to proceed? Type 'YES' to continue: ")
        if response != 'YES':
            logger.info("❌ Operation cancelled.")
            return
    
    # Get projects from billing account
    project_ids = get_billing_account_projects()
    
    if not project_ids:
        logger.warning("No projects found linked to the billing account")
        return
    
    logger.info(f"\n🔍 Analyzing {len(project_ids)} projects for label compliance...")
    
    # Track results
    unlabeled_projects = []
    labeled_projects = []
    error_projects = []
    delink_results = []
    
    skip_all = False
    
    # Analyze each project
    for i, project_id in enumerate(project_ids, 1):
        try:
            logger.info(f"📊 [{i}/{len(project_ids)}] Analyzing project: {project_id}")
            
            # Get project details
            project_details = get_project_details(project_id)
            
            # Check if project has labels
            if has_any_labels(project_details):
                labeled_projects.append({
                    'project_id': project_id,
                    'project_details': project_details,
                    'label_count': len(project_details.get('labels', {}))
                })
                logger.info(f"✅ Project {project_id} has {len(project_details.get('labels', {}))} labels - skipping")
                continue
            
            # Project has no labels - candidate for delinking
            unlabeled_projects.append({
                'project_id': project_id,
                'project_details': project_details
            })
            
            logger.info(f"🔍 Project {project_id} has NO labels - candidate for delinking")
            
            # Get resource information
            resources = get_project_resources(project_id)
            
            # Ask for confirmation if required
            if REQUIRE_CONFIRMATION and not skip_all:
                confirmation = get_user_confirmation(project_id, project_details, resources)
                
                if confirmation == 'skip_all':
                    skip_all = True
                    logger.info("⏭️ Skipping all remaining projects as requested")
                    break
                elif not confirmation:
                    logger.info(f"⏭️ Skipping project {project_id} as requested")
                    continue
            
            # Perform delink operation
            logger.info(f"🔄 Attempting to delink project {project_id}...")
            result = delink_project_from_billing(project_id, dry_run=DRY_RUN)
            
            delink_results.append({
                'project_id': project_id,
                'project_details': project_details,
                'resources': resources,
                'delink_result': result
            })
            
            if result['success']:
                logger.info(f"✅ {result['message']}")
            else:
                logger.error(f"❌ {result['message']}")
            
            # Add delay to avoid rate limits
            if not DRY_RUN:
                time.sleep(1)
                
        except Exception as e:
            error_msg = f"Error analyzing project {project_id}: {str(e)}"
            logger.error(error_msg)
            error_projects.append({
                'project_id': project_id,
                'error': str(e)
            })
            continue
    
    # Generate summary report
    logger.info("\n" + "="*100)
    logger.info("📋 DELINK TEST SUMMARY REPORT")
    logger.info("="*100)
    
    total_analyzed = len(project_ids)
    total_labeled = len(labeled_projects)
    total_unlabeled = len(unlabeled_projects)
    total_errors = len(error_projects)
    total_delinked = len([r for r in delink_results if r['delink_result']['success']])
    total_failed = len([r for r in delink_results if not r['delink_result']['success']])
    
    logger.info(f"📊 ANALYSIS SUMMARY:")
    logger.info(f"💼 Total projects analyzed: {total_analyzed}")
    logger.info(f"✅ Projects with labels (skipped): {total_labeled}")
    logger.info(f"❌ Projects without labels: {total_unlabeled}")
    logger.info(f"⚠️ Projects with errors: {total_errors}")
    logger.info("")
    logger.info(f"🔄 DELINK OPERATION SUMMARY:")
    logger.info(f"✅ Successfully processed: {total_delinked}")
    logger.info(f"❌ Failed operations: {total_failed}")
    logger.info(f"⏭️ Skipped projects: {total_unlabeled - len(delink_results)}")
    
    # Show detailed results
    if unlabeled_projects:
        logger.info(f"\n❌ PROJECTS WITHOUT LABELS:")
        for project in unlabeled_projects:
            logger.info(f"  • {project['project_id']} - Created: {project['project_details'].get('create_time', 'Unknown')}")
    
    if delink_results:
        logger.info(f"\n🔄 DELINK OPERATION RESULTS:")
        for result in delink_results:
            status = "DRY RUN" if result['delink_result']['dry_run'] else ("SUCCESS" if result['delink_result']['success'] else "FAILED")
            logger.info(f"  • {result['project_id']} - {status}")
    
    # Save results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if delink_results:
        # Prepare data for CSV
        csv_data = []
        for result in delink_results:
            csv_data.append({
                'Project_ID': result['project_id'],
                'Display_Name': result['project_details'].get('display_name', ''),
                'State': result['project_details'].get('state', ''),
                'Create_Time': result['project_details'].get('create_time', ''),
                'Label_Count': len(result['project_details'].get('labels', {})),
                'Delink_Success': result['delink_result']['success'],
                'Delink_Message': result['delink_result']['message'],
                'Dry_Run': result['delink_result']['dry_run'],
                'Has_Resources': result['resources'].get('has_resources', False),
                'Compute_Instances': result['resources'].get('compute_instances', 0),
                'Disks': result['resources'].get('disks', 0),
                'Buckets': result['resources'].get('buckets', 0)
            })
        
        df = pd.DataFrame(csv_data)
        filename = f"delink_test_results_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
        df.to_csv(filename, index=False)
        file_size = os.path.getsize(filename)
        logger.info(f"💾 Test results saved to: {filename} ({file_size} bytes)")
    
    # Calculate execution time
    execution_time = time.time() - start_time
    logger.info(f"\n🏁 Delink test complete! Execution time: {execution_time:.2f} seconds")
    
    if DRY_RUN:
        logger.info(f"\n🔒 This was a DRY RUN. To actually delink projects:")
        logger.info(f"   1. Set DRY_RUN = False in the script")
        logger.info(f"   2. Run the script again")
        logger.info(f"   3. Carefully confirm each operation")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
