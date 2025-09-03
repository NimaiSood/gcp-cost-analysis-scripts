#!/usr/bin/env python3
"""
Script to delink projects from billing account if they don't have project-level labels.
This script includes safety checks and confirmation prompts.

⚠️  WARNING: This script will disable billing for projects without labels!
    Projects will lose access to billable resources until billing is re-enabled.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
from datetime import datetime
from google.cloud import billing_v1
import pandas as pd
import subprocess
import json
import time

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
MAX_PROJECTS = 100

# Safety settings
DRY_RUN = True  # Set to False to actually delink projects
REQUIRE_CONFIRMATION = True  # Require user confirmation for each project

def get_projects_under_billing_account(billing_account_id):
    """
    Get all projects linked to a specific billing account.
    
    Args:
        billing_account_id (str): The billing account ID
        
    Returns:
        list: List of project objects
    """
    print(f"🔍 Retrieving projects for billing account: {billing_account_id}...")
    
    try:
        # Initialize the Cloud Billing client
        client = billing_v1.CloudBillingClient()
        
        # Get all projects under the billing account
        billing_account_name = f"billingAccounts/{billing_account_id}"
        request = billing_v1.ListProjectBillingInfoRequest(
            name=billing_account_name
        )
        
        projects = []
        page_result = client.list_project_billing_info(request=request)
        
        for project_billing_info in page_result:
            if project_billing_info.billing_enabled:
                # Extract project ID from the project name (format: projects/PROJECT_ID/billingInfo)
                project_name_parts = project_billing_info.name.split('/')
                if len(project_name_parts) >= 3:
                    project_id = project_name_parts[1]  # Get the PROJECT_ID part
                else:
                    project_id = project_billing_info.name  # Fallback to full name
                
                projects.append({
                    'project_id': project_id,
                    'project_name': project_billing_info.name,
                    'billing_enabled': project_billing_info.billing_enabled,
                    'billing_account_name': project_billing_info.billing_account_name
                })
        
        print(f"✅ Found {len(projects)} active projects linked to billing account.")
        return projects[:MAX_PROJECTS]  # Limit to top MAX_PROJECTS
        
    except Exception as e:
        print(f"❌ Error retrieving projects: {str(e)}")
        return []

def check_project_labels_gcloud(project_id):
    """
    Check project-level labels using gcloud CLI.
    
    Args:
        project_id (str): The project ID
        
    Returns:
        dict: Label information and status
    """
    try:
        # Use gcloud to get project information including labels
        cmd = [
            'gcloud', 'projects', 'describe', project_id, 
            '--format=json'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return {
                'has_labels': None,
                'labels': {},
                'label_count': 0,
                'error': f"gcloud error: {result.stderr.strip()}",
                'status': 'error'
            }
        
        # Parse the JSON output
        project_info = json.loads(result.stdout)
        
        # Extract labels if they exist
        labels = project_info.get('labels', {})
        has_labels = len(labels) > 0
        
        # Determine status
        if has_labels:
            status = 'has_labels'
        else:
            status = 'missing_labels'
        
        return {
            'has_labels': has_labels,
            'labels': labels,
            'label_count': len(labels),
            'project_number': project_info.get('projectNumber'),
            'lifecycle_state': project_info.get('lifecycleState'),
            'create_time': project_info.get('createTime'),
            'error': None,
            'status': status
        }
        
    except Exception as e:
        return {
            'has_labels': None,
            'labels': {},
            'label_count': 0,
            'error': str(e),
            'status': 'error'
        }

def check_project_resources(project_id):
    """
    Check if project has any billable resources using gcloud commands.
    
    Args:
        project_id (str): The project ID
        
    Returns:
        dict: Resource information
    """
    resources = {
        'compute_instances': 0,
        'disks': 0,
        'buckets': 0,
        'has_resources': False
    }
    
    try:
        # Check compute instances
        cmd = ['gcloud', 'compute', 'instances', 'list', '--project', project_id, '--format=value(name)']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            instances = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            resources['compute_instances'] = len(instances)
        
        # Check persistent disks
        cmd = ['gcloud', 'compute', 'disks', 'list', '--project', project_id, '--format=value(name)']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            disks = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            resources['disks'] = len(disks)
        
        # Check storage buckets
        cmd = ['gsutil', 'ls', '-p', project_id]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            buckets = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            resources['buckets'] = len(buckets)
        
        # Determine if project has resources
        resources['has_resources'] = (
            resources['compute_instances'] > 0 or 
            resources['disks'] > 0 or 
            resources['buckets'] > 0
        )
        
    except Exception as e:
        print(f"⚠️ Error checking resources for {project_id}: {str(e)}")
    
    return resources

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
        
        return {
            'success': True,
            'message': f"Successfully delinked project {project_id} from billing",
            'response': response,
            'dry_run': False
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Failed to delink project {project_id}: {str(e)}",
            'error': str(e),
            'dry_run': dry_run
        }

def get_user_confirmation(project_id, project_info, resources):
    """
    Get user confirmation before delinking a project.
    
    Args:
        project_id (str): The project ID
        project_info (dict): Project information
        resources (dict): Resource information
        
    Returns:
        bool: True if user confirms, False otherwise
    """
    print(f"\n⚠️  CONFIRMATION REQUIRED FOR PROJECT: {project_id}")
    print("-" * 60)
    print(f"📊 Project State: {project_info.get('lifecycle_state', 'UNKNOWN')}")
    print(f"🏷️ Labels: {project_info.get('label_count', 0)} labels")
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
    """Main function to delink projects without labels from billing account."""
    
    print("🚀 Starting script to delink unlabeled projects from billing...")
    print(f"📊 Target billing account: {BILLING_ACCOUNT_ID}")
    print(f"🔢 Limiting analysis to top {MAX_PROJECTS} projects")
    print(f"🔒 DRY RUN MODE: {'ENABLED' if DRY_RUN else 'DISABLED'}")
    print(f"✋ Confirmation Required: {'YES' if REQUIRE_CONFIRMATION else 'NO'}")
    print("=" * 80)
    
    # Safety warning
    if not DRY_RUN:
        print("⚠️  ⚠️  ⚠️  WARNING ⚠️  ⚠️  ⚠️")
        print("This script will ACTUALLY DELINK projects from billing!")
        print("Projects will lose access to billable resources.")
        print("Make sure you understand the consequences!")
        print("⚠️  ⚠️  ⚠️  WARNING ⚠️  ⚠️  ⚠️")
        
        response = input("\nAre you absolutely sure you want to proceed? Type 'YES' to continue: ")
        if response != 'YES':
            print("❌ Operation cancelled.")
            return
    
    # Check if gcloud is available
    try:
        subprocess.run(['gcloud', '--version'], capture_output=True, check=True)
        print("✅ gcloud CLI is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ gcloud CLI is not available. Please install Google Cloud SDK.")
        return
    
    # Get all projects under the billing account
    projects = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
    
    if not projects:
        print("❌ No projects found. Exiting.")
        return
    
    print(f"\n🏷️ Checking projects for labels and preparing delink candidates...")
    
    # Check labels for each project
    unlabeled_projects = []
    labeled_projects = []
    error_projects = []
    
    for i, project in enumerate(projects, 1):
        project_id = project['project_id']
        print(f"📊 [{i:3}/{len(projects)}] Checking project: {project_id}")
        
        label_info = check_project_labels_gcloud(project_id)
        
        # Combine project info with label info
        project_result = {
            **project,
            **label_info
        }
        
        # Categorize projects
        if label_info['status'] == 'missing_labels':
            unlabeled_projects.append(project_result)
        elif label_info['status'] == 'has_labels':
            labeled_projects.append(project_result)
        else:
            error_projects.append(project_result)
        
        # Progress indicator
        if i % 10 == 0 or i == len(projects):
            print(f"✅ Processed {i}/{len(projects)} projects")
    
    # Display summary
    print("\n" + "=" * 100)
    print(f"📊 DELINK CANDIDATES SUMMARY")
    print("=" * 100)
    print(f"🚨 Projects without labels (candidates for delinking): {len(unlabeled_projects)}")
    print(f"✅ Projects with labels (will keep billing): {len(labeled_projects)}")
    print(f"⚠️ Projects with errors (will skip): {len(error_projects)}")
    
    if not unlabeled_projects:
        print("\n✅ No unlabeled projects found. Nothing to delink!")
        return
    
    # Show candidates for delinking
    print(f"\n🎯 PROJECTS TO DELINK:")
    print("-" * 80)
    for i, project in enumerate(unlabeled_projects, 1):
        print(f"  {i:2}. {project['project_id']} (State: {project.get('lifecycle_state', 'UNKNOWN')})")
    
    if DRY_RUN:
        print(f"\n🔒 DRY RUN MODE: No actual delinking will occur.")
    
    # Process delinking
    delinked_projects = []
    skipped_projects = []
    failed_projects = []
    skip_all = False
    
    for i, project in enumerate(unlabeled_projects, 1):
        project_id = project['project_id']
        
        if skip_all:
            skipped_projects.append(project)
            continue
        
        print(f"\n🔄 [{i}/{len(unlabeled_projects)}] Processing project: {project_id}")
        
        # Check resources before delinking
        resources = check_project_resources(project_id)
        
        # Get confirmation if required
        if REQUIRE_CONFIRMATION and not DRY_RUN:
            confirmation = get_user_confirmation(project_id, project, resources)
            if confirmation == 'skip_all':
                skip_all = True
                skipped_projects.append(project)
                continue
            elif not confirmation:
                skipped_projects.append(project)
                continue
        
        # Perform delinking
        result = delink_project_from_billing(project_id, dry_run=DRY_RUN)
        
        if result['success']:
            delinked_projects.append({**project, 'delink_result': result, 'resources': resources})
            print(f"✅ {result['message']}")
        else:
            failed_projects.append({**project, 'delink_result': result, 'resources': resources})
            print(f"❌ {result['message']}")
        
        # Add small delay to avoid rate limits
        if not DRY_RUN:
            time.sleep(1)
    
    # Final summary
    print("\n" + "=" * 100)
    print("📊 DELINKING OPERATION SUMMARY")
    print("=" * 100)
    print(f"✅ Successfully processed: {len(delinked_projects)}")
    print(f"⏭️ Skipped projects: {len(skipped_projects)}")
    print(f"❌ Failed operations: {len(failed_projects)}")
    
    if delinked_projects:
        print(f"\n✅ SUCCESSFULLY PROCESSED PROJECTS:")
        for project in delinked_projects:
            status = "DRY RUN" if project['delink_result']['dry_run'] else "DELINKED"
            print(f"  • {project['project_id']} - {status}")
    
    if failed_projects:
        print(f"\n❌ FAILED OPERATIONS:")
        for project in failed_projects:
            print(f"  • {project['project_id']} - {project['delink_result']['message']}")
    
    # Save results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if delinked_projects:
        delinked_csv = f"delinked_projects_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
        delinked_df = pd.DataFrame(delinked_projects)
        delinked_df.to_csv(delinked_csv, index=False)
        print(f"\n💾 Delinked projects saved to: {delinked_csv}")
    
    # Save complete results
    all_results = delinked_projects + skipped_projects + failed_projects
    if all_results:
        results_csv = f"delink_operation_results_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
        results_df = pd.DataFrame(all_results)
        results_df.to_csv(results_csv, index=False)
        print(f"💾 Complete operation results saved to: {results_csv}")
    
    print(f"\n🏁 Delinking operation complete!")
    
    if DRY_RUN:
        print(f"\n🔒 This was a DRY RUN. To actually delink projects:")
        print(f"   1. Set DRY_RUN = False in the script")
        print(f"   2. Run the script again")
        print(f"   3. Carefully confirm each operation")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
