#!/usr/bin/env python3
"""
Script to check which projects linked to a billing account are missing labels.
This script will identify projects without proper labeling for compliance.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
from datetime import datetime
from google.cloud import billing_v1
from google.cloud import compute_v1
import pandas as pd

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
MAX_PROJECTS = 100

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

def check_project_labels(project_id):
    """
    Check if a project has labels by examining compute instances.
    Since we don't have resource manager access, we'll check instance labels as a proxy.
    
    Args:
        project_id (str): The project ID
        
    Returns:
        dict: Label information and status
    """
    try:
        # Initialize the Compute Engine client
        compute_client = compute_v1.InstancesClient()
        
        # Get aggregated list of instances across all zones
        request = compute_v1.AggregatedListInstancesRequest(
            project=project_id
        )
        
        instance_labels = {}
        has_labels = False
        total_instances = 0
        labeled_instances = 0
        
        # Check instances for labels
        try:
            page_result = compute_client.aggregated_list(request=request)
            
            for zone, instances_scoped_list in page_result:
                if instances_scoped_list.instances:
                    for instance in instances_scoped_list.instances:
                        total_instances += 1
                        if instance.labels:
                            has_labels = True
                            labeled_instances += 1
                            # Collect unique labels
                            for key, value in instance.labels.items():
                                if key not in instance_labels:
                                    instance_labels[key] = set()
                                instance_labels[key].add(value)
        
        except Exception as compute_error:
            # If Compute Engine API is not enabled or accessible, we can't check instance labels
            return {
                'has_labels': None,
                'total_instances': 0,
                'labeled_instances': 0,
                'unique_labels': {},
                'error': str(compute_error),
                'status': 'unknown'
            }
        
        # Convert sets to lists for JSON serialization
        unique_labels = {k: list(v) for k, v in instance_labels.items()}
        
        # Determine status
        if total_instances == 0:
            status = 'no_instances'
        elif has_labels:
            status = 'has_labels'
        else:
            status = 'missing_labels'
        
        return {
            'has_labels': has_labels,
            'total_instances': total_instances,
            'labeled_instances': labeled_instances,
            'unique_labels': unique_labels,
            'error': None,
            'status': status
        }
        
    except Exception as e:
        return {
            'has_labels': None,
            'total_instances': 0,
            'labeled_instances': 0,
            'unique_labels': {},
            'error': str(e),
            'status': 'error'
        }

def main():
    """Main function to check projects for missing labels."""
    
    print("🚀 Starting script to check projects for missing labels...")
    print(f"📊 Target billing account: {BILLING_ACCOUNT_ID}")
    print(f"🔢 Limiting analysis to top {MAX_PROJECTS} projects for efficiency")
    print("=" * 80)
    
    # Get all projects under the billing account
    projects = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
    
    if not projects:
        print("❌ No projects found. Exiting.")
        return
    
    print(f"\n🏷️ Checking label compliance for {len(projects)} projects (top {MAX_PROJECTS})...")
    print("(This may take a while as we check each project's resources)")
    
    # Check labels for each project
    unlabeled_projects = []
    labeled_projects = []
    error_projects = []
    no_instance_projects = []
    
    for i, project in enumerate(projects, 1):
        project_id = project['project_id']
        print(f"📊 [{i}/{len(projects)}] Checking project: {project_id}")
        
        label_info = check_project_labels(project_id)
        
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
        elif label_info['status'] == 'no_instances':
            no_instance_projects.append(project_result)
        else:
            error_projects.append(project_result)
        
        # Progress indicator
        if i % 10 == 0:
            print(f"✅ Processed {i}/{len(projects)} projects")
    
    # Display results
    print("\n" + "=" * 100)
    print(f"📊 LABEL COMPLIANCE REPORT (TOP {MAX_PROJECTS} PROJECTS)")
    print("=" * 100)
    
    print(f"\n🚨 PROJECTS MISSING LABELS ({len(unlabeled_projects)} projects):")
    print("-" * 80)
    if unlabeled_projects:
        print(f"{'#':<4} {'Project ID':<35} {'Instances':<10} {'Status':<15}")
        print("-" * 80)
        for i, project in enumerate(unlabeled_projects, 1):
            print(f"{i:<4} {project['project_id']:<35} {project['total_instances']:<10} {project['status']:<15}")
    else:
        print("✅ No projects with missing labels found!")
    
    print(f"\n✅ PROJECTS WITH LABELS ({len(labeled_projects)} projects):")
    print("-" * 80)
    if labeled_projects:
        print(f"{'#':<4} {'Project ID':<35} {'Labeled/Total':<15} {'Unique Labels':<10}")
        print("-" * 80)
        for i, project in enumerate(labeled_projects[:10], 1):  # Show first 10
            label_ratio = f"{project['labeled_instances']}/{project['total_instances']}"
            label_count = len(project['unique_labels'])
            print(f"{i:<4} {project['project_id']:<35} {label_ratio:<15} {label_count:<10}")
        if len(labeled_projects) > 10:
            print(f"     ... and {len(labeled_projects) - 10} more projects with labels")
    
    print(f"\n📋 PROJECTS WITH NO INSTANCES ({len(no_instance_projects)} projects):")
    print("-" * 80)
    if no_instance_projects:
        sample_size = min(10, len(no_instance_projects))
        for i, project in enumerate(no_instance_projects[:sample_size], 1):
            print(f"  {i:2}. {project['project_id']}")
        if len(no_instance_projects) > 10:
            print(f"     ... and {len(no_instance_projects) - 10} more projects without instances")
    
    if error_projects:
        print(f"\n⚠️ PROJECTS WITH ERRORS ({len(error_projects)} projects):")
        print("-" * 80)
        for i, project in enumerate(error_projects[:5], 1):  # Show first 5 errors
            print(f"  {i}. {project['project_id']}: {project['error'][:60]}...")
    
    # Summary statistics
    print(f"\n📈 SUMMARY STATISTICS:")
    print(f"📊 Total projects analyzed: {len(projects)} (top {MAX_PROJECTS})")
    print(f"🚨 Projects missing labels: {len(unlabeled_projects)}")
    print(f"✅ Projects with labels: {len(labeled_projects)}")
    print(f"📋 Projects with no instances: {len(no_instance_projects)}")
    print(f"⚠️ Projects with errors: {len(error_projects)}")
    
    compliance_rate = (len(labeled_projects) / (len(labeled_projects) + len(unlabeled_projects))) * 100 if (len(labeled_projects) + len(unlabeled_projects)) > 0 else 0
    print(f"📊 Label compliance rate: {compliance_rate:.1f}%")
    
    # Save detailed results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save unlabeled projects
    if unlabeled_projects:
        unlabeled_csv = f"unlabeled_projects_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
        unlabeled_df = pd.DataFrame(unlabeled_projects)
        unlabeled_df.to_csv(unlabeled_csv, index=False)
        print(f"\n💾 Unlabeled projects saved to: {unlabeled_csv}")
    
    # Save all results
    all_results_csv = f"all_projects_label_check_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
    all_results = unlabeled_projects + labeled_projects + no_instance_projects + error_projects
    all_df = pd.DataFrame(all_results)
    all_df.to_csv(all_results_csv, index=False)
    print(f"💾 Complete results saved to: {all_results_csv}")
    
    print(f"\n🏁 Label compliance check complete!")
    
    # Highlight critical findings
    if unlabeled_projects:
        print(f"\n🚨 ACTION REQUIRED: {len(unlabeled_projects)} projects need labels added!")
        print("Priority projects to label:")
        for i, project in enumerate(unlabeled_projects[:5], 1):
            print(f"  {i}. {project['project_id']} ({project['total_instances']} instances)")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
