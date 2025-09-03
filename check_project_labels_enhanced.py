#!/usr/bin/env python3
"""
Enhanced script to check project-level labels for projects linked to a billing account.
This script checks actual project labels, not just resource labels.

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

def check_project_labels_gcloud(project_id):
    """
    Check project-level labels using gcloud CLI.
    This is more reliable than API calls for project metadata.
    
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
        
    except subprocess.TimeoutExpired:
        return {
            'has_labels': None,
            'labels': {},
            'label_count': 0,
            'error': "Timeout waiting for gcloud response",
            'status': 'error'
        }
    except json.JSONDecodeError as e:
        return {
            'has_labels': None,
            'labels': {},
            'label_count': 0,
            'error': f"JSON parsing error: {str(e)}",
            'status': 'error'
        }
    except Exception as e:
        return {
            'has_labels': None,
            'labels': {},
            'label_count': 0,
            'error': str(e),
            'status': 'error'
        }

def check_required_labels(labels, required_labels=None):
    """
    Check if project has required labels.
    
    Args:
        labels (dict): Project labels
        required_labels (list): List of required label keys
        
    Returns:
        dict: Compliance information
    """
    if required_labels is None:
        # Define common required labels for compliance
        required_labels = ['environment', 'owner', 'cost-center', 'application']
    
    missing_labels = []
    present_labels = []
    
    for req_label in required_labels:
        if req_label in labels:
            present_labels.append(req_label)
        else:
            missing_labels.append(req_label)
    
    compliance_score = len(present_labels) / len(required_labels) * 100 if required_labels else 100
    
    return {
        'required_labels': required_labels,
        'present_labels': present_labels,
        'missing_labels': missing_labels,
        'compliance_score': compliance_score,
        'is_compliant': len(missing_labels) == 0
    }

def main():
    """Main function to check projects for missing labels."""
    
    print("🚀 Starting enhanced script to check PROJECT-LEVEL labels...")
    print(f"📊 Target billing account: {BILLING_ACCOUNT_ID}")
    print(f"🔢 Limiting analysis to top {MAX_PROJECTS} projects for efficiency")
    print("🔍 This script checks actual project labels (not just resource labels)")
    print("=" * 80)
    
    # Check if gcloud is available
    try:
        subprocess.run(['gcloud', '--version'], capture_output=True, check=True)
        print("✅ gcloud CLI is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ gcloud CLI is not available. Please install Google Cloud SDK.")
        print("   Visit: https://cloud.google.com/sdk/docs/install")
        return
    
    # Get all projects under the billing account
    projects = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
    
    if not projects:
        print("❌ No projects found. Exiting.")
        return
    
    print(f"\n🏷️ Checking PROJECT-LEVEL label compliance for {len(projects)} projects (top {MAX_PROJECTS})...")
    print("(Using gcloud CLI for accurate project metadata)")
    
    # Check labels for each project
    unlabeled_projects = []
    labeled_projects = []
    error_projects = []
    
    for i, project in enumerate(projects, 1):
        project_id = project['project_id']
        print(f"📊 [{i:3}/{len(projects)}] Checking project: {project_id}")
        
        label_info = check_project_labels_gcloud(project_id)
        
        # Check compliance with required labels
        if label_info['status'] == 'has_labels':
            compliance = check_required_labels(label_info['labels'])
            label_info.update(compliance)
        else:
            label_info.update({
                'required_labels': ['environment', 'owner', 'cost-center', 'application'],
                'present_labels': [],
                'missing_labels': ['environment', 'owner', 'cost-center', 'application'],
                'compliance_score': 0,
                'is_compliant': False
            })
        
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
    
    # Display results
    print("\n" + "=" * 120)
    print(f"📊 PROJECT-LEVEL LABEL COMPLIANCE REPORT (TOP {MAX_PROJECTS} PROJECTS)")
    print("=" * 120)
    
    print(f"\n🚨 PROJECTS MISSING LABELS ({len(unlabeled_projects)} projects):")
    print("-" * 100)
    if unlabeled_projects:
        print(f"{'#':<4} {'Project ID':<35} {'State':<12} {'Compliance':<12}")
        print("-" * 100)
        for i, project in enumerate(unlabeled_projects, 1):
            state = project.get('lifecycle_state', 'UNKNOWN')
            compliance = f"{project.get('compliance_score', 0):.0f}%"
            print(f"{i:<4} {project['project_id']:<35} {state:<12} {compliance:<12}")
    else:
        print("✅ No projects with missing labels found!")
    
    print(f"\n✅ PROJECTS WITH LABELS ({len(labeled_projects)} projects):")
    print("-" * 100)
    if labeled_projects:
        print(f"{'#':<4} {'Project ID':<35} {'Label Count':<12} {'Compliance':<12}")
        print("-" * 100)
        for i, project in enumerate(labeled_projects, 1):
            label_count = project.get('label_count', 0)
            compliance = f"{project.get('compliance_score', 0):.0f}%"
            print(f"{i:<4} {project['project_id']:<35} {label_count:<12} {compliance:<12}")
            
            # Show labels for first few projects
            if i <= 3:
                labels_str = ", ".join([f"{k}={v}" for k, v in project.get('labels', {}).items()])
                if labels_str:
                    print(f"      Labels: {labels_str[:80]}...")
    
    if error_projects:
        print(f"\n⚠️ PROJECTS WITH ERRORS ({len(error_projects)} projects):")
        print("-" * 100)
        for i, project in enumerate(error_projects[:5], 1):  # Show first 5 errors
            error_msg = project.get('error', 'Unknown error')[:60]
            print(f"  {i}. {project['project_id']}: {error_msg}...")
    
    # Summary statistics
    print(f"\n📈 SUMMARY STATISTICS:")
    print(f"📊 Total projects analyzed: {len(projects)} (top {MAX_PROJECTS})")
    print(f"🚨 Projects missing labels: {len(unlabeled_projects)}")
    print(f"✅ Projects with labels: {len(labeled_projects)}")
    print(f"⚠️ Projects with errors: {len(error_projects)}")
    
    total_checkable = len(unlabeled_projects) + len(labeled_projects)
    if total_checkable > 0:
        compliance_rate = (len(labeled_projects) / total_checkable) * 100
        print(f"📊 Overall label compliance rate: {compliance_rate:.1f}%")
        
        # Analyze by project type
        tp_unlabeled = [p for p in unlabeled_projects if p['project_id'].endswith('-tp')]
        g1p_unlabeled = [p for p in unlabeled_projects if p['project_id'].startswith('g1p-')]
        
        print(f"🏷️ Unlabeled '-tp' projects: {len(tp_unlabeled)}")
        print(f"🏷️ Unlabeled 'g1p-' projects: {len(g1p_unlabeled)}")
    
    # Save detailed results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save unlabeled projects
    if unlabeled_projects:
        unlabeled_csv = f"unlabeled_projects_detailed_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
        unlabeled_df = pd.DataFrame(unlabeled_projects)
        unlabeled_df.to_csv(unlabeled_csv, index=False)
        print(f"\n💾 Unlabeled projects saved to: {unlabeled_csv}")
    
    # Save all results
    all_results_csv = f"all_projects_label_check_detailed_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
    all_results = unlabeled_projects + labeled_projects + error_projects
    all_df = pd.DataFrame(all_results)
    all_df.to_csv(all_results_csv, index=False)
    print(f"💾 Complete results saved to: {all_results_csv}")
    
    print(f"\n🏁 Enhanced label compliance check complete!")
    
    # Highlight critical findings
    if unlabeled_projects:
        print(f"\n🚨 ACTION REQUIRED: {len(unlabeled_projects)} projects need labels added!")
        print("Priority projects to label (first 10):")
        for i, project in enumerate(unlabeled_projects[:10], 1):
            missing_count = len(project.get('missing_labels', []))
            print(f"  {i:2}. {project['project_id']} (missing {missing_count} required labels)")
        
        # Show specific missing labels for top projects
        print(f"\n📋 Required labels to add:")
        print("   Common required labels: environment, owner, cost-center, application")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
