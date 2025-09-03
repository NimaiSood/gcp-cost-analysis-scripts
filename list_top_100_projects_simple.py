#!/usr/bin/env python3
"""
Script to list the top 100 projects linked to a specific billing account.
Simplified version without cost analysis.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
from datetime import datetime
from google.cloud import billing_v1
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
        return projects
        
    except Exception as e:
        print(f"❌ Error retrieving projects: {str(e)}")
        return []

def main():
    """Main function to list top 100 projects."""
    
    print("🚀 Starting script to list top 100 projects...")
    print(f"📊 Target billing account: {BILLING_ACCOUNT_ID}")
    print("=" * 80)
    
    # Get all projects under the billing account
    projects = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
    
    if not projects:
        print("❌ No projects found. Exiting.")
        return
    
    # Limit to first 100 projects (could be enhanced to sort by other criteria)
    top_projects = projects[:MAX_PROJECTS]
    
    # Display results
    print("\n" + "=" * 120)
    print(f"📊 TOP {len(top_projects)} PROJECTS LINKED TO BILLING ACCOUNT {BILLING_ACCOUNT_ID}")
    print("=" * 120)
    
    print(f"{'#':<4} {'Project ID':<35} {'Full Project Name':<50} {'Billing Enabled':<15}")
    print("-" * 120)
    
    for i, project in enumerate(top_projects, 1):
        print(f"{i:<4} {project['project_id']:<35} {project['project_name']:<50} {str(project['billing_enabled']):<15}")
    
    print("-" * 120)
    print(f"TOTAL: {len(top_projects)} projects")
    print("=" * 120)
    
    # Summary statistics
    print(f"\n📈 SUMMARY:")
    print(f"📊 Total projects found: {len(projects)}")
    print(f"📋 Showing top: {len(top_projects)}")
    
    # Analyze project naming patterns
    tp_projects = [p for p in top_projects if p['project_id'].endswith('-tp')]
    g1p_projects = [p for p in top_projects if p['project_id'].startswith('g1p-')]
    
    print(f"🏷️ Projects ending with '-tp': {len(tp_projects)}")
    print(f"🏷️ Projects starting with 'g1p-': {len(g1p_projects)}")
    
    # Save to CSV for further analysis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"top_{MAX_PROJECTS}_projects_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
    
    # Prepare data for CSV
    csv_data = []
    for i, project in enumerate(top_projects, 1):
        csv_data.append({
            'rank': i,
            'project_id': project['project_id'],
            'full_project_name': project['project_name'],
            'billing_enabled': project['billing_enabled'],
            'billing_account': project['billing_account_name']
        })
    
    df = pd.DataFrame(csv_data)
    df.to_csv(csv_filename, index=False)
    print(f"\n💾 Results saved to: {csv_filename}")
    
    print(f"\n🏁 Analysis complete! Listed top {len(top_projects)} projects.")
    
    # Show sample project IDs for verification
    print(f"\n📋 Sample Project IDs:")
    for i, project in enumerate(top_projects[:10], 1):
        print(f"  {i:2}. {project['project_id']}")
    
    if len(top_projects) > 10:
        print(f"     ... and {len(top_projects) - 10} more projects")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
