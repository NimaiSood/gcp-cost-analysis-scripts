#!/usr/bin/env python3
"""
Simple script to list projects linked to a billing account for debugging.
"""

from google.cloud import billing_v1

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"

def list_projects_simple():
    """Simple function to list projects and debug the output."""
    
    print(f"🔍 Retrieving projects for billing account: {BILLING_ACCOUNT_ID}...")
    
    try:
        # Initialize the Cloud Billing client
        client = billing_v1.CloudBillingClient()
        
        # Get all projects under the billing account
        billing_account_name = f"billingAccounts/{BILLING_ACCOUNT_ID}"
        request = billing_v1.ListProjectBillingInfoRequest(
            name=billing_account_name
        )
        
        projects = []
        page_result = client.list_project_billing_info(request=request)
        
        print("Sample project billing info objects:")
        count = 0
        for project_billing_info in page_result:
            count += 1
            if count <= 5:  # Show first 5 for debugging
                print(f"Project {count}:")
                print(f"  Name: {project_billing_info.name}")
                print(f"  Billing Enabled: {project_billing_info.billing_enabled}")
                print(f"  Billing Account: {project_billing_info.billing_account_name}")
                print()
            
            if project_billing_info.billing_enabled:
                projects.append(project_billing_info.name)
            
            if count >= 100:  # Limit to first 100 for testing
                break
        
        print(f"✅ Found {len(projects)} active projects (showing first 100).")
        print("\nFirst 10 project IDs:")
        for i, project_name in enumerate(projects[:10], 1):
            print(f"{i:2}. {project_name}")
        
        return projects
        
    except Exception as e:
        print(f"❌ Error retrieving projects: {str(e)}")
        return []

if __name__ == "__main__":
    list_projects_simple()
