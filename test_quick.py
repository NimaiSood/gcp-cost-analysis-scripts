#!/usr/bin/env python3

from google.cloud import billing_v1

def quick_test():
    print("Starting quick test...")
    
    billing_client = billing_v1.CloudBillingClient()
    billing_account_id = "01227B-3F83E7-AC2416"
    billing_account_name = f"billingAccounts/{billing_account_id}"
    
    print(f"🔎 Scanning projects under billing account: {billing_account_id}...")
    
    try:
        projects = [
            project.project_id
            for project in billing_client.list_project_billing_info(parent=billing_account_name)
        ]
        print(f"✅ Found {len(projects)} projects")
        for i, project in enumerate(projects[:5]):  # Show first 5
            print(f"  {i+1}. {project}")
        if len(projects) > 5:
            print(f"  ... and {len(projects) - 5} more projects")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        
    print("Quick test completed!")

if __name__ == "__main__":
    quick_test()
