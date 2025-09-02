#!/usr/bin/env python3

import sys
from google.cloud import billing_v1

def diagnose_billing_access():
    print("🔍 Diagnosing Google Cloud Billing access...")
    
    try:
        client = billing_v1.CloudBillingClient()
        print("✅ Successfully created billing client")
        
        # Try to list all billing accounts the user has access to
        print("\n📋 Listing available billing accounts...")
        accounts = list(client.list_billing_accounts())
        
        if accounts:
            print(f"✅ Found {len(accounts)} billing account(s):")
            for account in accounts:
                print(f"   - ID: {account.name.split('/')[-1]}")
                print(f"     Display Name: {account.display_name}")
                print(f"     Open: {account.open}")
                print()
        else:
            print("❌ No billing accounts found. You may not have billing permissions.")
            
    except Exception as e:
        print(f"❌ Error accessing billing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    diagnose_billing_access()
