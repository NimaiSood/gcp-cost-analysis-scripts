from google.cloud import billing_v1
client = billing_v1.CloudBillingClient()
billing_account_id = '01227B-3F83E7-AC2416'
billing_account_name = f'billingAccounts/{billing_account_id}'
print(f'Trying to access: {billing_account_name}')
try:
    projects = list(client.list_project_billing_info(name=billing_account_name))
    print(f'Found {len(projects)} projects')
    for project in projects[:3]:  # Show first 3
        print(f'  - {project.project_id}')
except Exception as e:
    print(f'Error: {e}')
    print(f'Error type: {type(e).__name__}')
    import traceback
    traceback.print_exc()
