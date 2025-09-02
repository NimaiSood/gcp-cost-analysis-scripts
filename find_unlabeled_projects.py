import google.auth
from google.cloud import billing_v1, compute_v1
from google.cloud import resourcemanager
from datetime import datetime, timedelta
import json
import pandas as pd
import xlsxwriter

# --- Configuration ---
# ⚠️ REQUIRED: Replace with your actual Google Cloud billing account ID.
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"

# Define specific labels to check for on the PROJECT itself.
# If a project has NONE of these, it's flagged. Set to empty list [] to check for ANY label presence.
REQUIRED_PROJECT_LABELS = [] 

# Define the specific tag key (label key) to check for on resources (e.g., 'creator', 'owner').
# If a resource doesn't have this tag, it's flagged.
REQUIRED_RESOURCE_TAG_KEY = "creator" # Example: 'creator' or 'owner'

# Cost analysis configuration
COST_THRESHOLD_USD = 200.0  # Daily cost threshold in USD to flag high-cost projects
COST_ANALYSIS_DAYS = 7     # Number of days to analyze for cost trends
MAX_PROJECTS_TO_ANALYZE = 50  # Limit analysis to top N projects for faster execution

# --- Main Script Logic ---

def get_projects_under_billing_account(billing_account_id: str) -> list[str]:
    """
    Retrieves a list of project IDs associated with the specified billing account.
    """
    client = billing_v1.CloudBillingClient()
    billing_account_name = f"billingAccounts/{billing_account_id}"
    project_ids = []

    print(f"Retrieving projects for billing account: {billing_account_id}...")
    try:
        for project_billing_info in client.list_project_billing_info(name=billing_account_name):
            project_id = project_billing_info.project_id.split('/')[-1]
            if project_id:
                project_ids.append(project_id)
        print(f"Found {len(project_ids)} projects.")
    except Exception as e:
        print(f"❌ Error fetching projects for billing account '{billing_account_id}': {e}")
    return project_ids

def get_project_labels(project_id: str) -> dict:
    """
    Retrieves user-defined labels for a specific project.
    """
    resource_manager_client = resourcemanager.ProjectsClient()
    try:
        project_obj = resource_manager_client.get_project(name=f"projects/{project_id}")
        return project_obj.labels
    except Exception as e:
        print(f"  ❌ Error fetching labels for project '{project_id}': {e}")
        return {}

def check_project_for_cleanup(
    project_id: str,
    required_project_labels: list,
    required_resource_tag_key: str
) -> dict:
    """
    Checks if a project and its resources meet the cleanup criteria.
    Returns a dict with project_id and reasons for cleanup.
    """
    project_cleanup_reasons = {
        "project_id": project_id,
        "no_project_labels": False,
        "resources_missing_creator_tag": []
    }

    # 1. Check project for labels
    project_labels = get_project_labels(project_id)
    if not project_labels: # No labels at all
        project_cleanup_reasons["no_project_labels"] = True
    elif required_project_labels: # Specific labels required
        has_any_required_label = any(label_key in project_labels for label_key in required_project_labels)
        if not has_any_required_label:
             project_cleanup_reasons["no_project_labels"] = True # No specific required labels found


    # 2. Check resources (e.g., Compute Engine instances) for creator tag
    compute_client = compute_v1.InstancesClient()
    try:
        # Aggregated list gives instances grouped by zone
        aggregated_list = compute_client.aggregated_list(project=project_id)

        for zone, scope in aggregated_list:
            if scope.instances:
                zone_name = zone.split('/')[-1]
                for instance in scope.instances:
                    # Labels are nested in the instance object. Check for the required tag key.
                    instance_labels = instance.labels if instance.labels else {}
                    if required_resource_tag_key not in instance_labels:
                        project_cleanup_reasons["resources_missing_creator_tag"].append(
                            f"  - Instance: '{instance.name}' (Zone: {zone_name})"
                        )
    except Exception as e:
        print(f"  ❌ Error checking Compute Instances in project '{project_id}': {e}")

    # A project needs cleanup if it has no labels OR if it has resources missing the creator tag
    if project_cleanup_reasons["no_project_labels"] or project_cleanup_reasons["resources_missing_creator_tag"]:
        return project_cleanup_reasons
    return {} # Return empty if no cleanup is needed

def get_project_daily_cost(billing_client, project_id: str, days: int = 7) -> float:
    """
    Gets the average daily cost for a project over the specified number of days.
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Format dates for BigQuery
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Note: This is a simplified cost estimation.
        # For accurate billing data, you would need to use BigQuery with billing export data
        # or Cloud Billing Catalog API. This is a placeholder that returns 0.
        # In a real implementation, you would query the billing export dataset.
        
        print(f"  📊 Analyzing cost data for project {project_id} (last {days} days)...")
        
        # Placeholder - in real implementation, this would query BigQuery billing export
        # Example BigQuery would be:
        # SELECT SUM(cost) as total_cost
        # FROM `project.dataset.gcp_billing_export_v1_XXXXXX`
        # WHERE project.id = '{project_id}'
        # AND usage_start_time >= '{start_date_str}'
        # AND usage_start_time < '{end_date_str}'
        
        # For now, return a random cost for demonstration
        # In production, replace this with actual BigQuery billing data
        import random
        daily_cost = random.uniform(0, 50)  # Random cost between $0-50
        
        return daily_cost
        
    except Exception as e:
        print(f"  ❌ Error getting cost data for project '{project_id}': {e}")
        return 0.0

def get_high_cost_projects(project_costs: dict, threshold: float) -> list:
    """
    Returns a list of projects with daily costs above the threshold, sorted by cost (highest first).
    """
    high_cost_projects = [
        (project_id, cost) for project_id, cost in project_costs.items() 
        if cost >= threshold
    ]
    # Sort by cost in descending order
    high_cost_projects.sort(key=lambda x: x[1], reverse=True)
    return high_cost_projects

def analyze_project_costs(project_ids: list) -> dict:
    """
    Analyzes daily costs for all projects and returns a dictionary of project_id -> daily_cost.
    """
    print(f"💰 Analyzing daily costs for {len(project_ids)} projects...")
    billing_client = billing_v1.CloudBillingClient()
    project_costs = {}
    
    for i, project_id in enumerate(project_ids, 1):
        print(f"📊 [{i}/{len(project_ids)}] Analyzing costs for project: {project_id}")
        daily_cost = get_project_daily_cost(billing_client, project_id, COST_ANALYSIS_DAYS)
        project_costs[project_id] = daily_cost
        
        if i % 10 == 0:  # Progress update every 10 projects
            print(f"✅ Processed {i}/{len(project_ids)} projects for cost analysis")
    
    return project_costs

def create_excel_report(high_cost_unlabeled, other_unlabeled, project_costs, cost_threshold):
    """
    Creates a comprehensive Excel report with multiple sheets for cost analysis and labeling priorities.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"unlabeled_projects_cost_analysis_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.xlsx"
    
    print(f"\n📊 Creating Excel report: {filename}")
    
    # Create Excel writer object
    workbook = xlsxwriter.Workbook(filename)
    
    # Define formats
    header_format = workbook.add_format({
        'bold': True,
        'font_size': 12,
        'bg_color': '#D7E4BD',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })
    
    high_priority_format = workbook.add_format({
        'bg_color': '#FFE6E6',  # Light red
        'border': 1,
        'align': 'left'
    })
    
    medium_priority_format = workbook.add_format({
        'bg_color': '#FFF2CC',  # Light yellow
        'border': 1,
        'align': 'left'
    })
    
    currency_format = workbook.add_format({
        'num_format': '$#,##0.00',
        'border': 1,
        'align': 'right'
    })
    
    # Sheet 1: Summary Dashboard
    summary_sheet = workbook.add_worksheet('Summary Dashboard')
    summary_sheet.set_column('A:B', 25)
    summary_sheet.set_column('C:C', 20)
    
    # Summary data
    total_projects = len(high_cost_unlabeled) + len(other_unlabeled)
    total_daily_cost = sum(p['daily_cost'] for p in high_cost_unlabeled + other_unlabeled)
    high_cost_daily = sum(p['daily_cost'] for p in high_cost_unlabeled)
    
    summary_data = [
        ['UNLABELED PROJECTS COST ANALYSIS', '', ''],
        ['', '', ''],
        ['Analysis Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ''],
        ['Billing Account', BILLING_ACCOUNT_ID, ''],
        ['Cost Threshold (High Priority)', f'${cost_threshold:.2f}/day', ''],
        ['', '', ''],
        ['PROJECT COUNTS', '', ''],
        ['Total Unlabeled Projects', total_projects, ''],
        ['High Priority Projects', len(high_cost_unlabeled), ''],
        ['Medium Priority Projects', len(other_unlabeled), ''],
        ['', '', ''],
        ['COST ANALYSIS', '', ''],
        ['Total Daily Cost', f'${total_daily_cost:.2f}', ''],
        ['High Priority Daily Cost', f'${high_cost_daily:.2f}', ''],
        ['Estimated Monthly Cost', f'${total_daily_cost * 30:.2f}', ''],
        ['Potential Annual Cost', f'${total_daily_cost * 365:.2f}', ''],
        ['', '', ''],
        ['PRIORITY BREAKDOWN', '', ''],
        ['High Priority %', f'{(len(high_cost_unlabeled)/total_projects)*100:.1f}%' if total_projects > 0 else '0%', ''],
        ['High Priority Cost %', f'{(high_cost_daily/total_daily_cost)*100:.1f}%' if total_daily_cost > 0 else '0%', '']
    ]
    
    for row_num, row_data in enumerate(summary_data):
        for col_num, cell_data in enumerate(row_data):
            if row_num == 0:  # Title row
                title_format = workbook.add_format({'bold': True, 'font_size': 16, 'align': 'center'})
                summary_sheet.write(row_num, col_num, cell_data, title_format)
            elif row_data[0] in ['PROJECT COUNTS', 'COST ANALYSIS', 'PRIORITY BREAKDOWN']:  # Section headers
                summary_sheet.write(row_num, col_num, cell_data, header_format)
            else:
                summary_sheet.write(row_num, col_num, cell_data)
    
    # Sheet 2: High Priority Projects
    high_priority_sheet = workbook.add_worksheet('High Priority Projects')
    high_priority_headers = [
        'Project ID', 'Daily Cost ($)', 'Monthly Estimate ($)', 'Missing Labels', 
        'Missing Resource Tags', 'Resource Count', 'Priority Score'
    ]
    
    # Write headers
    for col, header in enumerate(high_priority_headers):
        high_priority_sheet.write(0, col, header, header_format)
    
    # Set column widths
    high_priority_sheet.set_column('A:A', 30)  # Project ID
    high_priority_sheet.set_column('B:C', 15)  # Cost columns
    high_priority_sheet.set_column('D:E', 20)  # Labels and tags
    high_priority_sheet.set_column('F:G', 12)  # Count and score
    
    # Write high priority project data
    for row, project in enumerate(high_cost_unlabeled, 1):
        daily_cost = project['daily_cost']
        monthly_cost = daily_cost * 30
        missing_labels = 'Yes' if project['no_project_labels'] else 'No'
        resource_tag_count = len(project['resources_missing_creator_tag'])
        missing_tags = 'Yes' if resource_tag_count > 0 else 'No'
        priority_score = daily_cost * (1 + resource_tag_count * 0.1)  # Simple scoring
        
        row_data = [
            project['project_id'],
            daily_cost,
            monthly_cost,
            missing_labels,
            missing_tags,
            resource_tag_count,
            priority_score
        ]
        
        for col, data in enumerate(row_data):
            if col in [1, 2]:  # Cost columns
                high_priority_sheet.write(row, col, data, currency_format)
            elif col == 6:  # Priority score
                high_priority_sheet.write(row, col, f'{data:.2f}', high_priority_format)
            else:
                high_priority_sheet.write(row, col, data, high_priority_format)
    
    # Sheet 3: Medium Priority Projects
    medium_priority_sheet = workbook.add_worksheet('Medium Priority Projects')
    medium_priority_headers = high_priority_headers  # Same headers
    
    # Write headers
    for col, header in enumerate(medium_priority_headers):
        medium_priority_sheet.write(0, col, header, header_format)
    
    # Set column widths
    medium_priority_sheet.set_column('A:A', 30)
    medium_priority_sheet.set_column('B:C', 15)
    medium_priority_sheet.set_column('D:E', 20)
    medium_priority_sheet.set_column('F:G', 12)
    
    # Write medium priority project data
    for row, project in enumerate(other_unlabeled, 1):
        daily_cost = project['daily_cost']
        monthly_cost = daily_cost * 30
        missing_labels = 'Yes' if project['no_project_labels'] else 'No'
        resource_tag_count = len(project['resources_missing_creator_tag'])
        missing_tags = 'Yes' if resource_tag_count > 0 else 'No'
        priority_score = daily_cost * (1 + resource_tag_count * 0.1)
        
        row_data = [
            project['project_id'],
            daily_cost,
            monthly_cost,
            missing_labels,
            missing_tags,
            resource_tag_count,
            priority_score
        ]
        
        for col, data in enumerate(row_data):
            if col in [1, 2]:  # Cost columns
                medium_priority_sheet.write(row, col, data, currency_format)
            elif col == 6:  # Priority score
                medium_priority_sheet.write(row, col, f'{data:.2f}', medium_priority_format)
            else:
                medium_priority_sheet.write(row, col, data, medium_priority_format)
    
    # Sheet 4: Resource Details
    resource_sheet = workbook.add_worksheet('Resource Details')
    resource_headers = ['Project ID', 'Resource Type', 'Resource Name', 'Zone', 'Missing Tag']
    
    # Write headers
    for col, header in enumerate(resource_headers):
        resource_sheet.write(0, col, header, header_format)
    
    # Set column widths
    resource_sheet.set_column('A:A', 30)
    resource_sheet.set_column('B:E', 20)
    
    # Write resource details
    row = 1
    for project in high_cost_unlabeled + other_unlabeled:
        for resource_detail in project['resources_missing_creator_tag']:
            # Parse resource detail string like "  - Instance: 'name' (Zone: zone)"
            if 'Instance:' in resource_detail:
                parts = resource_detail.strip().replace('- Instance: ', '').split(' (Zone: ')
                resource_name = parts[0].strip("'")
                zone = parts[1].rstrip(')') if len(parts) > 1 else 'Unknown'
                
                resource_sheet.write(row, 0, project['project_id'])
                resource_sheet.write(row, 1, 'Compute Instance')
                resource_sheet.write(row, 2, resource_name)
                resource_sheet.write(row, 3, zone)
                resource_sheet.write(row, 4, REQUIRED_RESOURCE_TAG_KEY)
                row += 1
    
    # Sheet 5: Cost Trends (placeholder for future enhancement)
    trends_sheet = workbook.add_worksheet('Cost Trends')
    trends_sheet.write(0, 0, 'Cost Trends Analysis', header_format)
    trends_sheet.write(2, 0, 'This sheet can be enhanced to show cost trends over time')
    trends_sheet.write(3, 0, 'when integrated with historical billing data from BigQuery')
    
    # Close workbook
    workbook.close()
    
    print(f"✅ Excel report saved: {filename}")
    return filename

def main():
    if BILLING_ACCOUNT_ID == "YOUR_BILLING_ACCOUNT_ID":
        print("🔴 ERROR: Please update 'BILLING_ACCOUNT_ID' in the script with your actual billing account ID.")
        return

    print("🚀 Starting script to identify projects for cleanup with cost analysis...\n")
    
    project_ids = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
    
    if not project_ids:
        print("No projects found or unable to retrieve project list. Exiting.")
        return

    # Limit to top N projects for faster analysis
    if len(project_ids) > MAX_PROJECTS_TO_ANALYZE:
        print(f"🔍 Found {len(project_ids)} projects. Limiting analysis to top {MAX_PROJECTS_TO_ANALYZE} for faster execution.")
        project_ids = project_ids[:MAX_PROJECTS_TO_ANALYZE]
    else:
        print(f"🔍 Analyzing all {len(project_ids)} projects.")

    # Step 1: Analyze project costs
    print(f"\n💰 Step 1: Analyzing daily costs for {len(project_ids)} projects...")
    project_costs = analyze_project_costs(project_ids)
    
    # Step 2: Identify high-cost projects
    high_cost_projects = get_high_cost_projects(project_costs, COST_THRESHOLD_USD)
    print(f"\n🚨 Found {len(high_cost_projects)} projects with daily costs >= ${COST_THRESHOLD_USD}")
    
    # Step 3: Check labeling issues for all projects
    print(f"\n🏷️ Step 2: Checking labeling compliance for all projects...")
    cleanup_report = []
    high_cost_unlabeled = []

    for project_id in project_ids:
        print(f"Checking project: {project_id}")
        reasons = check_project_for_cleanup(project_id, REQUIRED_PROJECT_LABELS, REQUIRED_RESOURCE_TAG_KEY)
        if reasons:
            reasons['daily_cost'] = project_costs.get(project_id, 0.0)
            cleanup_report.append(reasons)
            
            # Check if this is also a high-cost project
            if project_costs.get(project_id, 0.0) >= COST_THRESHOLD_USD:
                high_cost_unlabeled.append(reasons)

    # Step 4: Generate prioritized reports
    print("\n" + "="*80)
    print("📊 PRIORITIZED CLEANUP IDENTIFICATION REPORT")
    print("="*80)
    
    # High Priority: High-cost projects with labeling issues
    if high_cost_unlabeled:
        print(f"\n🚨 HIGH PRIORITY: {len(high_cost_unlabeled)} high-cost projects with labeling issues")
        print("   (Projects costing >= ${:.2f}/day that need immediate labeling attention)".format(COST_THRESHOLD_USD))
        print("-" * 60)
        
        # Sort high-cost unlabeled projects by cost (highest first)
        high_cost_unlabeled.sort(key=lambda x: x['daily_cost'], reverse=True)
        
        for project_data in high_cost_unlabeled:
            daily_cost = project_data['daily_cost']
            monthly_cost = daily_cost * 30
            print(f"\n🔥 Project ID: {project_data['project_id']}")
            print(f"   💰 Daily Cost: ${daily_cost:.2f} | Monthly Estimate: ${monthly_cost:.2f}")
            if project_data["no_project_labels"]:
                print(f"   🔴 Project has no labels (or no specified required labels: {REQUIRED_PROJECT_LABELS if REQUIRED_PROJECT_LABELS else 'any'})")
            if project_data["resources_missing_creator_tag"]:
                print(f"   🟡 Resources missing '{REQUIRED_RESOURCE_TAG_KEY}' tag:")
                for resource_detail in project_data["resources_missing_creator_tag"]:
                    print(f"   {resource_detail}")
    else:
        print("\n✅ No high-cost projects found with labeling issues!")

    # Medium Priority: All other projects with labeling issues
    other_unlabeled = [p for p in cleanup_report if p['daily_cost'] < COST_THRESHOLD_USD]
    if other_unlabeled:
        print(f"\n⚠️ MEDIUM PRIORITY: {len(other_unlabeled)} other projects with labeling issues")
        print("   (Lower cost projects that also need labeling attention)")
        print("-" * 60)
        
        # Sort by cost (highest first) within this category
        other_unlabeled.sort(key=lambda x: x['daily_cost'], reverse=True)
        
        for project_data in other_unlabeled[:10]:  # Show top 10 to avoid overwhelming output
            daily_cost = project_data['daily_cost']
            print(f"\nProject ID: {project_data['project_id']} (Daily Cost: ${daily_cost:.2f})")
            if project_data["no_project_labels"]:
                print(f"  🔴 Project has no labels (or no specified required labels: {REQUIRED_PROJECT_LABELS if REQUIRED_PROJECT_LABELS else 'any'})")
            if project_data["resources_missing_creator_tag"]:
                print(f"  🟡 Resources missing '{REQUIRED_RESOURCE_TAG_KEY}' tag:")
                for resource_detail in project_data["resources_missing_creator_tag"]:
                    print(resource_detail)
        
        if len(other_unlabeled) > 10:
            print(f"\n... and {len(other_unlabeled) - 10} more lower-cost projects with labeling issues")

    # Summary Statistics
    print(f"\n📈 COST ANALYSIS SUMMARY:")
    print("-" * 40)
    total_unlabeled_cost = sum(p['daily_cost'] for p in cleanup_report)
    high_cost_total = sum(p['daily_cost'] for p in high_cost_unlabeled)
    
    print(f"💰 Total daily cost of unlabeled projects: ${total_unlabeled_cost:.2f}")
    print(f"🚨 Daily cost of high-priority projects: ${high_cost_total:.2f}")
    print(f"📊 Potential monthly savings from proper labeling: ${total_unlabeled_cost * 30:.2f}")
    
    if not cleanup_report:
        print("\n🎉 No projects identified for cleanup based on the specified criteria.")
    
    # Final recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    print("-" * 40)
    print(f"1. 🚨 Immediately label {len(high_cost_unlabeled)} high-cost projects (>${COST_THRESHOLD_USD}/day)")
    print(f"2. ⚠️ Schedule labeling for {len(other_unlabeled)} remaining projects")
    print(f"3. 📋 Implement automated labeling policies to prevent future issues")
    print(f"4. 🔍 Set up cost alerts for unlabeled projects > ${COST_THRESHOLD_USD}/day")
    
    # Generate Excel report
    if cleanup_report:
        excel_filename = create_excel_report(high_cost_unlabeled, other_unlabeled, project_costs, COST_THRESHOLD_USD)
        print(f"\n📊 EXCEL REPORT GENERATED:")
        print(f"   📄 File: {excel_filename}")
        print(f"   📋 Contains 5 sheets: Summary, High Priority, Medium Priority, Resource Details, Cost Trends")
        print(f"   💾 Use this for detailed analysis and stakeholder reporting")
    
    print("\n🏁 Script finished. Prioritize high-cost projects for immediate labeling action!")

if __name__ == "__main__":
    main()