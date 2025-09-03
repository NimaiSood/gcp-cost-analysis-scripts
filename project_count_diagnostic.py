#!/usr/bin/env python3
"""
Diagnostic script to compare project counts between billing account projects 
and total accessible projects through Resource Manager API.

Author: Nimai Sood
Date: September 3, 2025
"""

import logging
import time
from google.cloud import billing_v1
from google.cloud import resourcemanager_v3
from google.api_core.exceptions import GoogleAPIError

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_billing_account_projects():
    """Get projects linked to the billing account."""
    logger.info(f"🔍 Getting projects from billing account: {BILLING_ACCOUNT_ID}")
    
    try:
        client = billing_v1.CloudBillingClient()
        billing_account_name = f"billingAccounts/{BILLING_ACCOUNT_ID}"
        
        request = billing_v1.ListProjectBillingInfoRequest(name=billing_account_name)
        page_result = client.list_project_billing_info(request=request)
        
        billing_projects = set()
        billing_project_details = []
        
        for project_billing_info in page_result:
            if project_billing_info.billing_enabled:
                project_name_parts = project_billing_info.name.split('/')
                if len(project_name_parts) >= 3:
                    project_id = project_name_parts[1]
                    billing_projects.add(project_id)
                    billing_project_details.append({
                        'project_id': project_id,
                        'billing_enabled': project_billing_info.billing_enabled,
                        'billing_account_name': project_billing_info.billing_account_name
                    })
        
        logger.info(f"✅ Found {len(billing_projects)} projects in billing account")
        return billing_projects, billing_project_details
        
    except Exception as e:
        logger.error(f"❌ Error getting billing projects: {str(e)}")
        return set(), []

def get_resource_manager_projects():
    """Get all projects accessible through Resource Manager API."""
    logger.info("🔍 Getting projects from Resource Manager API")
    
    try:
        client = resourcemanager_v3.ProjectsClient()
        request = resourcemanager_v3.SearchProjectsRequest()
        
        resource_projects = set()
        resource_project_details = []
        
        project_count = 0
        for project in client.search_projects(request=request):
            if project.state == resourcemanager_v3.Project.State.ACTIVE:
                resource_projects.add(project.project_id)
                resource_project_details.append({
                    'project_id': project.project_id,
                    'display_name': project.display_name,
                    'state': project.state.name,
                    'create_time': project.create_time.strftime('%Y-%m-%d %H:%M:%S') if project.create_time else 'Unknown'
                })
                project_count += 1
                
                # Progress update
                if project_count % 1000 == 0:
                    logger.info(f"📊 Processed {project_count} projects...")
        
        logger.info(f"✅ Found {len(resource_projects)} active projects via Resource Manager")
        return resource_projects, resource_project_details
        
    except Exception as e:
        logger.error(f"❌ Error getting Resource Manager projects: {str(e)}")
        return set(), []

def analyze_project_differences(billing_projects, resource_projects, billing_details, resource_details):
    """Analyze the differences between the two sets of projects."""
    logger.info("\n" + "="*80)
    logger.info("📊 PROJECT COMPARISON ANALYSIS")
    logger.info("="*80)
    
    # Basic statistics
    logger.info(f"📋 Billing Account Projects: {len(billing_projects)}")
    logger.info(f"📋 Resource Manager Projects: {len(resource_projects)}")
    logger.info(f"📈 Difference: {len(resource_projects) - len(billing_projects)}")
    
    # Find projects only in billing account (shouldn't happen normally)
    only_in_billing = billing_projects - resource_projects
    logger.info(f"🔶 Projects only in billing account: {len(only_in_billing)}")
    if only_in_billing and len(only_in_billing) <= 10:
        for project in list(only_in_billing)[:10]:
            logger.info(f"   - {project}")
    
    # Find projects only in Resource Manager (not linked to billing)
    only_in_resource = resource_projects - billing_projects
    logger.info(f"🔷 Projects only in Resource Manager: {len(only_in_resource)}")
    if only_in_resource:
        logger.info(f"   (First 10 examples:)")
        for project in list(only_in_resource)[:10]:
            logger.info(f"   - {project}")
    
    # Find common projects
    common_projects = billing_projects & resource_projects
    logger.info(f"🔗 Projects in both: {len(common_projects)}")
    
    # Sample analysis of projects only in Resource Manager
    if only_in_resource:
        logger.info(f"\n📝 Sample projects not linked to billing account:")
        sample_projects = list(only_in_resource)[:5]
        for project_id in sample_projects:
            # Find details for this project
            project_detail = next((p for p in resource_details if p['project_id'] == project_id), None)
            if project_detail:
                logger.info(f"   🔸 {project_id}")
                logger.info(f"      Display Name: {project_detail['display_name']}")
                logger.info(f"      State: {project_detail['state']}")
                logger.info(f"      Created: {project_detail['create_time']}")
    
    return {
        'billing_count': len(billing_projects),
        'resource_count': len(resource_projects),
        'only_in_billing': len(only_in_billing),
        'only_in_resource': len(only_in_resource),
        'common': len(common_projects)
    }

def main():
    """Main function to run the diagnostic."""
    start_time = time.time()
    
    logger.info("🚀 Starting project count diagnostic...")
    logger.info("="*80)
    
    # Get projects from both sources
    billing_projects, billing_details = get_billing_account_projects()
    resource_projects, resource_details = get_resource_manager_projects()
    
    if not billing_projects and not resource_projects:
        logger.error("❌ Failed to get projects from both sources")
        return
    
    # Analyze differences
    analysis = analyze_project_differences(billing_projects, resource_projects, billing_details, resource_details)
    
    # Summary
    execution_time = time.time() - start_time
    logger.info(f"\n🏁 Analysis complete! Execution time: {execution_time:.2f} seconds")
    
    logger.info("\n📊 SUMMARY:")
    logger.info(f"   • Billing Account: {analysis['billing_count']} projects")
    logger.info(f"   • Resource Manager: {analysis['resource_count']} projects")
    logger.info(f"   • Difference: {analysis['resource_count'] - analysis['billing_count']} additional projects via Resource Manager")
    logger.info(f"   • Common projects: {analysis['common']}")
    logger.info(f"   • Projects without billing: {analysis['only_in_resource']}")

if __name__ == "__main__":
    main()
