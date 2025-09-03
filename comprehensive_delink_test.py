#!/usr/bin/env python3
"""
Enhanced test script to demonstrate the complete workflow of identifying and delinking 
projects with no labels from a billing account.

This script shows the integration between label checking and delinking functionality.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
import logging
import time
import subprocess
import json
from datetime import datetime
from google.cloud import resourcemanager_v3
from google.cloud import billing_v1
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import (
    GoogleAPIError, 
    RetryError, 
    DeadlineExceeded, 
    PermissionDenied,
    NotFound,
    ServiceUnavailable
)
import pandas as pd
from functools import wraps

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
MAX_PROJECTS = 5  # Small number for testing
MAX_RETRIES = 3
RETRY_DELAY = 2

# Test settings
DRY_RUN = True  # ALWAYS True for safety in this test
INTERACTIVE_MODE = False  # Set to False to skip confirmations for automated testing

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'comprehensive_delink_test_{BILLING_ACCOUNT_ID.replace("-", "_")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def check_prerequisites():
    """Check if required tools and permissions are available."""
    logger.info("🔍 Checking prerequisites...")
    
    # Check gcloud CLI
    try:
        result = subprocess.run(['gcloud', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            logger.info("✅ gcloud CLI is available")
        else:
            logger.error("❌ gcloud CLI not available")
            return False
    except Exception as e:
        logger.error(f"❌ Error checking gcloud CLI: {str(e)}")
        return False
    
    # Check authentication
    try:
        result = subprocess.run(['gcloud', 'auth', 'list', '--filter=status:ACTIVE', '--format=value(account)'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            active_account = result.stdout.strip().split('\n')[0]
            logger.info(f"✅ Authenticated as: {active_account}")
        else:
            logger.error("❌ No active gcloud authentication found")
            return False
    except Exception as e:
        logger.error(f"❌ Error checking authentication: {str(e)}")
        return False
    
    return True

def get_project_labels_gcloud(project_id):
    """Get project labels using gcloud CLI for comparison."""
    try:
        cmd = ['gcloud', 'projects', 'describe', project_id, '--format=json']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return None
        
        project_data = json.loads(result.stdout)
        labels = project_data.get('labels', {})
        
        return {
            'has_labels': len(labels) > 0,
            'labels': labels,
            'label_count': len(labels),
            'method': 'gcloud_cli'
        }
        
    except Exception as e:
        logger.warning(f"Error getting labels for {project_id} via gcloud: {str(e)}")
        return None

def get_project_labels_api(project_id):
    """Get project labels using Google Cloud API for comparison."""
    try:
        client = resourcemanager_v3.ProjectsClient()
        project_name = f"projects/{project_id}"
        request = resourcemanager_v3.GetProjectRequest(name=project_name)
        project = client.get_project(request=request)
        
        labels = dict(project.labels) if project.labels else {}
        
        return {
            'has_labels': len(labels) > 0,
            'labels': labels,
            'label_count': len(labels),
            'method': 'google_api',
            'project_details': {
                'project_id': project.project_id,
                'display_name': project.display_name or project.project_id,
                'state': project.state.name,
                'create_time': project.create_time.strftime('%Y-%m-%d %H:%M:%S') if project.create_time else 'Unknown'
            }
        }
        
    except Exception as e:
        logger.warning(f"Error getting labels for {project_id} via API: {str(e)}")
        return None

def check_project_resources(project_id):
    """Check what resources a project has before delinking."""
    resources = {
        'compute_instances': 0,
        'compute_disks': 0,
        'storage_buckets': 0,
        'sql_instances': 0,
        'gke_clusters': 0,
        'has_resources': False,
        'resource_details': []
    }
    
    try:
        # Check compute instances
        cmd = ['gcloud', 'compute', 'instances', 'list', '--project', project_id, '--format=json']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            instances = json.loads(result.stdout)
            resources['compute_instances'] = len(instances)
            if instances:
                for instance in instances:
                    resources['resource_details'].append(f"Compute VM: {instance.get('name', 'unknown')}")
        
        # Check persistent disks
        cmd = ['gcloud', 'compute', 'disks', 'list', '--project', project_id, '--format=json']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            disks = json.loads(result.stdout)
            resources['compute_disks'] = len(disks)
            if disks:
                for disk in disks:
                    resources['resource_details'].append(f"Compute Disk: {disk.get('name', 'unknown')}")
        
        # Check storage buckets
        cmd = ['gsutil', 'ls', '-p', project_id]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            buckets = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            resources['storage_buckets'] = len(buckets)
            if buckets:
                for bucket in buckets:
                    resources['resource_details'].append(f"Storage Bucket: {bucket}")
        
        # Check SQL instances
        cmd = ['gcloud', 'sql', 'instances', 'list', '--project', project_id, '--format=json']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            sql_instances = json.loads(result.stdout)
            resources['sql_instances'] = len(sql_instances)
            if sql_instances:
                for instance in sql_instances:
                    resources['resource_details'].append(f"SQL Instance: {instance.get('name', 'unknown')}")
        
        # Check GKE clusters
        cmd = ['gcloud', 'container', 'clusters', 'list', '--project', project_id, '--format=json']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            clusters = json.loads(result.stdout)
            resources['gke_clusters'] = len(clusters)
            if clusters:
                for cluster in clusters:
                    resources['resource_details'].append(f"GKE Cluster: {cluster.get('name', 'unknown')}")
        
        # Determine if project has significant resources
        resources['has_resources'] = (
            resources['compute_instances'] > 0 or 
            resources['sql_instances'] > 0 or 
            resources['gke_clusters'] > 0 or
            resources['storage_buckets'] > 0
        )
        
    except Exception as e:
        logger.warning(f"Error checking resources for {project_id}: {str(e)}")
    
    return resources

def simulate_delink_operation(project_id, project_info, resources):
    """Simulate the delink operation with detailed analysis."""
    
    logger.info(f"🔄 Simulating delink operation for project: {project_id}")
    logger.info(f"📊 Project Analysis:")
    logger.info(f"   • Display Name: {project_info.get('project_details', {}).get('display_name', project_id)}")
    logger.info(f"   • State: {project_info.get('project_details', {}).get('state', 'UNKNOWN')}")
    logger.info(f"   • Created: {project_info.get('project_details', {}).get('create_time', 'Unknown')}")
    logger.info(f"   • Labels: {project_info.get('label_count', 0)}")
    
    if project_info.get('labels'):
        logger.info(f"   • Label keys: {', '.join(project_info['labels'].keys())}")
    
    logger.info(f"📋 Resource Analysis:")
    logger.info(f"   • Compute Instances: {resources['compute_instances']}")
    logger.info(f"   • Compute Disks: {resources['compute_disks']}")
    logger.info(f"   • Storage Buckets: {resources['storage_buckets']}")
    logger.info(f"   • SQL Instances: {resources['sql_instances']}")
    logger.info(f"   • GKE Clusters: {resources['gke_clusters']}")
    logger.info(f"   • Has Significant Resources: {resources['has_resources']}")
    
    if resources['resource_details']:
        logger.info(f"   • Resource Details:")
        for detail in resources['resource_details'][:5]:  # Show first 5
            logger.info(f"     - {detail}")
        if len(resources['resource_details']) > 5:
            logger.info(f"     - ... and {len(resources['resource_details']) - 5} more resources")
    
    # Risk assessment
    risk_level = "LOW"
    risk_factors = []
    
    if resources['has_resources']:
        risk_level = "HIGH"
        risk_factors.append("Project has active billable resources")
    
    if project_info.get('project_details', {}).get('state') != 'ACTIVE':
        risk_factors.append(f"Project state is {project_info.get('project_details', {}).get('state', 'UNKNOWN')}")
    
    if not project_info.get('has_labels'):
        risk_factors.append("Project has no governance labels")
    
    logger.info(f"⚠️ Risk Assessment: {risk_level}")
    for factor in risk_factors:
        logger.info(f"   • {factor}")
    
    # Simulate the actual delink operation
    try:
        logger.info(f"🔄 DRY RUN: Simulating billing delink for {project_id}")
        
        # This is what would happen in a real delink:
        # 1. Initialize billing client
        # 2. Update project billing info to disable billing
        # 3. Verify the operation succeeded
        
        time.sleep(1)  # Simulate API call time
        
        return {
            'success': True,
            'message': f"DRY RUN: Would successfully delink project {project_id} from billing account {BILLING_ACCOUNT_ID}",
            'dry_run': True,
            'risk_level': risk_level,
            'risk_factors': risk_factors
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"DRY RUN: Failed to delink project {project_id}: {str(e)}",
            'dry_run': True,
            'error': str(e)
        }

def main():
    """Main function to demonstrate comprehensive delink testing."""
    
    start_time = time.time()
    
    logger.info("🚀 Starting comprehensive delink test for unlabeled projects...")
    logger.info(f"💳 Target billing account: {BILLING_ACCOUNT_ID}")
    logger.info(f"📊 Testing with up to {MAX_PROJECTS} projects")
    logger.info(f"🔒 DRY RUN MODE: ALWAYS ENABLED (for safety)")
    logger.info(f"🤖 Interactive Mode: {'ENABLED' if INTERACTIVE_MODE else 'DISABLED (automated testing)'}")
    logger.info("="*100)
    
    # Check prerequisites
    if not check_prerequisites():
        logger.error("❌ Prerequisites not met. Exiting.")
        return
    
    # Get projects from billing account (using the same logic as our previous scripts)
    logger.info("🔍 Retrieving projects from billing account...")
    
    try:
        client = billing_v1.CloudBillingClient()
        billing_account_name = f"billingAccounts/{BILLING_ACCOUNT_ID}"
        request = billing_v1.ListProjectBillingInfoRequest(name=billing_account_name)
        
        project_ids = []
        page_result = client.list_project_billing_info(request=request)
        
        for project_billing_info in page_result:
            try:
                if project_billing_info.billing_enabled:
                    project_name_parts = project_billing_info.name.split('/')
                    if len(project_name_parts) >= 3:
                        project_id = project_name_parts[1]
                        project_ids.append(project_id)
                        
            except Exception as e:
                logger.warning(f"Error processing billing project: {str(e)}")
                continue
        
        logger.info(f"✅ Found {len(project_ids)} projects linked to billing account")
        test_projects = project_ids[:MAX_PROJECTS]
        logger.info(f"📊 Testing with first {len(test_projects)} projects")
        
    except Exception as e:
        logger.error(f"Failed to retrieve billing account projects: {str(e)}")
        return
    
    # Track results
    results = []
    unlabeled_count = 0
    labeled_count = 0
    error_count = 0
    delink_candidates = []
    
    logger.info(f"\n🔍 Analyzing {len(test_projects)} projects...")
    logger.info("="*100)
    
    # Analyze each project
    for i, project_id in enumerate(test_projects, 1):
        logger.info(f"\n📊 [{i}/{len(test_projects)}] Analyzing project: {project_id}")
        logger.info("-" * 80)
        
        try:
            # Get project labels using both methods for comparison
            api_labels = get_project_labels_api(project_id)
            gcloud_labels = get_project_labels_gcloud(project_id)
            
            # Use API results as primary, gcloud as verification
            if api_labels:
                project_info = api_labels
                
                # Compare methods if both successful
                if gcloud_labels:
                    if api_labels['label_count'] != gcloud_labels['label_count']:
                        logger.warning(f"⚠️ Label count mismatch: API={api_labels['label_count']}, gcloud={gcloud_labels['label_count']}")
                    else:
                        logger.info(f"✅ Label verification: Both methods agree on {api_labels['label_count']} labels")
                
            elif gcloud_labels:
                project_info = gcloud_labels
                logger.warning(f"⚠️ Using gcloud fallback for {project_id}")
            else:
                logger.error(f"❌ Could not retrieve project information for {project_id}")
                error_count += 1
                continue
            
            # Check if project has labels
            if project_info['has_labels']:
                labeled_count += 1
                logger.info(f"✅ Project has {project_info['label_count']} labels - skipping delink")
                logger.info(f"🏷️ Label keys: {', '.join(project_info['labels'].keys())}")
                
                results.append({
                    'project_id': project_id,
                    'has_labels': True,
                    'label_count': project_info['label_count'],
                    'delink_candidate': False,
                    'analysis_result': 'Skipped - has labels'
                })
                continue
            
            # Project has no labels - candidate for delinking
            unlabeled_count += 1
            logger.info(f"❌ Project has NO labels - CANDIDATE FOR DELINKING")
            
            # Check project resources
            logger.info(f"🔍 Checking project resources...")
            resources = check_project_resources(project_id)
            
            # Simulate delink operation
            delink_result = simulate_delink_operation(project_id, project_info, resources)
            
            # Record results
            result_record = {
                'project_id': project_id,
                'display_name': project_info.get('project_details', {}).get('display_name', project_id),
                'state': project_info.get('project_details', {}).get('state', 'UNKNOWN'),
                'create_time': project_info.get('project_details', {}).get('create_time', 'Unknown'),
                'has_labels': False,
                'label_count': 0,
                'delink_candidate': True,
                'has_resources': resources['has_resources'],
                'compute_instances': resources['compute_instances'],
                'compute_disks': resources['compute_disks'],
                'storage_buckets': resources['storage_buckets'],
                'sql_instances': resources['sql_instances'],
                'gke_clusters': resources['gke_clusters'],
                'risk_level': delink_result.get('risk_level', 'UNKNOWN'),
                'delink_success': delink_result['success'],
                'delink_message': delink_result['message'],
                'analysis_result': 'Processed for delinking'
            }
            
            results.append(result_record)
            delink_candidates.append(result_record)
            
            if delink_result['success']:
                logger.info(f"✅ {delink_result['message']}")
            else:
                logger.error(f"❌ {delink_result['message']}")
            
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Error analyzing project {project_id}: {str(e)}")
            
            results.append({
                'project_id': project_id,
                'has_labels': None,
                'label_count': None,
                'delink_candidate': False,
                'analysis_result': f'Error: {str(e)}'
            })
    
    # Generate comprehensive summary
    logger.info("\n" + "="*100)
    logger.info("📋 COMPREHENSIVE DELINK TEST SUMMARY")
    logger.info("="*100)
    
    total_analyzed = len(test_projects)
    successful_delinks = len([r for r in delink_candidates if r.get('delink_success', False)])
    failed_delinks = len([r for r in delink_candidates if not r.get('delink_success', False)])
    
    logger.info(f"📊 ANALYSIS SUMMARY:")
    logger.info(f"💼 Total projects analyzed: {total_analyzed}")
    logger.info(f"✅ Projects with labels (safe): {labeled_count}")
    logger.info(f"❌ Projects without labels (candidates): {unlabeled_count}")
    logger.info(f"⚠️ Projects with errors: {error_count}")
    logger.info("")
    logger.info(f"🔄 DELINK SIMULATION SUMMARY:")
    logger.info(f"✅ Successful delink simulations: {successful_delinks}")
    logger.info(f"❌ Failed delink simulations: {failed_delinks}")
    logger.info(f"📈 Success rate: {(successful_delinks/max(unlabeled_count,1)*100):.1f}%")
    
    # Risk analysis
    if delink_candidates:
        high_risk = len([r for r in delink_candidates if r.get('risk_level') == 'HIGH'])
        low_risk = len([r for r in delink_candidates if r.get('risk_level') == 'LOW'])
        
        logger.info(f"\n⚠️ RISK ANALYSIS:")
        logger.info(f"🔴 High risk projects: {high_risk}")
        logger.info(f"🟢 Low risk projects: {low_risk}")
        
        logger.info(f"\n❌ PROJECTS IDENTIFIED FOR DELINKING:")
        for candidate in delink_candidates:
            risk_indicator = "🔴" if candidate.get('risk_level') == 'HIGH' else "🟢"
            resources_indicator = "📦" if candidate.get('has_resources') else "📭"
            logger.info(f"  {risk_indicator} {resources_indicator} {candidate['project_id']} - Created: {candidate.get('create_time', 'Unknown')}")
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if results:
        # Save to CSV
        df = pd.DataFrame(results)
        filename = f"comprehensive_delink_test_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
        df.to_csv(filename, index=False)
        file_size = os.path.getsize(filename)
        logger.info(f"\n💾 Detailed results saved to: {filename} ({file_size} bytes)")
        
        # Save delink candidates separately
        if delink_candidates:
            candidates_df = pd.DataFrame(delink_candidates)
            candidates_filename = f"delink_candidates_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
            candidates_df.to_csv(candidates_filename, index=False)
            candidates_file_size = os.path.getsize(candidates_filename)
            logger.info(f"💾 Delink candidates saved to: {candidates_filename} ({candidates_file_size} bytes)")
    
    # Calculate execution time
    execution_time = time.time() - start_time
    logger.info(f"\n🏁 Comprehensive delink test complete! Execution time: {execution_time:.2f} seconds")
    
    # Final recommendations
    logger.info(f"\n📋 RECOMMENDATIONS:")
    if unlabeled_count == 0:
        logger.info("✅ All tested projects have proper labels. No delinking needed.")
    else:
        logger.info(f"⚠️  Found {unlabeled_count} projects without labels that could be delinked.")
        logger.info("🔄 Next steps:")
        logger.info("   1. Review the generated CSV files for detailed analysis")
        logger.info("   2. Verify that unlabeled projects are indeed unused/abandoned")
        logger.info("   3. Check with project owners before proceeding")
        logger.info("   4. Consider implementing automated labeling policies")
        logger.info("   5. Use the actual delink script with proper safeguards")
    
    logger.info(f"\n🔒 SAFETY NOTE: This was a comprehensive DRY RUN test.")
    logger.info("   No actual delinking operations were performed.")
    logger.info("   All billing configurations remain unchanged.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Test interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error during test: {str(e)}")
        sys.exit(1)
