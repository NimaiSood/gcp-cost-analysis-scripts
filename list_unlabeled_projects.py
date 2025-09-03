#!/usr/bin/env python3
"""
Script to identify projects without proper labels for projects linked to a specific billing account.
This script helps with project governance by finding projects that lack any labeling
and are associated with the specified billing account for cost management.

Author: Nimai Sood
Date: September 3, 2025
"""

import os
import sys
import logging
import time
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
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"  # Focus only on projects linked to this billing account
MAX_PROJECTS = 50  # Limit to first 50 projects for analysis
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Optional: Recommended label keys - customize based on your organization's labeling standards
# Set to None to check for ANY labels, or specify recommended labels for reporting
RECOMMENDED_LABELS = [
    'environment',  # e.g., dev, test, staging, prod
    'team',         # owning team
    'cost-center',  # cost allocation
    'project-type'  # e.g., application, infrastructure, sandbox
]

# Analysis mode: 'any' to check for any labels, 'recommended' to check for specific labels
ANALYSIS_MODE = 'any'  # Change to 'recommended' to check for specific required labels

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'project_labeling_analysis_{BILLING_ACCOUNT_ID.replace("-", "_")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Custom exceptions
class ProjectAnalysisError(Exception):
    """Custom exception for project analysis errors"""
    pass

class ProjectAccessError(Exception):
    """Custom exception for project access errors"""
    pass

class BillingAccountError(Exception):
    """Custom exception for billing account errors"""
    pass

def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """
    Decorator to retry functions on failure with exponential backoff.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay (int): Initial delay between retries in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ServiceUnavailable, DeadlineExceeded, RetryError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
                        
            # Re-raise the last exception if all retries failed
            raise last_exception
            
        return wrapper
    return decorator

def validate_environment():
    """
    Validate the environment and check for required credentials.
    
    Raises:
        ProjectAnalysisError: If environment validation fails
    """
    logger.info("🔍 Validating environment...")
    
    # Check for Google Cloud credentials
    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') and not os.environ.get('GOOGLE_CLOUD_PROJECT'):
        logger.warning("⚠️ Google Cloud credentials not explicitly set. Using default credentials...")
    
    try:
        # Test basic Resource Manager access
        client = resourcemanager_v3.ProjectsClient()
        logger.info("✅ Resource Manager client initialized successfully")
        
        # Test billing client access
        billing_client = billing_v1.CloudBillingClient()
        logger.info("✅ Billing client initialized successfully")
        
    except Exception as e:
        raise ProjectAnalysisError(f"Failed to initialize Google Cloud clients: {str(e)}")
    
    logger.info("✅ Environment validation passed")

@retry_on_failure()
def get_billing_account_projects():
    """
    Get projects linked to the specific billing account.
    
    Returns:
        list: List of project IDs linked to the billing account
        
    Raises:
        BillingAccountError: If billing account operations fail
        ProjectAccessError: If project access operations fail
    """
    logger.info(f"🔍 Retrieving projects linked to billing account: {BILLING_ACCOUNT_ID}")
    
    try:
        # Initialize the Cloud Billing client
        client = billing_v1.CloudBillingClient()
        
        # Validate billing account exists and is accessible
        billing_account_name = f"billingAccounts/{BILLING_ACCOUNT_ID}"
        
        try:
            # Test access to the billing account
            billing_account = client.get_billing_account(name=billing_account_name)
            if not billing_account:
                raise NotFound(f"Billing account {BILLING_ACCOUNT_ID} not found")
        except NotFound:
            raise BillingAccountError(f"Billing account {BILLING_ACCOUNT_ID} not found or inaccessible")
        except PermissionDenied:
            raise BillingAccountError(f"Insufficient permissions to access billing account {BILLING_ACCOUNT_ID}")
        
        # Get all projects under the billing account
        request = billing_v1.ListProjectBillingInfoRequest(name=billing_account_name)
        
        project_ids = []
        page_result = client.list_project_billing_info(request=request)
        
        project_count = 0
        for project_billing_info in page_result:
            try:
                if project_billing_info.billing_enabled:
                    # Extract project ID from the project name (format: projects/PROJECT_ID/billingInfo)
                    project_name_parts = project_billing_info.name.split('/')
                    if len(project_name_parts) >= 3:
                        project_id = project_name_parts[1]
                        project_ids.append(project_id)
                        project_count += 1
                        
                        # Log progress every 100 projects
                        if project_count % 100 == 0:
                            logger.info(f"📊 Processed {project_count} billing projects so far...")
                        
            except Exception as e:
                logger.warning(f"Error processing billing project: {str(e)}")
                continue
                
        logger.info(f"✅ Found {len(project_ids)} projects linked to billing account.")
        return project_ids
        
    except PermissionDenied as e:
        raise BillingAccountError(f"Insufficient permissions to access billing account {BILLING_ACCOUNT_ID}: {str(e)}")
    except Exception as e:
        raise ProjectAccessError(f"Failed to retrieve billing account projects: {str(e)}")

@retry_on_failure()
def get_project_details(project_id):
    """
    Get detailed information about a specific project using Resource Manager API.
    
    Args:
        project_id (str): The project ID to get details for
        
    Returns:
        dict: Project details including labels
        
    Raises:
        ProjectAccessError: If project access fails
    """
    try:
        client = resourcemanager_v3.ProjectsClient()
        
        # Get project details
        project_name = f"projects/{project_id}"
        request = resourcemanager_v3.GetProjectRequest(name=project_name)
        
        project = client.get_project(request=request)
        
        return {
            'project_id': project.project_id,
            'display_name': project.display_name or project.project_id,
            'state': project.state.name,
            'create_time': project.create_time.strftime('%Y-%m-%d %H:%M:%S') if project.create_time else 'Unknown',
            'labels': dict(project.labels) if project.labels else {}
        }
        
    except NotFound:
        logger.warning(f"Project {project_id} not found or not accessible")
        # Return minimal details for projects we can't access
        return {
            'project_id': project_id,
            'display_name': project_id,
            'state': 'UNKNOWN',
            'create_time': 'Unknown',
            'labels': {}
        }
    except Exception as e:
        logger.warning(f"Error getting details for project {project_id}: {str(e)}")
        # Return minimal details as fallback
        return {
            'project_id': project_id,
            'display_name': project_id,
            'state': 'ERROR',
            'create_time': 'Unknown',
            'labels': {}
        }

def analyze_project_labels(project_details):
    """
    Analyze a project's labels to identify labeling status.
    
    Args:
        project_details (dict): Project details including labels
        
    Returns:
        dict: Analysis results including labeling status and compliance
    """
    project_id = project_details['project_id']
    display_name = project_details['display_name']
    labels = project_details['labels']
    
    if ANALYSIS_MODE == 'any':
        # Check if project has ANY labels
        has_any_labels = len(labels) > 0
        missing_labels = [] if has_any_labels else ['No labels found']
        missing_count = 0 if has_any_labels else 1
        compliance_score = 100.0 if has_any_labels else 0.0
        is_compliant = has_any_labels
        
        analysis_type = "Any Labels"
        
    else:  # ANALYSIS_MODE == 'recommended'
        # Check for recommended/required labels
        missing_labels = []
        if RECOMMENDED_LABELS:
            for required_label in RECOMMENDED_LABELS:
                if required_label not in labels:
                    missing_labels.append(required_label)
            
            # Calculate compliance score based on recommended labels
            compliance_score = ((len(RECOMMENDED_LABELS) - len(missing_labels)) / len(RECOMMENDED_LABELS)) * 100
            is_compliant = len(missing_labels) == 0
        else:
            # Fallback to 'any' mode if no recommended labels specified
            has_any_labels = len(labels) > 0
            missing_labels = [] if has_any_labels else ['No labels found']
            compliance_score = 100.0 if has_any_labels else 0.0
            is_compliant = has_any_labels
        
        missing_count = len(missing_labels)
        analysis_type = "Recommended Labels"
    
    return {
        'project_id': project_id,
        'display_name': display_name,
        'state': project_details['state'],
        'create_time': project_details['create_time'],
        'total_labels': len(labels),
        'existing_labels': list(labels.keys()),
        'missing_labels': missing_labels,
        'missing_count': missing_count,
        'compliance_score': round(compliance_score, 1),
        'is_compliant': is_compliant,
        'all_labels': labels,
        'analysis_type': analysis_type,
        'has_any_labels': len(labels) > 0
    }

def generate_report(project_analyses):
    """
    Generate a comprehensive report of project labeling status.
    
    Args:
        project_analyses (list): List of project analysis results
    """
    if not project_analyses:
        logger.warning("No project data to analyze")
        return
    
    # Sort by compliance score (worst first), then by missing count
    project_analyses.sort(key=lambda x: (x['missing_count'], -x['total_labels'], x['project_id']), reverse=True)
    
    logger.info("\n" + "="*100)
    logger.info("📋 PROJECT LABELING STATUS REPORT")
    logger.info("="*100)
    
    # Summary statistics
    total_projects = len(project_analyses)
    non_compliant_projects = [p for p in project_analyses if not p['is_compliant']]
    compliant_projects = [p for p in project_analyses if p['is_compliant']]
    
    # Calculate statistics based on analysis mode
    if ANALYSIS_MODE == 'any':
        unlabeled_projects = [p for p in project_analyses if p['total_labels'] == 0]
        labeled_projects = [p for p in project_analyses if p['total_labels'] > 0]
        
        logger.info(f"📊 SUMMARY STATISTICS (Analysis Mode: Any Labels):")
        logger.info(f"💼 Total projects analyzed: {total_projects}")
        logger.info(f"❌ Projects without ANY labels: {len(unlabeled_projects)} ({len(unlabeled_projects)/total_projects*100:.1f}%)")
        logger.info(f"✅ Projects with labels: {len(labeled_projects)} ({len(labeled_projects)/total_projects*100:.1f}%)")
        
        if labeled_projects:
            avg_labels = sum(p['total_labels'] for p in labeled_projects) / len(labeled_projects)
            logger.info(f"🏷️ Average labels per labeled project: {avg_labels:.1f}")
        
        # Most common labels analysis
        if labeled_projects:
            all_label_keys = {}
            for project in labeled_projects:
                for label_key in project['existing_labels']:
                    all_label_keys[label_key] = all_label_keys.get(label_key, 0) + 1
            
            logger.info(f"\n📈 MOST COMMON LABEL KEYS:")
            sorted_labels = sorted(all_label_keys.items(), key=lambda x: x[1], reverse=True)
            for label_key, count in sorted_labels[:10]:  # Top 10
                percentage = (count / len(labeled_projects)) * 100
                logger.info(f"🏷️ '{label_key}': used in {count}/{len(labeled_projects)} labeled projects ({percentage:.1f}%)")
    
    else:  # recommended mode
        avg_compliance = sum(p['compliance_score'] for p in project_analyses) / total_projects
        avg_labels = sum(p['total_labels'] for p in project_analyses) / total_projects
        
        logger.info(f"📊 SUMMARY STATISTICS (Analysis Mode: Recommended Labels):")
        logger.info(f"💼 Total projects analyzed: {total_projects}")
        logger.info(f"❌ Non-compliant projects: {len(non_compliant_projects)} ({len(non_compliant_projects)/total_projects*100:.1f}%)")
        logger.info(f"✅ Compliant projects: {len(compliant_projects)} ({len(compliant_projects)/total_projects*100:.1f}%)")
        logger.info(f"📈 Average compliance score: {avg_compliance:.1f}%")
        logger.info(f"🏷️ Average labels per project: {avg_labels:.1f}")
    
    logger.info("")
    
    # Detailed project listing
    if ANALYSIS_MODE == 'any':
        logger.info(f"{'Rank':<4} {'Project ID':<30} {'Display Name':<25} {'Labels':<7} {'Status':<12} {'Label Keys'}")
        logger.info("-" * 100)
        
        for i, project in enumerate(project_analyses, 1):
            label_keys_str = ', '.join(project['existing_labels'][:3]) if project['existing_labels'] else 'None'
            if len(project['existing_labels']) > 3:
                label_keys_str += f" (+{len(project['existing_labels'])-3} more)"
            
            status = "✅ Labeled" if project['has_any_labels'] else "❌ No Labels"
            
            logger.info(f"{i:<4} {project['project_id']:<30} {project['display_name'][:24]:<25} "
                       f"{project['total_labels']:<7} {status:<12} {label_keys_str}")
    
    else:  # recommended mode
        logger.info(f"{'Rank':<4} {'Project ID':<30} {'Display Name':<25} {'Missing':<7} {'Score':<6} {'Missing Labels'}")
        logger.info("-" * 100)
        
        for i, project in enumerate(project_analyses, 1):
            missing_labels_str = ', '.join(project['missing_labels'][:3]) if project['missing_labels'] else 'None'
            if len(project['missing_labels']) > 3:
                missing_labels_str += f" (+{len(project['missing_labels'])-3} more)"
            
            compliance_icon = "✅" if project['is_compliant'] else "❌"
            
            logger.info(f"{i:<4} {project['project_id']:<30} {project['display_name'][:24]:<25} "
                       f"{project['missing_count']:<7} {project['compliance_score']:<5.1f}% {missing_labels_str}")
    
    logger.info("-" * 100)
    
    # Additional analysis based on mode
    if ANALYSIS_MODE == 'recommended' and RECOMMENDED_LABELS:
        # Missing labels frequency analysis for recommended mode
        logger.info("\n📊 MISSING RECOMMENDED LABELS FREQUENCY ANALYSIS:")
        missing_label_counts = {}
        for project in non_compliant_projects:
            for label in project['missing_labels']:
                missing_label_counts[label] = missing_label_counts.get(label, 0) + 1
        
        sorted_missing = sorted(missing_label_counts.items(), key=lambda x: x[1], reverse=True)
        for label, count in sorted_missing:
            percentage = (count / total_projects) * 100
            logger.info(f"🏷️ '{label}': missing in {count}/{total_projects} projects ({percentage:.1f}%)")

def save_results_to_csv(project_analyses):
    """
    Save the analysis results to a CSV file.
    
    Args:
        project_analyses (list): List of project analysis results
        
    Returns:
        str: Path to the saved CSV file
    """
    if not project_analyses:
        logger.warning("No data to save")
        return None
    
    # Prepare data for CSV
    csv_data = []
    for project in project_analyses:
        csv_data.append({
            'Project_ID': project['project_id'],
            'Display_Name': project['display_name'],
            'State': project['state'],
            'Create_Time': project['create_time'],
            'Total_Labels': project['total_labels'],
            'Has_Any_Labels': project['has_any_labels'],
            'Missing_Labels_Count': project['missing_count'],
            'Compliance_Score_Percent': project['compliance_score'],
            'Is_Compliant': project['is_compliant'],
            'Analysis_Type': project['analysis_type'],
            'Missing_Labels': ', '.join(project['missing_labels']),
            'Existing_Labels': ', '.join(project['existing_labels']),
            'All_Labels_JSON': str(project['all_labels'])
        })
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(csv_data)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode_suffix = "any_labels" if ANALYSIS_MODE == 'any' else "recommended_labels"
    filename = f"project_labeling_{mode_suffix}_{BILLING_ACCOUNT_ID.replace('-', '_')}_{timestamp}.csv"
    
    # Save to CSV
    df.to_csv(filename, index=False)
    
    # Get file size
    file_size = os.path.getsize(filename)
    
    logger.info(f"💾 Results saved to: {filename} ({file_size} bytes)")
    return filename

def main():
    """
    Main function to orchestrate the project labeling analysis for billing account projects.
    """
    start_time = time.time()
    
    analysis_description = "any labels" if ANALYSIS_MODE == 'any' else f"recommended labels ({', '.join(RECOMMENDED_LABELS) if RECOMMENDED_LABELS else 'none specified'})"
    
    logger.info("🚀 Starting project labeling analysis for billing account projects...")
    logger.info(f"💳 Target billing account: {BILLING_ACCOUNT_ID}")
    logger.info(f"📊 Analyzing up to {MAX_PROJECTS} projects")
    logger.info(f"🔍 Analysis mode: {ANALYSIS_MODE.upper()} - checking for {analysis_description}")
    logger.info("="*80)
    
    try:
        # Validate environment
        validate_environment()
        
        # Get projects linked to the billing account
        billing_project_ids = get_billing_account_projects()
        
        if not billing_project_ids:
            logger.warning("No projects found linked to the billing account")
            return
        
        # Limit to specified number of projects
        projects_to_analyze = billing_project_ids[:MAX_PROJECTS]
        
        logger.info(f"\n🔍 Analyzing labeling status for {len(projects_to_analyze)} projects...")
        logger.info(f"📊 (Limited from {len(billing_project_ids)} total billing account projects)")
        logger.info(f"🔍 Analysis mode: {ANALYSIS_MODE.upper()}")
        
        # Analyze each project
        project_analyses = []
        
        for i, project_id in enumerate(projects_to_analyze, 1):
            try:
                logger.info(f"📊 [{i}/{len(projects_to_analyze)}] Analyzing project: {project_id}")
                
                # Get project details
                project_details = get_project_details(project_id)
                
                # Analyze labels
                analysis = analyze_project_labels(project_details)
                project_analyses.append(analysis)
                
                # Progress update every 10 projects
                if i % 10 == 0:
                    logger.info(f"✅ Processed {i}/{len(projects_to_analyze)} projects")
                    
            except Exception as e:
                logger.error(f"Error analyzing project {project_id}: {str(e)}")
                continue
        
        logger.info(f"✅ Processed {len(project_analyses)}/{len(projects_to_analyze)} projects")
        
        # Generate report
        generate_report(project_analyses)
        
        # Save results to CSV
        csv_file = save_results_to_csv(project_analyses)
        if csv_file:
            logger.info(f"✅ Results saved to: {csv_file}")
        
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"\n🏁 Analysis complete! Execution time: {execution_time:.2f} seconds")
        
    except ProjectAnalysisError as e:
        logger.error(f"❌ Project analysis error: {str(e)}")
        sys.exit(1)
    except BillingAccountError as e:
        logger.error(f"❌ Billing account error: {str(e)}")
        sys.exit(1)
    except ProjectAccessError as e:
        logger.error(f"❌ Project access error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
