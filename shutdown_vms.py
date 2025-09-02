#!/usr/bin/env python3
"""
GCP VM Shutdown Script
Shuts down all running compute engine instances across all projects under a billing account.
WARNING: This script will stop ALL running VMs - use with extreme caution!
"""

import os
import datetime
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
import pandas as pd

# Google Cloud imports
from google.cloud import billing_v1, compute_v1
from google.api_core import exceptions

# --- Configuration ---
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
BATCH_SIZE = 500  # Number of projects to process per batch
MAX_WORKERS = 10  # Number of parallel workers for processing projects
MAX_RETRIES = 3  # Maximum number of retries for API calls
RETRY_DELAY = 5  # Delay between retries in seconds
DRY_RUN = True  # Set to False to actually shutdown VMs

# Exclusion lists - VMs that should NOT be shutdown
EXCLUDED_PROJECTS = [
    # Add project IDs that should be excluded from shutdown
    # Example: "production-project-123"
]

EXCLUDED_INSTANCES = [
    # Add specific instance names that should never be shutdown
    # Format: "project_id:instance_name"
    # Example: "my-project:critical-database-vm"
]

EXCLUDED_ZONES = [
    # Add zones that should be excluded
    # Example: "us-central1-a"
]

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_projects_under_billing_account(billing_account_id: str) -> List[str]:
    """Get all project IDs under the specified billing account."""
    try:
        logger.info(f"Initializing Cloud Billing client...")
        client = billing_v1.CloudBillingClient()
        billing_account_name = f'billingAccounts/{billing_account_id}'
        
        logger.info(f"Fetching projects under billing account: {billing_account_id}")
        
        try:
            projects = list(retry_api_call(client.list_project_billing_info, name=billing_account_name))
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing billing account {billing_account_id}: {e}")
            logger.error("Make sure you have 'Billing Account Viewer' role or higher")
            return []
        except exceptions.NotFound as e:
            logger.error(f"Billing account {billing_account_id} not found: {e}")
            logger.error("Please verify the billing account ID is correct")
            return []
        except exceptions.Forbidden as e:
            logger.error(f"Access forbidden to billing account {billing_account_id}: {e}")
            logger.error("Check if the billing account exists and you have proper access")
            return []
        except Exception as e:
            logger.error(f"Unexpected error accessing billing account: {type(e).__name__}: {e}")
            return []
        
        project_ids = []
        excluded_count = 0
        disabled_billing_count = 0
        
        for project in projects:
            if not project.billing_enabled:
                disabled_billing_count += 1
                continue
                
            if project.project_id in EXCLUDED_PROJECTS:
                excluded_count += 1
                logger.info(f"Excluding project: {project.project_id}")
                continue
                
            project_ids.append(project.project_id)
        
        logger.info(f"Found {len(project_ids)} active projects")
        logger.info(f"Skipped {disabled_billing_count} projects with disabled billing")
        logger.info(f"Excluded {excluded_count} projects from configuration")
        
        if not project_ids:
            logger.warning("No active projects found! This could indicate:")
            logger.warning("- All projects have disabled billing")
            logger.warning("- All projects are in the exclusion list")
            logger.warning("- Permission issues")
        
        return project_ids
    
    except Exception as e:
        logger.error(f"Critical error fetching projects: {type(e).__name__}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def retry_api_call(func, *args, **kwargs):
    """Retry API calls with exponential backoff and comprehensive error handling."""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except exceptions.ResourceExhausted as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Rate limit exceeded, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Rate limit exceeded after {MAX_RETRIES} attempts: {e}")
                raise
        except exceptions.DeadlineExceeded as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"API timeout, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"API timeout after {MAX_RETRIES} attempts: {e}")
                raise
        except exceptions.ServiceUnavailable as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Service unavailable, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Service unavailable after {MAX_RETRIES} attempts: {e}")
                raise
        except exceptions.InternalServerError as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Internal server error, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Internal server error after {MAX_RETRIES} attempts: {e}")
                raise
        except exceptions.BadGateway as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Bad gateway, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Bad gateway after {MAX_RETRIES} attempts: {e}")
                raise
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied (non-retryable): {e}")
            raise
        except exceptions.NotFound as e:
            logger.error(f"Resource not found (non-retryable): {e}")
            raise
        except exceptions.Forbidden as e:
            logger.error(f"Access forbidden (non-retryable): {e}")
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Unexpected error, waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}: {type(e).__name__}: {e}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Unexpected error after {MAX_RETRIES} attempts: {type(e).__name__}: {e}")
                raise

def get_running_instances(project_id: str) -> List[Dict[str, Any]]:
    """Get all running instances in a project with comprehensive error handling."""
    running_instances = []
    
    try:
        logger.debug(f"Initializing Compute instances client for {project_id}")
        instances_client = compute_v1.InstancesClient()
        
        try:
            logger.debug(f"Fetching instances for project: {project_id}")
            aggregated_instances = retry_api_call(instances_client.aggregated_list, project=project_id)
            
            for zone, instances_scoped_list in aggregated_instances:
                if instances_scoped_list.instances:
                    zone_name = zone.split('/')[-1] if '/' in zone else zone
                    
                    # Skip excluded zones
                    if zone_name in EXCLUDED_ZONES:
                        logger.info(f"Skipping excluded zone: {zone_name}")
                        continue
                    
                    for instance in instances_scoped_list.instances:
                        try:
                            # Check if instance is running
                            if instance.status == compute_v1.Instance.Status.RUNNING:
                                instance_key = f"{project_id}:{instance.name}"
                                
                                # Skip excluded instances
                                if instance_key in EXCLUDED_INSTANCES:
                                    logger.info(f"Skipping excluded instance: {instance_key}")
                                    continue
                                
                                # Get instance details with error handling
                                try:
                                    machine_type = instance.machine_type.split('/')[-1] if instance.machine_type else 'unknown'
                                    
                                    # Safely extract network information
                                    internal_ip = None
                                    external_ip = None
                                    
                                    if instance.network_interfaces:
                                        if len(instance.network_interfaces) > 0:
                                            internal_ip = getattr(instance.network_interfaces[0], 'network_i_p', None)
                                            
                                            if (hasattr(instance.network_interfaces[0], 'access_configs') and 
                                                instance.network_interfaces[0].access_configs and
                                                len(instance.network_interfaces[0].access_configs) > 0):
                                                external_ip = getattr(instance.network_interfaces[0].access_configs[0], 'nat_i_p', None)
                                    
                                    running_instances.append({
                                        'name': instance.name,
                                        'zone': zone_name,
                                        'machine_type': machine_type,
                                        'status': instance.status,
                                        'creation_timestamp': getattr(instance, 'creation_timestamp', 'unknown'),
                                        'internal_ip': internal_ip,
                                        'external_ip': external_ip
                                    })
                                    
                                except Exception as e:
                                    logger.warning(f"Error extracting details for instance {instance.name}: {e}")
                                    # Add instance with minimal info
                                    running_instances.append({
                                        'name': getattr(instance, 'name', 'unknown'),
                                        'zone': zone_name,
                                        'machine_type': 'unknown',
                                        'status': getattr(instance, 'status', 'unknown'),
                                        'creation_timestamp': 'unknown',
                                        'internal_ip': None,
                                        'external_ip': None
                                    })
                        
                        except Exception as e:
                            logger.warning(f"Error processing instance in {zone_name}: {e}")
                            continue
        
        except exceptions.PermissionDenied as e:
            logger.warning(f"Permission denied accessing instances in {project_id}: {e}")
            logger.warning(f"Make sure you have 'Compute Viewer' role or higher for {project_id}")
        except exceptions.Forbidden as e:
            logger.warning(f"Access forbidden to instances in {project_id}: {e}")
            logger.warning(f"Compute Engine API may be disabled for {project_id}")
        except exceptions.NotFound as e:
            logger.warning(f"Project {project_id} not found or not accessible: {e}")
        except exceptions.ServiceUnavailable as e:
            logger.warning(f"Compute Engine service unavailable for {project_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting instances for {project_id}: {type(e).__name__}: {e}")
    
    except Exception as e:
        logger.error(f"Critical error initializing compute client for {project_id}: {type(e).__name__}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    logger.debug(f"Found {len(running_instances)} running instances in {project_id}")
    return running_instances

def shutdown_instance(project_id: str, zone: str, instance_name: str, dry_run: bool = True) -> Dict[str, Any]:
    """Shutdown a single instance with comprehensive error handling."""
    result = {
        'project_id': project_id,
        'zone': zone,
        'instance_name': instance_name,
        'status': 'success',
        'error': None,
        'operation_id': None,
        'error_type': None
    }
    
    try:
        logger.debug(f"Initializing instances client for shutdown operation")
        instances_client = compute_v1.InstancesClient()
        
        if dry_run:
            logger.info(f"DRY RUN: Would shutdown {project_id}:{instance_name} in {zone}")
            result['status'] = 'dry_run'
            return result
        
        logger.info(f"Attempting to shutdown {project_id}:{instance_name} in {zone}")
        
        try:
            # Stop the instance
            operation = retry_api_call(
                instances_client.stop,
                project=project_id,
                zone=zone,
                instance=instance_name
            )
            
            result['operation_id'] = operation.name
            logger.info(f"Shutdown initiated for {project_id}:{instance_name} - Operation: {operation.name}")
            
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied shutting down {project_id}:{instance_name}: {e}")
            result['status'] = 'permission_denied'
            result['error'] = f"Permission denied: {str(e)}"
            result['error_type'] = 'PermissionDenied'
        except exceptions.NotFound as e:
            logger.warning(f"Instance not found {project_id}:{instance_name}: {e}")
            result['status'] = 'not_found'
            result['error'] = f"Instance not found: {str(e)}"
            result['error_type'] = 'NotFound'
        except exceptions.Forbidden as e:
            logger.error(f"Access forbidden for {project_id}:{instance_name}: {e}")
            result['status'] = 'forbidden'
            result['error'] = f"Access forbidden: {str(e)}"
            result['error_type'] = 'Forbidden'
        except exceptions.BadRequest as e:
            logger.error(f"Bad request shutting down {project_id}:{instance_name}: {e}")
            result['status'] = 'bad_request'
            result['error'] = f"Bad request (instance may already be stopping): {str(e)}"
            result['error_type'] = 'BadRequest'
        except exceptions.Conflict as e:
            logger.warning(f"Conflict shutting down {project_id}:{instance_name}: {e}")
            result['status'] = 'conflict'
            result['error'] = f"Conflict (operation already in progress): {str(e)}"
            result['error_type'] = 'Conflict'
        except exceptions.ResourceExhausted as e:
            logger.error(f"Rate limit exceeded for {project_id}:{instance_name}: {e}")
            result['status'] = 'rate_limited'
            result['error'] = f"Rate limit exceeded: {str(e)}"
            result['error_type'] = 'ResourceExhausted'
        except exceptions.DeadlineExceeded as e:
            logger.error(f"Timeout shutting down {project_id}:{instance_name}: {e}")
            result['status'] = 'timeout'
            result['error'] = f"Operation timeout: {str(e)}"
            result['error_type'] = 'DeadlineExceeded'
        except Exception as e:
            logger.error(f"Unexpected error shutting down {project_id}:{instance_name}: {type(e).__name__}: {e}")
            result['status'] = 'error'
            result['error'] = f"Unexpected error: {str(e)}"
            result['error_type'] = type(e).__name__
    
    except Exception as e:
        logger.error(f"Critical error in shutdown operation for {project_id}:{instance_name}: {type(e).__name__}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        result['status'] = 'critical_error'
        result['error'] = f"Critical error: {str(e)}"
        result['error_type'] = type(e).__name__
    
    return result

def process_project_batch(project_batch: List[str], batch_number: int, total_batches: int, dry_run: bool = True) -> List[Dict[str, Any]]:
    """Process a batch of projects with parallel execution."""
    logger.info(f"Processing batch {batch_number}/{total_batches} with {len(project_batch)} projects")
    
    batch_results = []
    batch_start_time = datetime.datetime.now()
    
    try:
        # Process projects in this batch with limited parallelism
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_project = {}
            
            # Submit all project processing tasks for this batch
            for project_id in project_batch:
                try:
                    future = executor.submit(process_project, project_id, dry_run)
                    future_to_project[future] = project_id
                except Exception as e:
                    logger.error(f"Failed to submit processing task for {project_id}: {e}")
                    # Create a failure result
                    batch_results.append({
                        'project_id': project_id,
                        'status': 'submission_failed',
                        'error': f"Failed to submit task: {str(e)}",
                        'error_type': type(e).__name__,
                        'running_instances': [],
                        'shutdown_results': [],
                        'total_running': 0,
                        'shutdown_attempted': 0,
                        'shutdown_successful': 0,
                        'shutdown_failed': 0,
                        'errors_by_type': {type(e).__name__: 1}
                    })
            
            # Collect results with timeout handling
            completed_count = 0
            for future in as_completed(future_to_project, timeout=1800):  # 30 minute timeout per batch
                project_id = future_to_project[future]
                completed_count += 1
                
                try:
                    result = future.result(timeout=180)  # 3 minute timeout per project
                    batch_results.append(result)
                    
                    # Log progress within batch
                    batch_progress = (completed_count / len(future_to_project)) * 100
                    if result['total_running'] > 0:
                        status_icon = "✅" if result['status'] == 'success' else "❌"
                        logger.info(f"{status_icon} Batch {batch_number} [{batch_progress:.1f}%] {project_id}: "
                                  f"{result['total_running']} running, "
                                  f"{result['shutdown_successful']}/{result['shutdown_attempted']} shutdown")
                    else:
                        logger.debug(f"⚪ Batch {batch_number} [{batch_progress:.1f}%] {project_id}: No running instances")
                    
                except Exception as e:
                    logger.error(f"Error processing project {project_id} in batch {batch_number}: {type(e).__name__}: {e}")
                    # Create a failure result
                    batch_results.append({
                        'project_id': project_id,
                        'status': 'processing_failed',
                        'error': f"Processing failed: {str(e)}",
                        'error_type': type(e).__name__,
                        'running_instances': [],
                        'shutdown_results': [],
                        'total_running': 0,
                        'shutdown_attempted': 0,
                        'shutdown_successful': 0,
                        'shutdown_failed': 0,
                        'errors_by_type': {type(e).__name__: 1}
                    })
    
    except Exception as e:
        logger.error(f"Critical error processing batch {batch_number}: {type(e).__name__}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Create failure results for remaining projects
        for project_id in project_batch:
            if not any(result['project_id'] == project_id for result in batch_results):
                batch_results.append({
                    'project_id': project_id,
                    'status': 'batch_failed',
                    'error': f"Batch processing failed: {str(e)}",
                    'error_type': type(e).__name__,
                    'running_instances': [],
                    'shutdown_results': [],
                    'total_running': 0,
                    'shutdown_attempted': 0,
                    'shutdown_successful': 0,
                    'shutdown_failed': 0,
                    'errors_by_type': {type(e).__name__: 1}
                })
    
    # Calculate batch summary
    batch_duration = datetime.datetime.now() - batch_start_time
    total_vms_in_batch = sum(result['total_running'] for result in batch_results)
    total_shutdown_attempted = sum(result['shutdown_attempted'] for result in batch_results)
    total_shutdown_successful = sum(result['shutdown_successful'] for result in batch_results)
    
    logger.info(f"✅ Batch {batch_number}/{total_batches} completed in {batch_duration}")
    logger.info(f"   Projects: {len(batch_results)}, VMs found: {total_vms_in_batch}, "
               f"Shutdown: {total_shutdown_successful}/{total_shutdown_attempted}")
    
    return batch_results
    """Process a single project to find and shutdown running instances with comprehensive error handling."""
    logger.info(f"Processing project: {project_id}")
    
    result = {
        'project_id': project_id,
        'status': 'success',
        'error': None,
        'error_type': None,
        'running_instances': [],
        'shutdown_results': [],
        'total_running': 0,
        'shutdown_attempted': 0,
        'shutdown_successful': 0,
        'shutdown_failed': 0,
        'errors_by_type': {}
    }
    
    try:
        # Get running instances
        try:
            running_instances = get_running_instances(project_id)
            result['running_instances'] = running_instances
            result['total_running'] = len(running_instances)
            
            logger.info(f"  Found {len(running_instances)} running instances in {project_id}")
            
        except Exception as e:
            logger.error(f"Failed to get instances for {project_id}: {type(e).__name__}: {e}")
            result['status'] = 'instance_discovery_failed'
            result['error'] = f"Failed to discover instances: {str(e)}"
            result['error_type'] = type(e).__name__
            return result
        
        if running_instances:
            # Shutdown instances with limited parallelism per project
            try:
                with ThreadPoolExecutor(max_workers=3) as executor:
                    shutdown_futures = {}
                    
                    for instance in running_instances:
                        try:
                            future = executor.submit(
                                shutdown_instance,
                                project_id,
                                instance['zone'],
                                instance['name'],
                                dry_run
                            )
                            shutdown_futures[future] = instance
                        except Exception as e:
                            logger.error(f"Failed to submit shutdown task for {instance['name']}: {e}")
                            # Create a failed result manually
                            failed_result = {
                                'project_id': project_id,
                                'zone': instance['zone'],
                                'instance_name': instance['name'],
                                'status': 'submission_failed',
                                'error': f"Failed to submit shutdown task: {str(e)}",
                                'error_type': type(e).__name__,
                                'operation_id': None
                            }
                            result['shutdown_results'].append(failed_result)
                            result['shutdown_attempted'] += 1
                            result['shutdown_failed'] += 1
                    
                    # Collect results
                    for future in as_completed(shutdown_futures):
                        instance = shutdown_futures[future]
                        try:
                            shutdown_result = future.result(timeout=60)  # 60 second timeout per operation
                            result['shutdown_results'].append(shutdown_result)
                            result['shutdown_attempted'] += 1
                            
                            if shutdown_result['status'] in ['success', 'dry_run']:
                                result['shutdown_successful'] += 1
                            else:
                                result['shutdown_failed'] += 1
                                # Track error types
                                error_type = shutdown_result.get('error_type', 'unknown')
                                result['errors_by_type'][error_type] = result['errors_by_type'].get(error_type, 0) + 1
                            
                        except Exception as e:
                            logger.error(f"Error in shutdown future for {instance['name']}: {type(e).__name__}: {e}")
                            result['shutdown_results'].append({
                                'project_id': project_id,
                                'zone': instance['zone'],
                                'instance_name': instance['name'],
                                'status': 'future_failed',
                                'error': f"Future execution failed: {str(e)}",
                                'error_type': type(e).__name__,
                                'operation_id': None
                            })
                            result['shutdown_attempted'] += 1
                            result['shutdown_failed'] += 1
                            
            except Exception as e:
                logger.error(f"Critical error in ThreadPoolExecutor for {project_id}: {type(e).__name__}: {e}")
                result['status'] = 'executor_failed'
                result['error'] = f"Thread pool execution failed: {str(e)}"
                result['error_type'] = type(e).__name__
        else:
            logger.debug(f"No running instances found in {project_id}")
        
    except Exception as e:
        logger.error(f"Critical error processing project {project_id}: {type(e).__name__}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        result['status'] = 'critical_error'
        result['error'] = f"Critical processing error: {str(e)}"
        result['error_type'] = type(e).__name__
    
    # Log summary for this project
    if result['total_running'] > 0:
        success_rate = (result['shutdown_successful'] / result['shutdown_attempted'] * 100) if result['shutdown_attempted'] > 0 else 0
        logger.info(f"  {project_id} summary: {result['total_running']} running, "
                   f"{result['shutdown_successful']}/{result['shutdown_attempted']} shutdown successful ({success_rate:.1f}%)")
        if result['errors_by_type']:
            logger.warning(f"  {project_id} errors: {dict(result['errors_by_type'])}")
    
    return result

def generate_shutdown_instances_report(all_results: List[Dict[str, Any]], billing_account_id: str, dry_run: bool) -> str:
    """Generate a comprehensive Excel report focusing on shutdown instances."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "dry_run" if dry_run else "actual"
    filename = f"vm_shutdown_instances_{mode}_{billing_account_id}_{timestamp}.xlsx"
    
    logger.info(f"Generating shutdown instances report: {filename}")
    
    # Prepare data for shutdown instances
    shutdown_instances_data = []
    project_summary_data = []
    batch_summary_data = []
    error_analysis_data = []
    
    # Track statistics
    total_projects = len(all_results)
    total_running = 0
    total_shutdown_attempted = 0
    total_shutdown_successful = 0
    total_shutdown_failed = 0
    projects_with_vms = 0
    projects_with_errors = 0
    error_types_summary = {}
    
    # Process results by batch (if we want to track batch performance)
    current_batch = 1
    projects_in_current_batch = 0
    batch_vms = 0
    batch_shutdown = 0
    
    for i, result in enumerate(all_results):
        project_id = result['project_id']
        
        # Track batch progress (assuming BATCH_SIZE projects per batch)
        if projects_in_current_batch >= BATCH_SIZE:
            # Save current batch summary
            batch_summary_data.append({
                'Batch Number': current_batch,
                'Projects in Batch': projects_in_current_batch,
                'VMs Found': batch_vms,
                'VMs Shutdown': batch_shutdown,
                'Batch Success Rate': f"{(batch_shutdown/batch_vms*100):.1f}%" if batch_vms > 0 else "N/A"
            })
            # Reset for next batch
            current_batch += 1
            projects_in_current_batch = 0
            batch_vms = 0
            batch_shutdown = 0
        
        projects_in_current_batch += 1
        
        # Update totals
        if result['total_running'] > 0:
            projects_with_vms += 1
        
        if result.get('error') or result.get('shutdown_failed', 0) > 0:
            projects_with_errors += 1
        
        total_running += result['total_running']
        total_shutdown_attempted += result['shutdown_attempted']
        total_shutdown_successful += result['shutdown_successful']
        total_shutdown_failed += result.get('shutdown_failed', 0)
        
        batch_vms += result['total_running']
        batch_shutdown += result['shutdown_successful']
        
        # Aggregate error types
        for error_type, count in result.get('errors_by_type', {}).items():
            error_types_summary[error_type] = error_types_summary.get(error_type, 0) + count
        
        # Project summary
        project_summary_data.append({
            'Batch': current_batch,
            'Project ID': project_id,
            'Status': result['status'],
            'Running Instances Found': result['total_running'],
            'Shutdown Attempted': result['shutdown_attempted'],
            'Shutdown Successful': result['shutdown_successful'],
            'Shutdown Failed': result.get('shutdown_failed', 0),
            'Success Rate': f"{(result['shutdown_successful']/result['shutdown_attempted']*100):.1f}%" if result['shutdown_attempted'] > 0 else "N/A",
            'Error Type': result.get('error_type', ''),
            'Error Message': result.get('error', '')
        })
        
        # Detailed shutdown instances data
        for op in result.get('shutdown_results', []):
            # Find the corresponding instance details
            instance_details = None
            for instance in result.get('running_instances', []):
                if instance['name'] == op['instance_name'] and instance['zone'] == op['zone']:
                    instance_details = instance
                    break
            
            shutdown_instances_data.append({
                'Batch': current_batch,
                'Project ID': op['project_id'],
                'Instance Name': op['instance_name'],
                'Zone': op['zone'],
                'Machine Type': instance_details['machine_type'] if instance_details else 'unknown',
                'Internal IP': instance_details['internal_ip'] if instance_details else None,
                'External IP': instance_details['external_ip'] if instance_details else None,
                'Created': instance_details['creation_timestamp'] if instance_details else 'unknown',
                'Shutdown Status': op['status'],
                'Operation ID': op['operation_id'],
                'Action Taken': 'Would shutdown' if dry_run else 'Shutdown initiated',
                'Error Type': op.get('error_type', ''),
                'Error Message': op.get('error', '')
            })
        
        # Error analysis
        if result.get('error'):
            error_analysis_data.append({
                'Batch': current_batch,
                'Project ID': project_id,
                'Error Type': result.get('error_type', 'Unknown'),
                'Error Message': result.get('error', ''),
                'Impact': f"Failed to process {result['total_running']} running instances" if result['total_running'] > 0 else "Project processing failed"
            })
    
    # Add final batch if it has projects
    if projects_in_current_batch > 0:
        batch_summary_data.append({
            'Batch Number': current_batch,
            'Projects in Batch': projects_in_current_batch,
            'VMs Found': batch_vms,
            'VMs Shutdown': batch_shutdown,
            'Batch Success Rate': f"{(batch_shutdown/batch_vms*100):.1f}%" if batch_vms > 0 else "N/A"
        })
    
    try:
        # Create Excel report with multiple sheets
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main sheet: Shutdown Instances (most important)
            if shutdown_instances_data:
                shutdown_df = pd.DataFrame(shutdown_instances_data)
                shutdown_df.to_excel(writer, sheet_name='Shutdown Instances', index=False)
            
            # Project Summary
            if project_summary_data:
                summary_df = pd.DataFrame(project_summary_data)
                summary_df.to_excel(writer, sheet_name='Project Summary', index=False)
            
            # Batch Summary
            if batch_summary_data:
                batch_df = pd.DataFrame(batch_summary_data)
                batch_df.to_excel(writer, sheet_name='Batch Summary', index=False)
            
            # Error Analysis
            if error_analysis_data:
                errors_df = pd.DataFrame(error_analysis_data)
                errors_df.to_excel(writer, sheet_name='Error Analysis', index=False)
            
            # Error Types Summary
            if error_types_summary:
                error_summary_df = pd.DataFrame([
                    {'Error Type': error_type, 'Total Count': count} 
                    for error_type, count in error_types_summary.items()
                ])
                error_summary_df.to_excel(writer, sheet_name='Error Types Summary', index=False)
        
        logger.info(f"Shutdown instances report successfully saved: {filename}")
        
    except Exception as e:
        logger.error(f"Error generating Excel report: {type(e).__name__}: {e}")
        logger.warning("Attempting to save as CSV files instead...")
        
        try:
            # Fallback to CSV files
            if shutdown_instances_data:
                csv_filename = filename.replace('.xlsx', '_shutdown_instances.csv')
                pd.DataFrame(shutdown_instances_data).to_csv(csv_filename, index=False)
                logger.info(f"Shutdown instances saved as CSV: {csv_filename}")
        except Exception as csv_error:
            logger.error(f"Failed to save CSV fallback: {csv_error}")
    
    # Print comprehensive summary
    print(f"\n🛑 VM SHUTDOWN INSTANCES REPORT")
    print(f"=" * 80)
    print(f"Billing Account: {billing_account_id}")
    print(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL SHUTDOWN'}")
    print(f"Processing Method: Batch processing ({BATCH_SIZE} projects per batch)")
    print(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Report saved as: {filename}")
    
    print(f"\n📊 OVERALL SUMMARY:")
    print(f"  • Total Projects Processed: {total_projects}")
    print(f"  • Total Batches: {len(batch_summary_data)}")
    print(f"  • Projects with Running VMs: {projects_with_vms}")
    print(f"  • Projects with Errors: {projects_with_errors}")
    print(f"  • Total Running Instances Found: {total_running}")
    print(f"  • Total Shutdown Attempted: {total_shutdown_attempted}")
    print(f"  • Total Shutdown Successful: {total_shutdown_successful}")
    print(f"  • Total Shutdown Failed: {total_shutdown_failed}")
    
    if total_shutdown_attempted > 0:
        overall_success_rate = (total_shutdown_successful / total_shutdown_attempted) * 100
        print(f"  • Overall Success Rate: {overall_success_rate:.1f}%")
    
    if len(shutdown_instances_data) > 0:
        print(f"\n🖥️  INSTANCES THAT {'WOULD BE' if dry_run else 'WERE'} SHUTDOWN:")
        print(f"  • Total Instances: {len(shutdown_instances_data)}")
        
        # Show top projects by instance count
        project_instance_counts = {}
        for instance in shutdown_instances_data:
            project_id = instance['Project ID']
            project_instance_counts[project_id] = project_instance_counts.get(project_id, 0) + 1
        
        if project_instance_counts:
            top_projects = sorted(project_instance_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            print(f"  • Top Projects by VM Count:")
            for project_id, count in top_projects:
                print(f"    - {project_id}: {count} instances")
    
    if error_types_summary:
        print(f"\n❌ ERROR ANALYSIS:")
        for error_type, count in sorted(error_types_summary.items(), key=lambda x: x[1], reverse=True):
            print(f"  • {error_type}: {count} occurrences")
    
    if dry_run:
        print(f"\n⚠️  This was a DRY RUN - no VMs were actually shutdown!")
        print(f"   The report shows what WOULD happen if you set DRY_RUN = False")
    else:
        print(f"\n✅ Shutdown operations initiated - check GCP Console for progress")
        if total_shutdown_failed > 0:
            print(f"⚠️  {total_shutdown_failed} shutdown operations failed - check the Error Analysis sheet")
    
    return filename
    """Generate a detailed report of the shutdown operation with comprehensive error analysis."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "dry_run" if dry_run else "actual"
    filename = f"vm_shutdown_report_{mode}_{billing_account_id}_{timestamp}.xlsx"
    
    logger.info(f"Generating comprehensive report: {filename}")
    
    # Prepare data
    summary_data = []
    instances_data = []
    operations_data = []
    errors_data = []
    
    total_running = 0
    total_shutdown_attempted = 0
    total_shutdown_successful = 0
    total_shutdown_failed = 0
    total_projects_with_vms = 0
    total_projects_with_errors = 0
    error_types_summary = {}
    
    for result in all_results:
        project_id = result['project_id']
        
        if result['total_running'] > 0:
            total_projects_with_vms += 1
        
        if result.get('error') or result['shutdown_failed'] > 0:
            total_projects_with_errors += 1
        
        total_running += result['total_running']
        total_shutdown_attempted += result['shutdown_attempted']
        total_shutdown_successful += result['shutdown_successful']
        total_shutdown_failed += result.get('shutdown_failed', 0)
        
        # Aggregate error types
        for error_type, count in result.get('errors_by_type', {}).items():
            error_types_summary[error_type] = error_types_summary.get(error_type, 0) + count
        
        # Project summary
        summary_data.append({
            'Project ID': project_id,
            'Status': result['status'],
            'Running Instances': result['total_running'],
            'Shutdown Attempted': result['shutdown_attempted'],
            'Shutdown Successful': result['shutdown_successful'],
            'Shutdown Failed': result.get('shutdown_failed', 0),
            'Success Rate': f"{(result['shutdown_successful']/result['shutdown_attempted']*100):.1f}%" if result['shutdown_attempted'] > 0 else "N/A",
            'Error Type': result.get('error_type', ''),
            'Error': result.get('error', '')
        })
        
        # Instance details
        for instance in result['running_instances']:
            instances_data.append({
                'Project ID': project_id,
                'Instance Name': instance['name'],
                'Zone': instance['zone'],
                'Machine Type': instance['machine_type'],
                'Status': instance['status'],
                'Internal IP': instance['internal_ip'],
                'External IP': instance['external_ip'],
                'Created': instance['creation_timestamp']
            })
        
        # Shutdown operations
        for op in result['shutdown_results']:
            operations_data.append({
                'Project ID': op['project_id'],
                'Instance Name': op['instance_name'],
                'Zone': op['zone'],
                'Operation Status': op['status'],
                'Operation ID': op['operation_id'],
                'Error Type': op.get('error_type', ''),
                'Error': op.get('error', '')
            })
            
            # Collect unique errors for analysis
            if op.get('error'):
                errors_data.append({
                    'Project ID': op['project_id'],
                    'Instance Name': op['instance_name'],
                    'Zone': op['zone'],
                    'Error Type': op.get('error_type', 'Unknown'),
                    'Error Message': op.get('error', ''),
                    'Operation Status': op['status']
                })
    
    try:
        # Create Excel report
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Summary sheet
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Project Summary', index=False)
            
            # Instances sheet
            if instances_data:
                instances_df = pd.DataFrame(instances_data)
                instances_df.to_excel(writer, sheet_name='Running Instances', index=False)
            
            # Operations sheet
            if operations_data:
                operations_df = pd.DataFrame(operations_data)
                operations_df.to_excel(writer, sheet_name='Shutdown Operations', index=False)
            
            # Errors sheet
            if errors_data:
                errors_df = pd.DataFrame(errors_data)
                errors_df.to_excel(writer, sheet_name='Errors Analysis', index=False)
            
            # Error summary sheet
            if error_types_summary:
                error_summary_df = pd.DataFrame([
                    {'Error Type': error_type, 'Count': count} 
                    for error_type, count in error_types_summary.items()
                ])
                error_summary_df.to_excel(writer, sheet_name='Error Types Summary', index=False)
        
        logger.info(f"Report successfully saved: {filename}")
        
    except Exception as e:
        logger.error(f"Error generating Excel report: {type(e).__name__}: {e}")
        logger.warning("Attempting to save as CSV files instead...")
        
        try:
            # Fallback to CSV files
            if summary_data:
                csv_filename = filename.replace('.xlsx', '_summary.csv')
                pd.DataFrame(summary_data).to_csv(csv_filename, index=False)
                logger.info(f"Summary saved as CSV: {csv_filename}")
        except Exception as csv_error:
            logger.error(f"Failed to save CSV fallback: {csv_error}")
    
    # Print comprehensive summary
    print(f"\n🛑 VM SHUTDOWN REPORT")
    print(f"=" * 70)
    print(f"Billing Account: {billing_account_id}")
    print(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL SHUTDOWN'}")
    print(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Report saved as: {filename}")
    print(f"\n📊 SUMMARY:")
    print(f"  • Total Projects Processed: {len(all_results)}")
    print(f"  • Projects with Running VMs: {total_projects_with_vms}")
    print(f"  • Projects with Errors: {total_projects_with_errors}")
    print(f"  • Total Running Instances Found: {total_running}")
    print(f"  • Shutdown Attempted: {total_shutdown_attempted}")
    print(f"  • Shutdown Successful: {total_shutdown_successful}")
    print(f"  • Shutdown Failed: {total_shutdown_failed}")
    if total_shutdown_attempted > 0:
        success_rate = (total_shutdown_successful / total_shutdown_attempted) * 100
        print(f"  • Overall Success Rate: {success_rate:.1f}%")
    
    if error_types_summary:
        print(f"\n❌ ERROR ANALYSIS:")
        for error_type, count in sorted(error_types_summary.items(), key=lambda x: x[1], reverse=True):
            print(f"  • {error_type}: {count} occurrences")
    
    if dry_run:
        print(f"\n⚠️  This was a DRY RUN - no VMs were actually shutdown!")
        print(f"   Set DRY_RUN = False in the script to perform actual shutdown.")
    else:
        print(f"\n✅ Shutdown operations initiated - check GCP Console for progress")
        if total_shutdown_failed > 0:
            print(f"⚠️  {total_shutdown_failed} shutdown operations failed - check the errors sheet")
    
    return filename

def confirm_shutdown(total_projects: int, dry_run: bool) -> bool:
    """Ask for user confirmation before proceeding."""
    if dry_run:
        print(f"\n🔍 DRY RUN MODE - No VMs will be shutdown")
        print(f"   This will scan {total_projects} projects for running instances")
        return True
    
    print(f"\n⚠️  WARNING: ACTUAL SHUTDOWN MODE")
    print(f"   This will shutdown ALL running VMs across {total_projects} projects!")
    print(f"   Excluded projects: {len(EXCLUDED_PROJECTS)}")
    print(f"   Excluded instances: {len(EXCLUDED_INSTANCES)}")
    print(f"   Excluded zones: {len(EXCLUDED_ZONES)}")
    print(f"\n   This action cannot be easily undone!")
    
    response = input("\n   Type 'SHUTDOWN' to confirm: ")
    return response == 'SHUTDOWN'

def main():
    """Main function to orchestrate the VM shutdown operation with batch processing."""
    print(f"🛑 GCP VM Shutdown Script - Batch Processing")
    print(f"Billing Account: {BILLING_ACCOUNT_ID}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'ACTUAL SHUTDOWN'}")
    print(f"Batch Size: {BATCH_SIZE} projects per batch")
    print(f"Max Workers: {MAX_WORKERS}")
    
    try:
        # Validate environment and credentials
        logger.info("Validating environment and credentials...")
        
        # Get all projects under the billing account
        project_ids = get_projects_under_billing_account(BILLING_ACCOUNT_ID)
        
        if not project_ids:
            logger.error("No projects found or unable to access billing account")
            print("\n❌ No projects found! Possible issues:")
            print("   • Invalid billing account ID")
            print("   • No permissions to access billing account")
            print("   • All projects have disabled billing")
            print("   • All projects are in exclusion list")
            return
        
        # Calculate batch information
        total_projects = len(project_ids)
        total_batches = (total_projects + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division
        
        print(f"\n📋 BATCH PROCESSING PLAN:")
        print(f"   • Total Projects: {total_projects}")
        print(f"   • Batch Size: {BATCH_SIZE}")
        print(f"   • Total Batches: {total_batches}")
        print(f"   • Excluded Projects: {len(EXCLUDED_PROJECTS)}")
        print(f"   • Excluded Instances: {len(EXCLUDED_INSTANCES)}")
        print(f"   • Excluded Zones: {len(EXCLUDED_ZONES)}")
        
        # Confirm before proceeding
        if not confirm_shutdown(total_projects, DRY_RUN):
            print("Operation cancelled by user")
            return
        
        print(f"\n🚀 Starting batch processing of VM shutdown...")
        overall_start_time = datetime.datetime.now()
        
        # Process projects in batches
        all_results = []
        
        for batch_num in range(1, total_batches + 1):
            batch_start_idx = (batch_num - 1) * BATCH_SIZE
            batch_end_idx = min(batch_start_idx + BATCH_SIZE, total_projects)
            project_batch = project_ids[batch_start_idx:batch_end_idx]
            
            print(f"\n📦 Processing Batch {batch_num}/{total_batches}")
            print(f"   Projects {batch_start_idx + 1}-{batch_end_idx} of {total_projects}")
            
            try:
                # Process this batch
                batch_results = process_project_batch(project_batch, batch_num, total_batches, DRY_RUN)
                all_results.extend(batch_results)
                
                # Show batch summary
                batch_vms = sum(result['total_running'] for result in batch_results)
                batch_shutdown = sum(result['shutdown_successful'] for result in batch_results)
                batch_errors = sum(1 for result in batch_results if result.get('error'))
                
                print(f"   ✅ Batch {batch_num} completed: {batch_vms} VMs found, "
                      f"{batch_shutdown} shutdown operations, {batch_errors} errors")
                
                # Brief pause between batches to avoid overwhelming APIs
                if batch_num < total_batches:
                    time.sleep(2)
                    
            except KeyboardInterrupt:
                logger.warning(f"Operation interrupted by user during batch {batch_num}")
                print(f"\n⚠️  Operation interrupted during batch {batch_num}")
                print(f"   Processed {len(all_results)} projects so far")
                break
            except Exception as e:
                logger.error(f"Critical error processing batch {batch_num}: {type(e).__name__}: {e}")
                print(f"❌ Critical error in batch {batch_num}: {e}")
                
                # Create failure results for projects in this batch
                for project_id in project_batch:
                    all_results.append({
                        'project_id': project_id,
                        'status': 'batch_critical_error',
                        'error': f"Batch {batch_num} critical error: {str(e)}",
                        'error_type': type(e).__name__,
                        'running_instances': [],
                        'shutdown_results': [],
                        'total_running': 0,
                        'shutdown_attempted': 0,
                        'shutdown_successful': 0,
                        'shutdown_failed': 0,
                        'errors_by_type': {type(e).__name__: 1}
                    })
                continue
        
        # Calculate overall statistics
        overall_duration = datetime.datetime.now() - overall_start_time
        total_vms_found = sum(result['total_running'] for result in all_results)
        total_shutdown_successful = sum(result['shutdown_successful'] for result in all_results)
        total_shutdown_failed = sum(result.get('shutdown_failed', 0) for result in all_results)
        
        print(f"\n⏱️  Overall processing completed in {overall_duration}")
        print(f"📊 Final Summary:")
        print(f"   • Projects Processed: {len(all_results)}")
        print(f"   • VMs Found: {total_vms_found}")
        print(f"   • Shutdown Successful: {total_shutdown_successful}")
        print(f"   • Shutdown Failed: {total_shutdown_failed}")
        
        # Generate comprehensive report
        try:
            print(f"\n📄 Generating comprehensive Excel report...")
            report_file = generate_shutdown_instances_report(all_results, BILLING_ACCOUNT_ID, DRY_RUN)
            print(f"✅ Report generated: {report_file}")
        except Exception as e:
            logger.error(f"Failed to generate report: {type(e).__name__}: {e}")
            print(f"❌ Failed to generate report: {e}")
        
        if not DRY_RUN and total_shutdown_successful > 0:
            print(f"\n⚠️  {total_shutdown_successful} VMs are being shutdown across {len(all_results)} projects")
            print(f"💡 Check the GCP Console to monitor progress")
            print(f"💡 Operation IDs are available in the Excel report for tracking")
        
    except KeyboardInterrupt:
        logger.warning("Operation interrupted by user")
        print(f"\n⚠️  Operation interrupted by user (Ctrl+C)")
        print(f"   Some operations may still be in progress")
    except Exception as e:
        logger.error(f"Critical error in main function: {type(e).__name__}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        print(f"\n❌ Critical error: {e}")
        print(f"   Check the logs for detailed error information")

if __name__ == "__main__":
    main()
