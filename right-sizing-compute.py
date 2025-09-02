#!/usr/bin/env python3
"""
GCP VM Right-Sizing Script
Uses GCP Recommender APIs to analyze CPU and Memory utilization of compute VMs
and recommend smaller sizes for over-provisioned instances.
"""

import os
import datetime
import logging
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

# Google Cloud imports
from google.cloud import billing_v1, compute_v1, recommender_v1
from google.api_core import exceptions
import google.cloud.bigquery as bigquery

# --- Configuration ---
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
BATCH_SIZE = 20  # Larger batches for background processing
MAX_WORKERS = 10  # More workers for background processing
MAX_RETRIES = 3  # Maximum number of retries for API calls
RETRY_DELAY = 5  # Delay between retries in seconds
TOP_PROJECTS_LIMIT = None  # Analyze ALL projects (None = no limit)

# Recommender configuration
RECOMMENDER_ID = "google.compute.instance.MachineTypeRecommender"
INSIGHT_TYPE = "google.compute.instance.OvercommittedUtilization"

# Background processing configuration
ENABLE_BACKGROUND_MODE = True  # Enable background processing
BACKGROUND_LOG_FILE = f"vm_rightsizing_background_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
CHECKPOINT_INTERVAL = 50  # Save progress every N projects

# Machine type families for cost analysis
MACHINE_TYPE_FAMILIES = {
    'e2': {'cpu_cost_per_hour': 0.031611, 'memory_cost_per_gb_hour': 0.004237},
    'n1': {'cpu_cost_per_hour': 0.031611, 'memory_cost_per_gb_hour': 0.004237},
    'n2': {'cpu_cost_per_hour': 0.031611, 'memory_cost_per_gb_hour': 0.004237},
    'n2d': {'cpu_cost_per_hour': 0.028449, 'memory_cost_per_gb_hour': 0.003814},
    'c2': {'cpu_cost_per_hour': 0.033174, 'memory_cost_per_gb_hour': 0.004446},
    'c2d': {'cpu_cost_per_hour': 0.029851, 'memory_cost_per_gb_hour': 0.004003},
    'm1': {'cpu_cost_per_hour': 0.040136, 'memory_cost_per_gb_hour': 0.005379},
    'm2': {'cpu_cost_per_hour': 0.040136, 'memory_cost_per_gb_hour': 0.005379},
}

# Set up logging for background processing
if ENABLE_BACKGROUND_MODE:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(BACKGROUND_LOG_FILE),
            logging.StreamHandler()  # Also log to console
        ]
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
logger = logging.getLogger(__name__)

class VMRightSizingAnalyzer:
    """Analyzes VM utilization and provides right-sizing recommendations."""
    
    def __init__(self):
        """Initialize the analyzer with GCP clients."""
        self.billing_client = billing_v1.CloudBillingClient()
        self.compute_client = compute_v1.InstancesClient()
        self.machine_types_client = compute_v1.MachineTypesClient()
        self.recommender_client = recommender_v1.RecommenderClient()
        self.bigquery_client = bigquery.Client()
        self.recommendations = []
        self.processed_projects = 0
        self.total_potential_savings = 0.0
        
    def retry_api_call(self, func, *args, **kwargs):
        """Retry API calls with exponential backoff and comprehensive error handling."""
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except exceptions.PermissionDenied as e:
                logger.debug(f"Permission denied (non-retryable): {e}")
                raise e  # Don't retry permission errors
            except exceptions.NotFound as e:
                logger.debug(f"Resource not found (non-retryable): {e}")
                raise e  # Don't retry not found errors
            except exceptions.ServiceUnavailable as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Service unavailable after {MAX_RETRIES} attempts: {e}")
                    raise e
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Service unavailable (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            except exceptions.TooManyRequests as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Rate limit exceeded after {MAX_RETRIES} attempts: {e}")
                    raise e
                wait_time = RETRY_DELAY * (2 ** attempt) * 2  # Longer wait for rate limits
                logger.warning(f"Rate limit exceeded (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            except exceptions.GoogleAPICallError as e:
                if "SERVICE_DISABLED" in str(e) or "API has not been used" in str(e):
                    logger.debug(f"API disabled (non-retryable): {e}")
                    raise e  # Don't retry API disabled errors
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"API call failed after {MAX_RETRIES} attempts: {e}")
                    raise e
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"API call failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Unexpected error after {MAX_RETRIES} attempts: {e}")
                    raise e
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Unexpected error (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
        return None
    
    def get_projects_from_billing_account(self) -> List[str]:
        """Get all projects linked to the billing account."""
        logger.info(f"Fetching projects for billing account: {BILLING_ACCOUNT_ID}")
        
        try:
            billing_account_name = f"billingAccounts/{BILLING_ACCOUNT_ID}"
            request = billing_v1.ListProjectBillingInfoRequest(name=billing_account_name)
            
            projects = []
            page_result = self.retry_api_call(self.billing_client.list_project_billing_info, request=request)
            
            for project_billing_info in page_result:
                if project_billing_info.billing_enabled:
                    project_id = project_billing_info.name.split('/')[1]
                    projects.append(project_id)
            
            logger.info(f"Found {len(projects)} active projects")
            return projects
            
        except Exception as e:
            logger.error(f"Error fetching projects from billing account: {e}")
            return []
    
    def get_top_compute_cost_projects(self, all_projects: List[str]) -> List[Tuple[str, float]]:
        """Get top projects by compute engine costs using BigQuery billing export."""
        logger.info(f"Analyzing compute costs for {len(all_projects)} projects to identify top {TOP_PROJECTS_LIMIT}")
        
        # Try to query billing export data
        # Note: This requires billing export to be configured in BigQuery
        try:
            # First, try to find the billing export dataset
            billing_datasets = []
            for dataset in self.bigquery_client.list_datasets():
                if 'billing' in dataset.dataset_id.lower() or 'gcp' in dataset.dataset_id.lower():
                    billing_datasets.append(f"{dataset.project}.{dataset.dataset_id}")
            
            if not billing_datasets:
                logger.warning("No billing export dataset found, using alternative method")
                return self.get_projects_with_compute_instances(all_projects)
            
            # Query the billing export for compute costs (last 30 days)
            query = f"""
            SELECT 
                project.id as project_id,
                SUM(cost) as total_compute_cost
            FROM `{billing_datasets[0]}.gcp_billing_export_v1_{BILLING_ACCOUNT_ID.replace('-', '_')}`
            WHERE 
                service.description LIKE '%Compute Engine%'
                AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                AND project.id IN UNNEST(@project_list)
            GROUP BY project.id
            ORDER BY total_compute_cost DESC
            LIMIT {TOP_PROJECTS_LIMIT}
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("project_list", "STRING", all_projects)
                ]
            )
            
            query_job = self.bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            
            top_projects = []
            for row in results:
                top_projects.append((row.project_id, float(row.total_compute_cost)))
                
            logger.info(f"Found {len(top_projects)} projects with compute costs from billing data")
            return top_projects
            
        except Exception as e:
            logger.warning(f"Could not query billing data: {e}, using alternative method")
            return self.get_projects_with_compute_instances(all_projects)
    
    def get_projects_with_compute_instances(self, all_projects: List[str]) -> List[Tuple[str, float]]:
        """Alternative method: Get projects that have compute instances and estimate their priority."""
        logger.info(f"Using alternative method to identify projects with compute instances")
        
        projects_with_instances = []
        
        def check_project_instances(project_id: str) -> Tuple[str, int]:
            """Check if project has compute instances and count them."""
            try:
                # List all instances across all zones
                request = compute_v1.AggregatedListInstancesRequest(project=project_id)
                instance_count = 0
                
                page_result = self.retry_api_call(
                    self.compute_client.aggregated_list,
                    request=request
                )
                
                for zone, instances_scoped_list in page_result:
                    if instances_scoped_list.instances:
                        for instance in instances_scoped_list.instances:
                            if instance.status == "RUNNING":
                                instance_count += 1
                
                return (project_id, instance_count)
                
            except exceptions.PermissionDenied:
                logger.debug(f"Permission denied for project {project_id}")
                return (project_id, 0)
            except Exception as e:
                logger.debug(f"Error checking instances for project {project_id}: {e}")
                return (project_id, 0)
        
        # Use threading to check projects in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_project = {
                executor.submit(check_project_instances, project_id): project_id 
                for project_id in all_projects
            }
            
            for future in as_completed(future_to_project):
                try:
                    project_id, instance_count = future.result()
                    if instance_count > 0:
                        # Estimate cost based on instance count (rough approximation)
                        estimated_cost = instance_count * 50  # $50/month per instance estimate
                        projects_with_instances.append((project_id, estimated_cost))
                        logger.info(f"Project {project_id}: {instance_count} running instances")
                except Exception as e:
                    logger.warning(f"Error processing project: {e}")
        
        # Sort by estimated cost and return top projects
        projects_with_instances.sort(key=lambda x: x[1], reverse=True)
        top_projects = projects_with_instances[:TOP_PROJECTS_LIMIT]
        
        logger.info(f"Selected top {len(top_projects)} projects with running instances")
        return top_projects
    
    def get_machine_type_details(self, project_id: str, zone: str, machine_type_name: str) -> Optional[Dict]:
        """Get machine type details including vCPUs and memory."""
        try:
            machine_type = self.retry_api_call(
                self.machine_types_client.get,
                project=project_id,
                zone=zone,
                machine_type=machine_type_name
            )
            
            return {
                'name': machine_type.name,
                'vcpus': machine_type.guest_cpus,
                'memory_gb': machine_type.memory_mb / 1024,
                'description': machine_type.description
            }
        except Exception as e:
            logger.warning(f"Could not get machine type details for {machine_type_name}: {e}")
            return None
    
    def parse_machine_type_from_url(self, machine_type_url: str) -> str:
        """Extract machine type name from URL."""
        return machine_type_url.split('/')[-1]
    
    def extract_zone_from_url(self, zone_url: str) -> str:
        """Extract zone name from URL."""
        return zone_url.split('/')[-1]
    
    def estimate_monthly_cost(self, machine_type: str, zone: str = None, hours_per_month: int = 730) -> float:
        """Estimate monthly cost for a machine type."""
        try:
            # Extract machine family and specs
            machine_family = machine_type.split('-')[0] if machine_type else 'e2'
            
            # Get vCPUs and memory from machine type name
            vcpus, memory_gb = self.get_machine_type_specs(machine_type)
            
            # Get cost factors for the machine family
            cost_factors = MACHINE_TYPE_FAMILIES.get(machine_family, MACHINE_TYPE_FAMILIES['e2'])
            
            # Calculate monthly cost
            monthly_cost = (
                vcpus * cost_factors['cpu_cost_per_hour'] * hours_per_month +
                memory_gb * cost_factors['memory_cost_per_gb_hour'] * hours_per_month
            )
            
            return monthly_cost
            
        except Exception as e:
            logger.debug(f"Error estimating cost for {machine_type}: {e}")
            return 0.0
    
    def get_machine_type_specs(self, machine_type: str) -> tuple:
        """Extract vCPUs and memory from machine type name."""
        try:
            # Common GCP machine type patterns
            import re
            
            # Standard machine types (e.g., n1-standard-4, n2-standard-8)
            standard_match = re.match(r'[a-z]+\d*-standard-(\d+)', machine_type)
            if standard_match:
                vcpus = int(standard_match.group(1))
                memory_gb = vcpus * 3.75  # Standard ratio
                return vcpus, memory_gb
            
            # High memory types (e.g., n1-highmem-4, n2-highmem-8)
            highmem_match = re.match(r'[a-z]+\d*-highmem-(\d+)', machine_type)
            if highmem_match:
                vcpus = int(highmem_match.group(1))
                memory_gb = vcpus * 6.5  # High memory ratio
                return vcpus, memory_gb
            
            # High CPU types (e.g., n1-highcpu-4, n2-highcpu-8)
            highcpu_match = re.match(r'[a-z]+\d*-highcpu-(\d+)', machine_type)
            if highcpu_match:
                vcpus = int(highcpu_match.group(1))
                memory_gb = vcpus * 0.9  # High CPU ratio
                return vcpus, memory_gb
            
            # Custom machine types (e.g., custom-4-8192)
            custom_match = re.match(r'custom-(\d+)-(\d+)', machine_type)
            if custom_match:
                vcpus = int(custom_match.group(1))
                memory_mb = int(custom_match.group(2))
                memory_gb = round(memory_mb / 1024, 2)
                return vcpus, memory_gb
            
            # E2 types (e.g., e2-standard-4, e2-medium, e2-small)
            if machine_type.startswith('e2-'):
                if 'micro' in machine_type:
                    return 1, 1
                elif 'small' in machine_type:
                    return 1, 2
                elif 'medium' in machine_type:
                    return 1, 4
                else:
                    e2_match = re.match(r'e2-\w+-(\d+)', machine_type)
                    if e2_match:
                        vcpus = int(e2_match.group(1))
                        memory_gb = vcpus * 4  # E2 standard ratio
                        return vcpus, memory_gb
            
            # Default fallback - try to extract numbers
            numbers = re.findall(r'\d+', machine_type)
            if numbers:
                vcpus = int(numbers[-1])  # Last number is usually vCPUs
                memory_gb = vcpus * 3.75  # Default standard ratio
                return vcpus, memory_gb
            
            return 0, 0
            
        except Exception as e:
            logger.debug(f"Error parsing machine type {machine_type}: {e}")
            return 0, 0

    def calculate_cost_savings(self, current_machine: Dict, recommended_machine: Dict, 
                             hours_per_month: int = 730) -> float:
        """Calculate potential monthly cost savings."""
        # Extract machine family from machine type name
        current_family = current_machine['name'].split('-')[0]
        recommended_family = recommended_machine['name'].split('-')[0]
        
        # Get cost factors
        current_costs = MACHINE_TYPE_FAMILIES.get(current_family, MACHINE_TYPE_FAMILIES['e2'])
        recommended_costs = MACHINE_TYPE_FAMILIES.get(recommended_family, MACHINE_TYPE_FAMILIES['e2'])
        
        # Calculate monthly costs
        current_monthly_cost = (
            current_machine['vcpus'] * current_costs['cpu_cost_per_hour'] * hours_per_month +
            current_machine['memory_gb'] * current_costs['memory_cost_per_gb_hour'] * hours_per_month
        )
        
        recommended_monthly_cost = (
            recommended_machine['vcpus'] * recommended_costs['cpu_cost_per_hour'] * hours_per_month +
            recommended_machine['memory_gb'] * recommended_costs['memory_cost_per_gb_hour'] * hours_per_month
        )
        
        return current_monthly_cost - recommended_monthly_cost
    
    def save_checkpoint(self, recommendations: List[Dict], processed: int, total: int):
        """Save progress checkpoint for background processing."""
        try:
            checkpoint_data = {
                'timestamp': datetime.datetime.now().isoformat(),
                'processed_projects': processed,
                'total_projects': total,
                'recommendations_count': len(recommendations),
                'total_potential_savings': sum(r['estimated_monthly_savings_usd'] for r in recommendations),
                'progress_percentage': (processed / total) * 100,
                'recommendations': recommendations
            }
            
            checkpoint_file = f"vm_rightsizing_checkpoint_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
            
            logger.info(f"💾 Checkpoint saved: {checkpoint_file} - {processed}/{total} projects processed ({(processed/total)*100:.1f}%)")
            
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")
    
    def get_vm_recommendations(self, project_id: str) -> List[Dict]:
        """Get VM right-sizing recommendations for a project."""
        recommendations = []
        
        try:
            # Get recommendations for the project
            parent = f"projects/{project_id}/locations/global/recommenders/{RECOMMENDER_ID}"
            
            request = recommender_v1.ListRecommendationsRequest(
                parent=parent,
                filter='recommenderSubtype="UNDERUTILIZED_VM"'
            )
            
            page_result = self.retry_api_call(
                self.recommender_client.list_recommendations,
                request=request
            )
            
            for recommendation in page_result:
                try:
                    # Parse recommendation content
                    content = recommendation.content
                    operation_groups = content.operation_groups
                    
                    for operation_group in operation_groups:
                        for operation in operation_group.operations:
                            if operation.action == "replace":
                                resource_name = operation.resource
                                
                                # Extract instance details from resource name
                                # Format: //compute.googleapis.com/projects/{project}/zones/{zone}/instances/{instance}
                                path_parts = resource_name.split('/')
                                if len(path_parts) >= 8:
                                    instance_project = path_parts[4]
                                    zone = path_parts[6]
                                    instance_name = path_parts[7]
                                    
                                    # Get current instance details
                                    try:
                                        instance = self.retry_api_call(
                                            self.compute_client.get,
                                            project=instance_project,
                                            zone=zone,
                                            instance=instance_name
                                        )
                                        
                                        if instance.status == "RUNNING":
                                            current_machine_type = self.parse_machine_type_from_url(instance.machine_type)
                                            current_machine_details = self.get_machine_type_details(
                                                instance_project, zone, current_machine_type
                                            )
                                            
                                            # Extract recommended machine type from operation
                                            if operation.value and 'machineType' in operation.value:
                                                recommended_machine_type = operation.value['machineType'].split('/')[-1]
                                                recommended_machine_details = self.get_machine_type_details(
                                                    instance_project, zone, recommended_machine_type
                                                )
                                                
                                                if current_machine_details and recommended_machine_details:
                                                    # Calculate potential savings
                                                    monthly_savings = self.calculate_cost_savings(
                                                        current_machine_details, recommended_machine_details
                                                    )
                                                    
                                                    # Extract utilization insights with enhanced pattern detection
                                                    cpu_utilization = "N/A"
                                                    memory_utilization = "N/A"
                                                    
                                                    # Enhanced insight extraction from recommendation insights
                                                    if hasattr(recommendation, 'associated_insights'):
                                                        for insight_ref in recommendation.associated_insights:
                                                            try:
                                                                # Get full insight details
                                                                insight_name = insight_ref.insight
                                                                insight_request = recommender_v1.GetInsightRequest(name=insight_name)
                                                                insight = self.retry_api_call(
                                                                    self.recommender_client.get_insight,
                                                                    request=insight_request
                                                                )
                                                                
                                                                # Parse insight content for utilization data
                                                                insight_content = insight.content
                                                                description = insight.description.lower()
                                                                
                                                                # Extract CPU utilization
                                                                import re
                                                                if 'cpu' in description:
                                                                    cpu_pattern = r'(\d+\.?\d*)%?\s*cpu|cpu.*?(\d+\.?\d*)%'
                                                                    cpu_match = re.search(cpu_pattern, description)
                                                                    if cpu_match:
                                                                        cpu_val = cpu_match.group(1) or cpu_match.group(2)
                                                                        cpu_utilization = f"{cpu_val}% avg utilization"
                                                                    elif 'low' in description and 'cpu' in description:
                                                                        cpu_utilization = "Low utilization (< 20%)"
                                                                    elif 'high' in description and 'cpu' in description:
                                                                        cpu_utilization = "High utilization (> 80%)"
                                                                    elif 'under' in description and 'cpu' in description:
                                                                        cpu_utilization = "Under-utilized"
                                                                
                                                                # Extract memory utilization
                                                                if 'memory' in description:
                                                                    mem_pattern = r'(\d+\.?\d*)%?\s*memory|memory.*?(\d+\.?\d*)%'
                                                                    mem_match = re.search(mem_pattern, description)
                                                                    if mem_match:
                                                                        mem_val = mem_match.group(1) or mem_match.group(2)
                                                                        memory_utilization = f"{mem_val}% avg utilization"
                                                                    elif 'low' in description and 'memory' in description:
                                                                        memory_utilization = "Low utilization (< 20%)"
                                                                    elif 'high' in description and 'memory' in description:
                                                                        memory_utilization = "High utilization (> 80%)"
                                                                    elif 'under' in description and 'memory' in description:
                                                                        memory_utilization = "Under-utilized"
                                                                
                                                            except Exception as insight_error:
                                                                logger.debug(f"Error extracting insight details: {insight_error}")
                                                    
                                                    # Fallback: Extract from recommendation description
                                                    if cpu_utilization == "N/A" or memory_utilization == "N/A":
                                                        desc_lower = recommendation.description.lower()
                                                        
                                                        if cpu_utilization == "N/A":
                                                            if 'cpu' in desc_lower:
                                                                import re
                                                                cpu_pattern = r'(\d+\.?\d*)%?\s*cpu|cpu.*?(\d+\.?\d*)%'
                                                                cpu_match = re.search(cpu_pattern, desc_lower)
                                                                if cpu_match:
                                                                    cpu_val = cpu_match.group(1) or cpu_match.group(2)
                                                                    cpu_utilization = f"{cpu_val}% avg utilization"
                                                                elif 'underutilized' in desc_lower or 'under-utilized' in desc_lower:
                                                                    cpu_utilization = "Under-utilized"
                                                                elif 'low' in desc_lower:
                                                                    cpu_utilization = "Low utilization detected"
                                                        
                                                        if memory_utilization == "N/A":
                                                            if 'memory' in desc_lower:
                                                                import re
                                                                mem_pattern = r'(\d+\.?\d*)%?\s*memory|memory.*?(\d+\.?\d*)%'
                                                                mem_match = re.search(mem_pattern, desc_lower)
                                                                if mem_match:
                                                                    mem_val = mem_match.group(1) or mem_match.group(2)
                                                                    memory_utilization = f"{mem_val}% avg utilization"
                                                                elif 'underutilized' in desc_lower or 'under-utilized' in desc_lower:
                                                                    memory_utilization = "Under-utilized"
                                                                elif 'low' in desc_lower:
                                                                    memory_utilization = "Low utilization detected"
                                                    
                                                    recommendation_data = {
                                                        'project_id': instance_project,
                                                        'zone': zone,
                                                        'instance_name': instance_name,
                                                        'current_machine_type': current_machine_type,
                                                        'current_vcpus': current_machine_details['vcpus'],
                                                        'current_memory_gb': current_machine_details['memory_gb'],
                                                        'recommended_machine_type': recommended_machine_type,
                                                        'recommended_vcpus': recommended_machine_details['vcpus'],
                                                        'recommended_memory_gb': recommended_machine_details['memory_gb'],
                                                        'cpu_utilization': cpu_utilization,
                                                        'memory_utilization': memory_utilization,
                                                        'estimated_monthly_savings_usd': round(monthly_savings, 2),
                                                        'recommendation_priority': recommendation.priority.name,
                                                        'recommendation_description': recommendation.description,
                                                        'last_refresh_time': recommendation.last_refresh_time.strftime('%Y-%m-%d %H:%M:%S') if recommendation.last_refresh_time else 'N/A'
                                                    }
                                                    
                                                    recommendations.append(recommendation_data)
                                                    logger.info(f"  Found recommendation for {instance_name}: {current_machine_type} -> {recommended_machine_type} (${monthly_savings:.2f}/month savings)")
                                    
                                    except Exception as e:
                                        logger.warning(f"Error processing instance {instance_name}: {e}")
                                        continue
                
                except Exception as e:
                    logger.warning(f"Error processing recommendation: {e}")
                    continue
            
        except exceptions.PermissionDenied:
            logger.warning(f"Permission denied for recommendations in project {project_id}")
        except exceptions.NotFound:
            logger.warning(f"Recommender API not available for project {project_id}")
        except Exception as e:
            logger.warning(f"Error getting recommendations for project {project_id}: {e}")
        
        return recommendations
    
    def process_project_batch(self, projects: List[str]) -> List[Dict]:
        """Process a batch of projects for recommendations with enhanced error handling."""
        batch_recommendations = []
        successful_projects = 0
        failed_projects = 0
        
        logger.info(f"Starting batch processing for {len(projects)} projects")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_project = {
                executor.submit(self.get_vm_recommendations_safe, project_id): project_id 
                for project_id in projects
            }
            
            for future in as_completed(future_to_project):
                project_id = future_to_project[future]
                try:
                    recommendations = future.result(timeout=120)  # 2-minute timeout per project
                    if recommendations is not None:
                        batch_recommendations.extend(recommendations)
                        successful_projects += 1
                        if recommendations:
                            logger.info(f"✅ Project {project_id} ({self.processed_projects + successful_projects} total) - Found {len(recommendations)} recommendations")
                        else:
                            logger.debug(f"✅ Project {project_id} ({self.processed_projects + successful_projects} total) - No recommendations found")
                    else:
                        failed_projects += 1
                        logger.warning(f"❌ Project {project_id} - Failed to get recommendations")
                except Exception as e:
                    failed_projects += 1
                    logger.error(f"❌ Project {project_id} - Error: {e}")
        
        self.processed_projects += successful_projects
        logger.info(f"Batch completed: {successful_projects} successful, {failed_projects} failed")
        return batch_recommendations
    
    def get_vm_recommendations_safe(self, project_id: str) -> Optional[List[Dict]]:
        """Safely get VM recommendations for a project with comprehensive error handling."""
        try:
            return self.get_vm_recommendations(project_id)
        except exceptions.PermissionDenied:
            logger.debug(f"Permission denied for project {project_id}")
            return []
        except exceptions.NotFound:
            logger.debug(f"Recommender API not available for project {project_id}")
            return []
        except Exception as e:
            if "SERVICE_DISABLED" in str(e) or "API has not been used" in str(e):
                logger.debug(f"Recommender API disabled for project {project_id}")
                return []
            else:
                logger.warning(f"Error getting recommendations for project {project_id}: {e}")
                return None  # Return None to indicate failure vs empty recommendations
    
    def analyze_all_projects(self) -> List[Dict]:
        """Analyze all projects under the billing account for VM right-sizing opportunities."""
        logger.info("Starting comprehensive VM right-sizing analysis across all projects...")
        
        try:
            # Get all projects
            all_projects = self.get_projects_from_billing_account()
            if not all_projects:
                logger.error("No projects found or error accessing billing account")
                return []

            # Get projects with compute instances (all or top N based on configuration)
            if TOP_PROJECTS_LIMIT:
                logger.info(f"Identifying top {TOP_PROJECTS_LIMIT} projects with highest compute costs...")
                target_projects_with_costs = self.get_top_compute_cost_projects(all_projects)
                analysis_scope = f"top {TOP_PROJECTS_LIMIT} projects"
            else:
                logger.info("Analyzing ALL projects with compute instances...")
                target_projects_with_costs = self.get_top_compute_cost_projects(all_projects)
                analysis_scope = "all projects"
            
            if not target_projects_with_costs:
                logger.error("No projects with compute costs found")
                return []
            
            # Extract just the project IDs
            target_projects = [project_id for project_id, cost in target_projects_with_costs]
            
            logger.info(f"🎯 Focusing analysis on {analysis_scope} with compute instances")
            logger.info(f"📊 Total projects to analyze: {len(target_projects)}")
            
            # Log the top projects with their costs (first 10)
            logger.info("Top projects by compute cost:")
            for i, (project_id, cost) in enumerate(target_projects_with_costs[:10], 1):
                logger.info(f"  {i}. {project_id}: ${cost:.2f}")
            
            if len(target_projects_with_costs) > 10:
                logger.info(f"  ... and {len(target_projects_with_costs) - 10} more projects")
            
            all_recommendations = []
            total_batches = (len(target_projects) + BATCH_SIZE - 1) // BATCH_SIZE
            
            # Background processing with checkpoints
            start_time = time.time()
            processed_projects = 0
            
            # Process projects in batches with enhanced error recovery
            for i in range(0, len(target_projects), BATCH_SIZE):
                batch = target_projects[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                
                logger.info(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} projects)")
                if ENABLE_BACKGROUND_MODE:
                    # In background mode, log less verbose project lists
                    logger.info(f"Batch contains projects: {len(batch)} projects starting with {batch[0]}")
                else:
                    logger.info(f"Batch projects: {', '.join(batch)}")
                
                try:
                    batch_recommendations = self.process_project_batch(batch)
                    all_recommendations.extend(batch_recommendations)
                    processed_projects += len(batch)
                    
                    # Calculate running total of potential savings
                    batch_savings = sum(rec['estimated_monthly_savings_usd'] for rec in batch_recommendations)
                    self.total_potential_savings += batch_savings
                    
                    # Progress reporting
                    elapsed_time = time.time() - start_time
                    progress_pct = (batch_num / total_batches) * 100
                    estimated_total_time = elapsed_time / progress_pct * 100 if progress_pct > 0 else 0
                    eta = estimated_total_time - elapsed_time
                    
                    logger.info(f"✅ Batch {batch_num}/{total_batches} completed ({progress_pct:.1f}%)")
                    logger.info(f"   Found {len(batch_recommendations)} recommendations, ${batch_savings:.2f} potential monthly savings")
                    logger.info(f"   Total processed: {processed_projects}/{len(target_projects)} projects")
                    logger.info(f"   Elapsed: {elapsed_time/60:.1f}m, ETA: {eta/60:.1f}m")
                    
                    # Checkpoint progress periodically
                    if ENABLE_BACKGROUND_MODE and processed_projects % CHECKPOINT_INTERVAL == 0:
                        self.save_checkpoint(all_recommendations, processed_projects, len(target_projects))
                    
                    # Brief pause between batches to avoid overwhelming APIs
                    if batch_num < total_batches:
                        pause_time = 2 if ENABLE_BACKGROUND_MODE else 3
                        logger.info(f"Pausing {pause_time} seconds before next batch...")
                        time.sleep(pause_time)
                        
                except Exception as e:
                    logger.error(f"❌ Batch {batch_num} failed: {e}")
                    logger.info("Continuing with next batch...")
                    continue
            
            self.recommendations = all_recommendations
            self.processed_projects = processed_projects
            
            total_time = time.time() - start_time
            logger.info(f"\n🎉 Analysis completed! Total time: {total_time/60:.1f} minutes")
            logger.info(f"📊 Total recommendations: {len(all_recommendations)}")
            logger.info(f"💰 Total potential monthly savings: ${self.total_potential_savings:.2f}")
            logger.info(f"📈 Average savings per recommendation: ${self.total_potential_savings/len(all_recommendations):.2f}" if all_recommendations else "No recommendations found")
            
            return all_recommendations
            
        except Exception as e:
            logger.error(f"Critical error in analysis: {e}")
            return []
    
    def generate_recommendations_report(self) -> str:
        """Generate comprehensive Excel report with detailed VM right-sizing recommendations."""
        if not self.recommendations:
            logger.warning("No recommendations to report")
            return ""
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"vm_rightsizing_recommendations_{BILLING_ACCOUNT_ID}_{timestamp}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main recommendations sheet with enhanced details
                df_recommendations = pd.DataFrame(self.recommendations)
                
                # Reorder and enhance columns for better readability
                if not df_recommendations.empty:
                    column_order = [
                        'project_id', 'zone', 'instance_name',
                        'current_machine_type', 'current_vcpus', 'current_memory_gb',
                        'recommended_machine_type', 'recommended_vcpus', 'recommended_memory_gb',
                        'cpu_utilization', 'memory_utilization',
                        'estimated_monthly_savings_usd', 'recommendation_priority',
                        'recommendation_description', 'last_refresh_time'
                    ]
                    
                    # Ensure all columns exist
                    for col in column_order:
                        if col not in df_recommendations.columns:
                            df_recommendations[col] = 'N/A'
                    
                    df_recommendations = df_recommendations[column_order]
                    
                    # Add calculated fields
                    df_recommendations['cpu_reduction_percent'] = (
                        (df_recommendations['current_vcpus'] - df_recommendations['recommended_vcpus']) 
                        / df_recommendations['current_vcpus'] * 100
                    ).round(1)
                    
                    df_recommendations['memory_reduction_percent'] = (
                        (df_recommendations['current_memory_gb'] - df_recommendations['recommended_memory_gb']) 
                        / df_recommendations['current_memory_gb'] * 100
                    ).round(1)
                    
                    df_recommendations['annual_savings_usd'] = (
                        df_recommendations['estimated_monthly_savings_usd'] * 12
                    ).round(2)
                    
                    # Rename columns for better readability
                    df_recommendations = df_recommendations.rename(columns={
                        'project_id': 'Project ID',
                        'zone': 'Zone',
                        'instance_name': 'VM Instance Name',
                        'current_machine_type': 'Current Instance Type',
                        'current_vcpus': 'Current vCPUs',
                        'current_memory_gb': 'Current Memory (GB)',
                        'recommended_machine_type': 'Recommended Instance Type',
                        'recommended_vcpus': 'Recommended vCPUs',
                        'recommended_memory_gb': 'Recommended Memory (GB)',
                        'cpu_utilization': 'CPU Utilization Pattern',
                        'memory_utilization': 'Memory Utilization Pattern',
                        'cpu_reduction_percent': 'CPU Reduction (%)',
                        'memory_reduction_percent': 'Memory Reduction (%)',
                        'estimated_monthly_savings_usd': 'Monthly Savings (USD)',
                        'annual_savings_usd': 'Annual Savings (USD)',
                        'recommendation_priority': 'Priority',
                        'recommendation_description': 'Recommendation Details',
                        'last_refresh_time': 'Last Analysis Date'
                    })
                
                df_recommendations.to_excel(writer, sheet_name='VM_Recommendations', index=False)
                
                # Summary sheet with key metrics
                total_current_vcpus = sum(self.recommendations[i]['current_vcpus'] for i in range(len(self.recommendations)))
                total_recommended_vcpus = sum(self.recommendations[i]['recommended_vcpus'] for i in range(len(self.recommendations)))
                total_current_memory = sum(self.recommendations[i]['current_memory_gb'] for i in range(len(self.recommendations)))
                total_recommended_memory = sum(self.recommendations[i]['recommended_memory_gb'] for i in range(len(self.recommendations)))
                
                summary_data = {
                    'Metric': [
                        'Analysis Date',
                        'Billing Account',
                        'Total Projects Analyzed',
                        'Total VM Instances with Recommendations',
                        'Total Potential Monthly Savings (USD)',
                        'Total Potential Annual Savings (USD)',
                        'Average Monthly Savings per VM (USD)',
                        'Average Annual Savings per VM (USD)',
                        '',
                        'Resource Optimization Summary:',
                        'Total Current vCPUs',
                        'Total Recommended vCPUs',
                        'Total vCPU Reduction',
                        'vCPU Reduction Percentage',
                        'Total Current Memory (GB)',
                        'Total Recommended Memory (GB)',
                        'Total Memory Reduction (GB)',
                        'Memory Reduction Percentage',
                        '',
                        'Priority Breakdown:',
                        'High Priority Recommendations (P1)',
                        'Medium Priority Recommendations (P2)',
                        'Low Priority Recommendations (P3)',
                        'Unknown Priority',
                        '',
                        'Top Instance Types for Right-sizing:',
                        f"Most over-provisioned type: {self._get_most_common_current_type()}",
                        f"Most recommended type: {self._get_most_common_recommended_type()}",
                    ],
                    'Value': [
                        datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        BILLING_ACCOUNT_ID,
                        self.processed_projects,
                        len(self.recommendations),
                        f"${self.total_potential_savings:.2f}",
                        f"${self.total_potential_savings * 12:.2f}",
                        f"${self.total_potential_savings / len(self.recommendations):.2f}" if self.recommendations else "$0.00",
                        f"${(self.total_potential_savings * 12) / len(self.recommendations):.2f}" if self.recommendations else "$0.00",
                        '',
                        '',
                        total_current_vcpus,
                        total_recommended_vcpus,
                        total_current_vcpus - total_recommended_vcpus,
                        f"{((total_current_vcpus - total_recommended_vcpus) / total_current_vcpus * 100):.1f}%" if total_current_vcpus > 0 else "0%",
                        f"{total_current_memory:.1f}",
                        f"{total_recommended_memory:.1f}",
                        f"{total_current_memory - total_recommended_memory:.1f}",
                        f"{((total_current_memory - total_recommended_memory) / total_current_memory * 100):.1f}%" if total_current_memory > 0 else "0%",
                        '',
                        '',
                        len([r for r in self.recommendations if r.get('recommendation_priority') == 'P1']),
                        len([r for r in self.recommendations if r.get('recommendation_priority') == 'P2']),
                        len([r for r in self.recommendations if r.get('recommendation_priority') == 'P3']),
                        len([r for r in self.recommendations if r.get('recommendation_priority', '') not in ['P1', 'P2', 'P3']]),
                        '',
                        '',
                        '',
                        '',
                    ]
                }
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Executive_Summary', index=False)
                
                # Top savings opportunities (top 20)
                if self.recommendations:
                    df_top_savings = df_recommendations.nlargest(20, 'Monthly Savings (USD)')
                    df_top_savings.to_excel(writer, sheet_name='Top_Savings_Opportunities', index=False)
                
                # Instance type analysis
                if self.recommendations:
                    instance_analysis_data = []
                    
                    # Group by current instance type
                    from collections import defaultdict
                    type_groups = defaultdict(list)
                    for rec in self.recommendations:
                        type_groups[rec['current_machine_type']].append(rec)
                    
                    for instance_type, recs in type_groups.items():
                        total_instances = len(recs)
                        total_savings = sum(r['estimated_monthly_savings_usd'] for r in recs)
                        avg_savings = total_savings / total_instances
                        total_current_vcpus = sum(r['current_vcpus'] for r in recs)
                        total_recommended_vcpus = sum(r['recommended_vcpus'] for r in recs)
                        
                        # Most common recommendation for this type
                        recommended_types = [r['recommended_machine_type'] for r in recs]
                        most_common_rec = max(set(recommended_types), key=recommended_types.count)
                        
                        instance_analysis_data.append({
                            'Current Instance Type': instance_type,
                            'Number of Instances': total_instances,
                            'Total Monthly Savings (USD)': round(total_savings, 2),
                            'Average Savings per Instance (USD)': round(avg_savings, 2),
                            'Total Current vCPUs': total_current_vcpus,
                            'Total Recommended vCPUs': total_recommended_vcpus,
                            'vCPU Reduction': total_current_vcpus - total_recommended_vcpus,
                            'Most Common Recommendation': most_common_rec,
                            'Optimization Potential': 'High' if avg_savings > 100 else 'Medium' if avg_savings > 50 else 'Low'
                        })
                    
                    df_instance_analysis = pd.DataFrame(instance_analysis_data)
                    df_instance_analysis = df_instance_analysis.sort_values('Total Monthly Savings (USD)', ascending=False)
                    df_instance_analysis.to_excel(writer, sheet_name='Instance_Type_Analysis', index=False)
                
                # Project-wise analysis
                if self.recommendations:
                    project_analysis_data = []
                    
                    # Group by project
                    project_groups = defaultdict(list)
                    for rec in self.recommendations:
                        project_groups[rec['project_id']].append(rec)
                    
                    for project_id, recs in project_groups.items():
                        total_instances = len(recs)
                        total_savings = sum(r['estimated_monthly_savings_usd'] for r in recs)
                        high_priority = len([r for r in recs if r.get('recommendation_priority') == 'P1'])
                        
                        project_analysis_data.append({
                            'Project ID': project_id,
                            'Number of Instances': total_instances,
                            'Total Monthly Savings (USD)': round(total_savings, 2),
                            'High Priority Recommendations': high_priority,
                            'Optimization Priority': 'Critical' if high_priority > 0 and total_savings > 500 
                                                  else 'High' if total_savings > 200 
                                                  else 'Medium' if total_savings > 50 
                                                  else 'Low'
                        })
                    
                    df_project_analysis = pd.DataFrame(project_analysis_data)
                    df_project_analysis = df_project_analysis.sort_values('Total Monthly Savings (USD)', ascending=False)
                    df_project_analysis.to_excel(writer, sheet_name='Project_Analysis', index=False)
                
                # Implementation guide
                implementation_guide = [
                    ['Step', 'Action', 'Description', 'Considerations'],
                    ['1', 'Review Recommendations', 'Analyze the VM_Recommendations sheet for detailed insights', 'Focus on high-priority recommendations first'],
                    ['2', 'Validate Current Usage', 'Verify current CPU and memory utilization patterns', 'Check monitoring data for the past 30 days'],
                    ['3', 'Plan Maintenance Windows', 'Schedule downtime for instance type changes', 'VMs need to be stopped to change machine type'],
                    ['4', 'Test in Non-Production', 'Apply recommendations to dev/test environments first', 'Validate application performance with new sizes'],
                    ['5', 'Implement Gradually', 'Roll out changes in batches', 'Start with lowest-risk, highest-savings opportunities'],
                    ['6', 'Monitor Performance', 'Track application performance after changes', 'Ensure no performance degradation'],
                    ['7', 'Measure Savings', 'Calculate actual cost savings achieved', 'Compare billing before and after implementation'],
                    ['', '', '', ''],
                    ['Important Notes:', '', '', ''],
                    ['• VM Shutdown Required', 'VMs must be stopped to change machine types', '', ''],
                    ['• Disk Compatibility', 'Ensure disk sizes are compatible with new machine types', '', ''],
                    ['• Network Performance', 'Some machine types have different network performance', '', ''],
                    ['• Licensing', 'Verify software licensing compatibility with new configurations', '', ''],
                    ['• Monitoring', 'Set up alerts for CPU/memory usage post-implementation', '', ''],
                ]
                
                df_guide = pd.DataFrame(implementation_guide[1:], columns=implementation_guide[0])
                df_guide.to_excel(writer, sheet_name='Implementation_Guide', index=False)
            
            logger.info(f"Comprehensive report generated: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return ""
    
    def _get_most_common_current_type(self) -> str:
        """Get the most common current machine type."""
        if not self.recommendations:
            return "N/A"
        current_types = [r['current_machine_type'] for r in self.recommendations]
        return max(set(current_types), key=current_types.count)
    
    def _get_most_common_recommended_type(self) -> str:
        """Get the most common recommended machine type."""
        if not self.recommendations:
            return "N/A"
        recommended_types = [r['recommended_machine_type'] for r in self.recommendations]
        return max(set(recommended_types), key=recommended_types.count)

def main():
    """Main function to run the VM right-sizing analysis."""
    analysis_scope = f"Top {TOP_PROJECTS_LIMIT} Projects" if TOP_PROJECTS_LIMIT else "ALL Projects"
    mode = "Background Processing" if ENABLE_BACKGROUND_MODE else "Interactive Mode"
    
    print(f"🔍 GCP VM Right-Sizing Analysis - {analysis_scope}")
    print("=" * 60)
    print(f"Billing Account: {BILLING_ACCOUNT_ID}")
    print(f"Analysis Scope: {analysis_scope} with Compute Instances")
    print(f"Processing Mode: {mode}")
    print(f"Batch Size: {BATCH_SIZE} projects per batch")
    print(f"Max Workers: {MAX_WORKERS} parallel threads")
    if ENABLE_BACKGROUND_MODE:
        print(f"Background Log: {BACKGROUND_LOG_FILE}")
        print(f"Checkpoint Interval: Every {CHECKPOINT_INTERVAL} projects")
    print("=" * 60)
    
    analyzer = VMRightSizingAnalyzer()
    
    try:
        start_time = time.time()
        
        # Analyze projects
        logger.info("🚀 Starting analysis...")
        recommendations = analyzer.analyze_all_projects()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"⏱️  Total Analysis Time: {duration/60:.1f} minutes")
        print(f"📁 Total Projects Analyzed: {analyzer.processed_projects}")
        print(f"💡 Total Recommendations Found: {len(recommendations)}")
        print(f"💰 Total Potential Monthly Savings: ${analyzer.total_potential_savings:.2f}")
        print(f"💰 Total Potential Annual Savings: ${analyzer.total_potential_savings * 12:.2f}")
        
        if recommendations:
            print(f"📈 Average Savings per Recommendation: ${analyzer.total_potential_savings / len(recommendations):.2f}")
            
            # Show top 5 savings opportunities
            sorted_recommendations = sorted(recommendations, key=lambda x: x['estimated_monthly_savings_usd'], reverse=True)
            print("\n🎯 TOP 5 SAVINGS OPPORTUNITIES")
            print("-" * 80)
            for i, rec in enumerate(sorted_recommendations[:5], 1):
                print(f"{i}. {rec['project_id']}/{rec['instance_name']}")
                print(f"   Current: {rec['current_machine_type']} ({rec['current_vcpus']} vCPUs, {rec['current_memory_gb']:.1f} GB)")
                print(f"   Recommended: {rec['recommended_machine_type']} ({rec['recommended_vcpus']} vCPUs, {rec['recommended_memory_gb']:.1f} GB)")
                print(f"   💰 Monthly Savings: ${rec['estimated_monthly_savings_usd']:.2f}")
                print(f"   🎯 Priority: {rec.get('recommendation_priority', 'N/A')}")
                print()
        else:
            print("ℹ️  No right-sizing recommendations found.")
            print("   This could mean:")
            print("   - VMs are already optimally sized")
            print("   - Recommender API needs time to gather utilization data")
            print("   - API access issues for the analyzed projects")
        
        # Generate report
        if recommendations:
            print("📄 Generating detailed Excel report...")
            report_file = analyzer.generate_recommendations_report()
            if report_file:
                print(f"✅ Detailed report saved to: {report_file}")
            else:
                print("❌ Failed to generate report")
        
        print("\n🎉 Analysis completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user")
        logger.info("Analysis interrupted by user")
        if analyzer.recommendations:
            print(f"📊 Partial results: {len(analyzer.recommendations)} recommendations found")
            report_file = analyzer.generate_recommendations_report()
            if report_file:
                print(f"📄 Partial report saved to: {report_file}")
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print(f"❌ Analysis failed: {e}")
        print("💡 Check the logs for more details")
        
        # Try to save partial results if any
        if hasattr(analyzer, 'recommendations') and analyzer.recommendations:
            print(f"📊 Attempting to save partial results: {len(analyzer.recommendations)} recommendations")
            try:
                report_file = analyzer.generate_recommendations_report()
                if report_file:
                    print(f"📄 Partial report saved to: {report_file}")
            except Exception as report_error:
                logger.error(f"Failed to save partial results: {report_error}")
                print("❌ Failed to save partial results")

if __name__ == "__main__":
    main()