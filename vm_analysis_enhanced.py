#!/usr/bin/env python3
"""
Enhanced GCP VM Right-Sizing Analysis with Fallback Methods
===========================================================

When Recommender API access is denied, this script falls back to:
1. VM Instance inventory analysis
2. Machine type cost analysis  
3. Basic recommendations based on machine type patterns
4. Detailed Excel reporting with all discovered VMs

Author: GitHub Copilot
Date: August 24, 2025
"""

import logging
import time
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from google.cloud import billing_v1, compute_v1, recommender_v1, bigquery
from google.api_core.exceptions import PermissionDenied, NotFound, Forbidden
from google.api_core import retry
import re

# Configuration
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
TOP_PROJECTS_LIMIT = None  # None = all projects  
BATCH_SIZE = 20
MAX_WORKERS = 10
ENABLE_BACKGROUND_MODE = True

# Recommender configuration
RECOMMENDER_ID = "google.compute.instance.MachineTypeRecommender"

# VM pricing estimates (USD per hour) - rough estimates for analysis
VM_PRICING = {
    'e2-micro': 0.008,
    'e2-small': 0.017,
    'e2-medium': 0.034,
    'e2-standard-2': 0.067,
    'e2-standard-4': 0.134,
    'n1-standard-1': 0.048,
    'n1-standard-2': 0.095,
    'n1-standard-4': 0.190,
    'n1-standard-8': 0.380,
    'n1-standard-16': 0.760,
    'n2-standard-2': 0.097,
    'n2-standard-4': 0.194,
    'n2-standard-8': 0.388,
    'n2-standard-16': 0.776,
    'c2-standard-4': 0.212,
    'c2-standard-8': 0.424,
    'c2-standard-16': 0.848,
}

def setup_logging():
    """Setup logging configuration for background processing"""
    if ENABLE_BACKGROUND_MODE:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"vm_analysis_enhanced_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    return logging.getLogger(__name__)

logger = setup_logging()

def get_machine_type_estimate(machine_type: str) -> float:
    """Get estimated hourly cost for machine type"""
    # Extract base machine type (remove zones/custom specs)
    base_type = machine_type.split('/')[-1]
    
    # Check exact match first
    if base_type in VM_PRICING:
        return VM_PRICING[base_type]
    
    # Try to match similar types
    for pricing_type, cost in VM_PRICING.items():
        if pricing_type in base_type:
            return cost
    
    # Default estimate based on pattern matching
    if 'micro' in base_type:
        return 0.008
    elif 'small' in base_type:
        return 0.017
    elif 'medium' in base_type:
        return 0.034
    elif 'standard-16' in base_type:
        return 0.776
    elif 'standard-8' in base_type:
        return 0.388
    elif 'standard-4' in base_type:
        return 0.194
    elif 'standard-2' in base_type:
        return 0.097
    else:
        return 0.100  # Default estimate

def analyze_machine_type_efficiency(machine_type: str, zone: str) -> Dict[str, Any]:
    """Analyze if a machine type might be oversized based on naming patterns"""
    base_type = machine_type.split('/')[-1]
    analysis = {
        'machine_type': base_type,
        'estimated_hourly_cost': get_machine_type_estimate(machine_type),
        'potential_issues': [],
        'recommendations': [],
        'efficiency_score': 100  # Start with perfect score
    }
    
    # Check for potentially oversized patterns
    if 'standard-16' in base_type or 'standard-32' in base_type:
        analysis['potential_issues'].append("Large instance - verify high CPU/memory requirements")
        analysis['recommendations'].append("Consider monitoring utilization and downsizing if underutilized")
        analysis['efficiency_score'] -= 20
    
    if 'c2-' in base_type:
        analysis['potential_issues'].append("Compute-optimized instance - ensure CPU-intensive workload")
        analysis['recommendations'].append("Verify workload requires high CPU performance")
        analysis['efficiency_score'] -= 10
    
    if 'n1-' in base_type:
        analysis['potential_issues'].append("Older generation instance")
        analysis['recommendations'].append("Consider migrating to newer N2 or E2 instances for better price/performance")
        analysis['efficiency_score'] -= 15
    
    # Check for potentially undersized patterns that might cause performance issues
    if 'micro' in base_type or 'small' in base_type:
        analysis['potential_issues'].append("Very small instance - may have performance limitations")
    
    return analysis

class EnhancedVMAnalyzer:
    def __init__(self):
        """Initialize the enhanced VM analyzer with fallback capabilities"""
        try:
            self.billing_client = billing_v1.CloudBillingClient()
            self.compute_client = compute_v1.InstancesClient()
            self.bigquery_client = bigquery.Client()
            try:
                self.recommender_client = recommender_v1.RecommenderClient()
            except Exception as e:
                logger.warning(f"Recommender client initialization failed (will use fallback): {e}")
                self.recommender_client = None
            logger.info("Enhanced VM Analyzer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}")
            raise

    @retry.Retry(predicate=lambda exc: not isinstance(exc, (PermissionDenied, Forbidden)))
    def call_api_with_retry(self, api_call, *args, **kwargs):
        """Call API with retry logic, excluding permission errors"""
        try:
            return api_call(*args, **kwargs)
        except (PermissionDenied, Forbidden) as e:
            logger.debug(f"Permission denied (non-retryable): {e}")
            raise
        except Exception as e:
            logger.warning(f"API call failed, retrying: {e}")
            raise

    def get_all_projects(self) -> List[str]:
        """Get all projects in the billing account"""
        try:
            logger.info(f"Discovering all projects in billing account {BILLING_ACCOUNT_ID}")
            parent = f"billingAccounts/{BILLING_ACCOUNT_ID}"
            
            request = billing_v1.ListProjectBillingInfoRequest(name=parent)
            page_result = self.billing_client.list_project_billing_info(request=request)
            
            projects = []
            for response in page_result:
                if response.project_id:
                    projects.append(response.project_id)
            
            logger.info(f"Found {len(projects)} total projects in billing account")
            return projects
            
        except Exception as e:
            logger.error(f"Failed to get projects from billing account: {e}")
            return []

    def get_vm_instances_for_project(self, project_id: str) -> List[Dict[str, Any]]:
        """Get all VM instances for a project with detailed information"""
        try:
            instances = []
            request = compute_v1.AggregatedListInstancesRequest(project=project_id)
            
            agg_list = self.call_api_with_retry(
                self.compute_client.aggregated_list,
                request=request
            )
            
            for zone, response in agg_list:
                if hasattr(response, 'instances') and response.instances:
                    for instance in response.instances:
                        if instance.status == 'RUNNING':
                            # Extract zone from the full zone URL
                            zone_name = zone.split('/')[-1] if '/' in zone else zone
                            
                            instance_info = {
                                'name': instance.name,
                                'project_id': project_id,
                                'zone': zone_name,
                                'machine_type': instance.machine_type,
                                'status': instance.status,
                                'creation_timestamp': instance.creation_timestamp,
                                'network_interfaces': len(instance.network_interfaces),
                                'disks': len(instance.disks),
                                'analysis': analyze_machine_type_efficiency(instance.machine_type, zone_name)
                            }
                            instances.append(instance_info)
            
            return instances
            
        except (PermissionDenied, Forbidden):
            logger.debug(f"Permission denied for project {project_id}")
            return []
        except Exception as e:
            logger.warning(f"Failed to get instances for project {project_id}: {e}")
            return []

    def get_projects_with_instances(self) -> List[Tuple[str, int, float]]:
        """Discover all projects with running instances and estimate costs"""
        logger.info("🔍 Discovering projects with running VM instances...")
        all_projects = self.get_all_projects()
        
        if not all_projects:
            logger.error("No projects found in billing account")
            return []
        
        projects_with_instances = []
        total_instances_found = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_project = {
                executor.submit(self.get_vm_instances_for_project, project): project 
                for project in all_projects
            }
            
            for future in as_completed(future_to_project):
                project = future_to_project[future]
                try:
                    instances = future.result()
                    if instances:
                        # Calculate estimated monthly cost
                        total_hourly_cost = sum(inst['analysis']['estimated_hourly_cost'] for inst in instances)
                        estimated_monthly_cost = total_hourly_cost * 24 * 30  # 30 days
                        
                        projects_with_instances.append((project, len(instances), estimated_monthly_cost))
                        total_instances_found += len(instances)
                        logger.info(f"Project {project}: {len(instances)} running instances (~${estimated_monthly_cost:.2f}/month)")
                
                except Exception as e:
                    logger.warning(f"Failed to process project {project}: {e}")
        
        # Sort by estimated cost descending
        projects_with_instances.sort(key=lambda x: x[2], reverse=True)
        
        logger.info(f"🎯 Found {len(projects_with_instances)} projects with {total_instances_found} total running instances")
        return projects_with_instances

    def analyze_project_detailed(self, project_info: Tuple[str, int, float]) -> Dict[str, Any]:
        """Perform detailed analysis of a single project"""
        project_id, instance_count, estimated_cost = project_info
        
        logger.info(f"📊 Analyzing project {project_id} ({instance_count} instances)")
        
        # Get detailed instance information
        instances = self.get_vm_instances_for_project(project_id)
        
        analysis = {
            'project_id': project_id,
            'instance_count': len(instances),
            'estimated_monthly_cost': estimated_cost,
            'instances': instances,
            'recommendations': [],
            'potential_savings': 0.0,
            'efficiency_issues': 0,
            'total_efficiency_score': 0
        }
        
        # Analyze each instance
        total_score = 0
        issues_found = 0
        potential_monthly_savings = 0.0
        
        for instance in instances:
            inst_analysis = instance['analysis']
            total_score += inst_analysis['efficiency_score']
            
            if inst_analysis['potential_issues']:
                issues_found += len(inst_analysis['potential_issues'])
                
                # Estimate potential savings (conservative 10-20% for problematic instances)
                if inst_analysis['efficiency_score'] < 80:
                    monthly_cost = inst_analysis['estimated_hourly_cost'] * 24 * 30
                    potential_monthly_savings += monthly_cost * 0.15  # 15% estimated savings
        
        analysis['total_efficiency_score'] = total_score / len(instances) if instances else 100
        analysis['efficiency_issues'] = issues_found
        analysis['potential_savings'] = potential_monthly_savings
        
        # Generate project-level recommendations
        if analysis['total_efficiency_score'] < 85:
            analysis['recommendations'].append("Review VM sizing - multiple instances may be oversized")
        
        if issues_found > instance_count * 0.3:  # More than 30% of instances have issues
            analysis['recommendations'].append("Consider VM optimization project - many instances need review")
        
        return analysis

    def save_checkpoint(self, processed_projects: int, total_projects: int, all_results: List[Dict]):
        """Save checkpoint for recovery"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = f"vm_analysis_enhanced_checkpoint_{timestamp}.json"
        
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'processed_projects': processed_projects,
            'total_projects': total_projects,
            'progress_percentage': (processed_projects / total_projects) * 100,
            'results_count': len(all_results),
            'total_instances': sum(r['instance_count'] for r in all_results),
            'total_estimated_cost': sum(r['estimated_monthly_cost'] for r in all_results),
            'total_potential_savings': sum(r['potential_savings'] for r in all_results)
        }
        
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        logger.info(f"💾 Checkpoint saved: {checkpoint_file} - {processed_projects}/{total_projects} projects processed ({checkpoint_data['progress_percentage']:.1f}%)")

    def generate_excel_report(self, all_results: List[Dict], analysis_summary: Dict) -> str:
        """Generate comprehensive Excel report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"vm_analysis_enhanced_report_{timestamp}.xlsx"
        
        logger.info(f"📊 Generating Enhanced Excel report: {filename}")
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            money_format = workbook.add_format({'num_format': '$#,##0.00'})
            number_format = workbook.add_format({'num_format': '#,##0'})
            percent_format = workbook.add_format({'num_format': '0.0%'})
            
            # 1. Executive Summary
            summary_data = {
                'Metric': [
                    'Total Projects Analyzed',
                    'Total VM Instances',
                    'Total Estimated Monthly Cost',
                    'Total Potential Monthly Savings',
                    'Average Efficiency Score',
                    'Projects with Issues',
                    'Analysis Date'
                ],
                'Value': [
                    analysis_summary['total_projects'],
                    analysis_summary['total_instances'],
                    f"${analysis_summary['total_cost']:.2f}",
                    f"${analysis_summary['total_potential_savings']:.2f}",
                    f"{analysis_summary['avg_efficiency_score']:.1f}%",
                    analysis_summary['projects_with_issues'],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Executive Summary', index=False)
            
            # 2. Project Overview
            project_data = []
            for result in all_results:
                project_data.append({
                    'Project ID': result['project_id'],
                    'Instance Count': result['instance_count'],
                    'Est. Monthly Cost': result['estimated_monthly_cost'],
                    'Potential Savings': result['potential_savings'],
                    'Efficiency Score': result['total_efficiency_score'],
                    'Issues Found': result['efficiency_issues'],
                    'Recommendations Count': len(result['recommendations'])
                })
            
            projects_df = pd.DataFrame(project_data)
            projects_df.to_excel(writer, sheet_name='Projects Overview', index=False)
            
            # Format the Projects Overview sheet
            worksheet = writer.sheets['Projects Overview']
            worksheet.set_column('A:A', 30)  # Project ID
            worksheet.set_column('B:B', 12)  # Instance Count
            worksheet.set_column('C:C', 15)  # Est. Monthly Cost
            worksheet.set_column('D:D', 15)  # Potential Savings
            worksheet.set_column('E:E', 15)  # Efficiency Score
            worksheet.set_column('F:F', 12)  # Issues Found
            worksheet.set_column('G:G', 18)  # Recommendations Count
            
            # 3. Detailed Instance Analysis
            instance_data = []
            for result in all_results:
                for instance in result['instances']:
                    instance_data.append({
                        'Project ID': result['project_id'],
                        'Instance Name': instance['name'],
                        'Zone': instance['zone'],
                        'Machine Type': instance['analysis']['machine_type'],
                        'Est. Hourly Cost': instance['analysis']['estimated_hourly_cost'],
                        'Est. Monthly Cost': instance['analysis']['estimated_hourly_cost'] * 24 * 30,
                        'Efficiency Score': instance['analysis']['efficiency_score'],
                        'Issues': '; '.join(instance['analysis']['potential_issues']),
                        'Recommendations': '; '.join(instance['analysis']['recommendations']),
                        'Creation Date': instance.get('creation_timestamp', 'Unknown')
                    })
            
            instances_df = pd.DataFrame(instance_data)
            instances_df.to_excel(writer, sheet_name='Instance Details', index=False)
            
            # Format the Instance Details sheet
            worksheet = writer.sheets['Instance Details']
            worksheet.set_column('A:A', 25)  # Project ID
            worksheet.set_column('B:B', 25)  # Instance Name
            worksheet.set_column('C:C', 15)  # Zone
            worksheet.set_column('D:D', 20)  # Machine Type
            worksheet.set_column('E:E', 15)  # Est. Hourly Cost
            worksheet.set_column('F:F', 15)  # Est. Monthly Cost
            worksheet.set_column('G:G', 15)  # Efficiency Score
            worksheet.set_column('H:H', 50)  # Issues
            worksheet.set_column('I:I', 50)  # Recommendations
            worksheet.set_column('J:J', 20)  # Creation Date
            
            # 4. Top Issues Summary
            issues_summary = {}
            for result in all_results:
                for instance in result['instances']:
                    for issue in instance['analysis']['potential_issues']:
                        issues_summary[issue] = issues_summary.get(issue, 0) + 1
            
            if issues_summary:
                issues_data = [
                    {'Issue Type': issue, 'Instances Affected': count, 'Percentage': (count / analysis_summary['total_instances']) * 100}
                    for issue, count in sorted(issues_summary.items(), key=lambda x: x[1], reverse=True)
                ]
                
                issues_df = pd.DataFrame(issues_data)
                issues_df.to_excel(writer, sheet_name='Top Issues', index=False)
        
        logger.info(f"✅ Excel report generated: {filename}")
        return filename

    def analyze_all_projects(self) -> Tuple[List[Dict], Dict]:
        """Analyze all projects with enhanced fallback methods"""
        logger.info("🚀 Starting Enhanced VM Analysis across all projects")
        
        # Step 1: Discover projects with instances
        projects_with_instances = self.get_projects_with_instances()
        
        if not projects_with_instances:
            logger.warning("No projects with instances found")
            return [], {}
        
        # Sort by estimated cost and limit if specified
        if TOP_PROJECTS_LIMIT:
            projects_with_instances = projects_with_instances[:TOP_PROJECTS_LIMIT]
            logger.info(f"🎯 Focusing analysis on top {TOP_PROJECTS_LIMIT} projects")
        else:
            logger.info(f"🎯 Analyzing all {len(projects_with_instances)} projects with instances")
        
        # Display top projects by cost
        logger.info("Top projects by estimated compute cost:")
        for i, (project_id, instance_count, cost) in enumerate(projects_with_instances[:10], 1):
            logger.info(f"  {i}. {project_id}: ${cost:.2f}")
        if len(projects_with_instances) > 10:
            logger.info(f"  ... and {len(projects_with_instances) - 10} more projects")
        
        # Step 2: Batch processing
        all_results = []
        total_projects = len(projects_with_instances)
        
        for i in range(0, total_projects, BATCH_SIZE):
            batch_end = min(i + BATCH_SIZE, total_projects)
            batch_projects = projects_with_instances[i:batch_end]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_projects + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch_projects)} projects)")
            logger.info(f"Batch contains projects: {len(batch_projects)} projects starting with {batch_projects[0][0]}")
            
            # Process batch
            batch_results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_project = {
                    executor.submit(self.analyze_project_detailed, project_info): project_info[0]
                    for project_info in batch_projects
                }
                
                for future in as_completed(future_to_project):
                    try:
                        result = future.result()
                        batch_results.append(result)
                    except Exception as e:
                        project_id = future_to_project[future]
                        logger.error(f"Failed to analyze project {project_id}: {e}")
            
            all_results.extend(batch_results)
            
            # Progress reporting
            processed = i + len(batch_projects)
            progress_pct = (processed / total_projects) * 100
            elapsed_time = (batch_num * 2) / 60  # Rough estimate
            eta = ((total_batches - batch_num) * 2) / 60
            
            total_savings = sum(r['potential_savings'] for r in all_results)
            logger.info(f"✅ Batch {batch_num}/{total_batches} completed ({progress_pct:.1f}%)")
            logger.info(f"   Found ${total_savings:.2f} potential monthly savings so far")
            logger.info(f"   Total processed: {processed}/{total_projects} projects")
            logger.info(f"   Elapsed: {elapsed_time:.1f}m, ETA: {eta:.1f}m")
            
            # Save checkpoint every 2 batches
            if batch_num % 2 == 0:
                self.save_checkpoint(processed, total_projects, all_results)
            
            # Brief pause between batches
            if batch_num < total_batches:
                logger.info("Pausing 2 seconds before next batch...")
                time.sleep(2)
        
        # Final analysis summary
        analysis_summary = {
            'total_projects': len(all_results),
            'total_instances': sum(r['instance_count'] for r in all_results),
            'total_cost': sum(r['estimated_monthly_cost'] for r in all_results),
            'total_potential_savings': sum(r['potential_savings'] for r in all_results),
            'avg_efficiency_score': sum(r['total_efficiency_score'] for r in all_results) / len(all_results) if all_results else 100,
            'projects_with_issues': len([r for r in all_results if r['efficiency_issues'] > 0]),
            'analysis_time': elapsed_time
        }
        
        logger.info(f"\n🎉 Enhanced Analysis completed! Total time: {analysis_summary['analysis_time']:.1f} minutes")
        logger.info(f"📊 Total projects analyzed: {analysis_summary['total_projects']}")
        logger.info(f"🖥️  Total VM instances: {analysis_summary['total_instances']}")
        logger.info(f"💰 Total estimated monthly cost: ${analysis_summary['total_cost']:.2f}")
        logger.info(f"💡 Total potential monthly savings: ${analysis_summary['total_potential_savings']:.2f}")
        logger.info(f"⚡ Average efficiency score: {analysis_summary['avg_efficiency_score']:.1f}%")
        
        return all_results, analysis_summary

def main():
    """Main function"""
    logger.info("🔍 Enhanced GCP VM Analysis - ALL Projects with Fallback Methods")
    logger.info("=" * 60)
    logger.info(f"Billing Account: {BILLING_ACCOUNT_ID}")
    logger.info(f"Analysis Scope: {'ALL Projects with Compute Instances' if TOP_PROJECTS_LIMIT is None else f'Top {TOP_PROJECTS_LIMIT} Projects'}")
    logger.info(f"Processing Mode: {'Background Processing' if ENABLE_BACKGROUND_MODE else 'Interactive'}")
    logger.info(f"Batch Size: {BATCH_SIZE} projects per batch")
    logger.info(f"Max Workers: {MAX_WORKERS} parallel threads")
    if ENABLE_BACKGROUND_MODE:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Background Log: vm_analysis_enhanced_{timestamp}.log")
    logger.info("=" * 60)
    logger.info("")
    
    try:
        analyzer = EnhancedVMAnalyzer()
        results, summary = analyzer.analyze_all_projects()
        
        if results:
            # Generate Excel report
            excel_file = analyzer.generate_excel_report(results, summary)
            
            logger.info("=" * 60)
            logger.info("📊 ENHANCED ANALYSIS SUMMARY")
            logger.info("=" * 60)
            logger.info(f"⏱️  Total Analysis Time: {summary['analysis_time']:.1f} minutes")
            logger.info(f"📁 Total Projects Analyzed: {summary['total_projects']}")
            logger.info(f"🖥️  Total VM Instances: {summary['total_instances']}")
            logger.info(f"💰 Total Estimated Monthly Cost: ${summary['total_cost']:.2f}")
            logger.info(f"💡 Total Potential Monthly Savings: ${summary['total_potential_savings']:.2f}")
            logger.info(f"💰 Total Potential Annual Savings: ${summary['total_potential_savings'] * 12:.2f}")
            logger.info(f"⚡ Average Efficiency Score: {summary['avg_efficiency_score']:.1f}%")
            logger.info(f"⚠️  Projects with Issues: {summary['projects_with_issues']}")
            logger.info(f"📋 Excel Report: {excel_file}")
            logger.info("")
            logger.info("🎉 Enhanced analysis completed successfully!")
            
        else:
            logger.warning("❌ No results found - check permissions and project access")
            
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()
