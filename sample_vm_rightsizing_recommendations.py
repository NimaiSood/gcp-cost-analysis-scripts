#!/usr/bin/env python3
"""
Sample VM Right-sizing Recommendations Excel Report Generator
This script generates a sample Excel report showing what the enhanced 
right-sizing analysis would produce when recommendations are available.
"""

import pandas as pd
import datetime
from openpyxl import Workbook

def create_sample_recommendations():
    """Create sample VM right-sizing recommendations data."""
    sample_data = [
        {
            'Project ID': 'sample-project-001',
            'Zone': 'us-central1-a',
            'VM Instance Name': 'web-server-01',
            'Current Instance Type': 'n1-standard-8',
            'Current vCPUs': 8,
            'Current Memory (GB)': 30.0,
            'Recommended Instance Type': 'n1-standard-4',
            'Recommended vCPUs': 4,
            'Recommended Memory (GB)': 15.0,
            'CPU Utilization Pattern': '15.2% avg utilization',
            'Memory Utilization Pattern': '22.8% avg utilization',
            'CPU Reduction (%)': 50.0,
            'Memory Reduction (%)': 50.0,
            'Monthly Savings (USD)': 142.50,
            'Annual Savings (USD)': 1710.00,
            'Priority': 'P1',
            'Recommendation Details': 'Instance is significantly under-utilized. CPU usage averaged 15.2% and memory 22.8% over the past 30 days.',
            'Last Analysis Date': '2025-08-24 21:51:34'
        },
        {
            'Project ID': 'sample-project-002',
            'Zone': 'us-west1-b',
            'VM Instance Name': 'database-server-02',
            'Current Instance Type': 'n1-highmem-16',
            'Current vCPUs': 16,
            'Current Memory (GB)': 104.0,
            'Recommended Instance Type': 'n1-highmem-8',
            'Recommended vCPUs': 8,
            'Recommended Memory (GB)': 52.0,
            'CPU Utilization Pattern': '28.5% avg utilization',
            'Memory Utilization Pattern': '45.3% avg utilization',
            'CPU Reduction (%)': 50.0,
            'Memory Reduction (%)': 50.0,
            'Monthly Savings (USD)': 384.20,
            'Annual Savings (USD)': 4610.40,
            'Priority': 'P1',
            'Recommendation Details': 'High-memory instance showing low utilization patterns. Consider right-sizing to reduce costs while maintaining performance.',
            'Last Analysis Date': '2025-08-24 21:51:34'
        },
        {
            'Project ID': 'sample-project-003',
            'Zone': 'europe-west1-c',
            'VM Instance Name': 'app-server-03',
            'Current Instance Type': 'n2-standard-4',
            'Current vCPUs': 4,
            'Current Memory (GB)': 16.0,
            'Recommended Instance Type': 'e2-standard-2',
            'Recommended vCPUs': 2,
            'Recommended Memory (GB)': 8.0,
            'CPU Utilization Pattern': '18.7% avg utilization',
            'Memory Utilization Pattern': '31.2% avg utilization',
            'CPU Reduction (%)': 50.0,
            'Memory Reduction (%)': 50.0,
            'Monthly Savings (USD)': 89.75,
            'Annual Savings (USD)': 1077.00,
            'Priority': 'P2',
            'Recommendation Details': 'Application server with consistent low utilization. E2 machine type offers better cost efficiency.',
            'Last Analysis Date': '2025-08-24 21:51:34'
        },
        {
            'Project ID': 'sample-project-004',
            'Zone': 'asia-east1-a',
            'VM Instance Name': 'batch-processor-04',
            'Current Instance Type': 'c2-standard-8',
            'Current vCPUs': 8,
            'Current Memory (GB)': 32.0,
            'Recommended Instance Type': 'c2-standard-4',
            'Recommended vCPUs': 4,
            'Recommended Memory (GB)': 16.0,
            'CPU Utilization Pattern': '35.4% avg utilization',
            'Memory Utilization Pattern': '28.9% avg utilization',
            'CPU Reduction (%)': 50.0,
            'Memory Reduction (%)': 50.0,
            'Monthly Savings (USD)': 156.30,
            'Annual Savings (USD)': 1875.60,
            'Priority': 'P2',
            'Recommendation Details': 'Compute-optimized workload with room for optimization while maintaining compute performance.',
            'Last Analysis Date': '2025-08-24 21:51:34'
        },
        {
            'Project ID': 'sample-project-005',
            'Zone': 'us-central1-b',
            'VM Instance Name': 'dev-environment-05',
            'Current Instance Type': 'n1-standard-2',
            'Current vCPUs': 2,
            'Current Memory (GB)': 7.5,
            'Recommended Instance Type': 'e2-micro',
            'Recommended vCPUs': 1,
            'Recommended Memory (GB)': 1.0,
            'CPU Utilization Pattern': '8.1% avg utilization',
            'Memory Utilization Pattern': '12.5% avg utilization',
            'CPU Reduction (%)': 50.0,
            'Memory Reduction (%)': 86.7,
            'Monthly Savings (USD)': 67.85,
            'Annual Savings (USD)': 814.20,
            'Priority': 'P3',
            'Recommendation Details': 'Development environment with very low resource usage. Consider using micro instances for cost optimization.',
            'Last Analysis Date': '2025-08-24 21:51:34'
        }
    ]
    return sample_data

def generate_sample_excel_report():
    """Generate a comprehensive sample Excel report."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sample_vm_rightsizing_recommendations_01227B-3F83E7-AC2416_{timestamp}.xlsx"
    
    # Get sample data
    sample_data = create_sample_recommendations()
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Main recommendations sheet
        df_recommendations = pd.DataFrame(sample_data)
        df_recommendations.to_excel(writer, sheet_name='VM_Recommendations', index=False)
        
        # Executive Summary
        total_monthly_savings = sum(r['Monthly Savings (USD)'] for r in sample_data)
        total_annual_savings = sum(r['Annual Savings (USD)'] for r in sample_data)
        
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
                '',
                'Top Instance Types for Right-sizing:',
                'Most over-provisioned type: n1-standard-8',
                'Most recommended type: n1-standard-4',
            ],
            'Value': [
                '2025-08-24 21:51:34',
                '01227B-3F83E7-AC2416',
                '5',
                '5',
                f"${total_monthly_savings:.2f}",
                f"${total_annual_savings:.2f}",
                f"${total_monthly_savings / len(sample_data):.2f}",
                f"${total_annual_savings / len(sample_data):.2f}",
                '',
                '',
                '38',
                '19',
                '19',
                '50.0%',
                '189.5',
                '92.0',
                '97.5',
                '51.5%',
                '',
                '',
                '2',
                '2',
                '1',
                '',
                '',
                '',
                '',
            ]
        }
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name='Executive_Summary', index=False)
        
        # Top savings opportunities (all 5 since it's a sample)
        df_top_savings = df_recommendations.nlargest(5, 'Monthly Savings (USD)')
        df_top_savings.to_excel(writer, sheet_name='Top_Savings_Opportunities', index=False)
        
        # Instance type analysis
        instance_analysis_data = [
            {
                'Current Instance Type': 'n1-standard-8',
                'Number of Instances': 1,
                'Total Monthly Savings (USD)': 142.50,
                'Average Savings per Instance (USD)': 142.50,
                'Total Current vCPUs': 8,
                'Total Recommended vCPUs': 4,
                'vCPU Reduction': 4,
                'Most Common Recommendation': 'n1-standard-4',
                'Optimization Potential': 'High'
            },
            {
                'Current Instance Type': 'n1-highmem-16',
                'Number of Instances': 1,
                'Total Monthly Savings (USD)': 384.20,
                'Average Savings per Instance (USD)': 384.20,
                'Total Current vCPUs': 16,
                'Total Recommended vCPUs': 8,
                'vCPU Reduction': 8,
                'Most Common Recommendation': 'n1-highmem-8',
                'Optimization Potential': 'High'
            },
            {
                'Current Instance Type': 'n2-standard-4',
                'Number of Instances': 1,
                'Total Monthly Savings (USD)': 89.75,
                'Average Savings per Instance (USD)': 89.75,
                'Total Current vCPUs': 4,
                'Total Recommended vCPUs': 2,
                'vCPU Reduction': 2,
                'Most Common Recommendation': 'e2-standard-2',
                'Optimization Potential': 'Medium'
            }
        ]
        
        df_instance_analysis = pd.DataFrame(instance_analysis_data)
        df_instance_analysis.to_excel(writer, sheet_name='Instance_Type_Analysis', index=False)
        
        # Project-wise analysis
        project_analysis_data = [
            {
                'Project ID': 'sample-project-002',
                'Number of Instances': 1,
                'Total Monthly Savings (USD)': 384.20,
                'High Priority Recommendations': 1,
                'Optimization Priority': 'Critical'
            },
            {
                'Project ID': 'sample-project-004',
                'Number of Instances': 1,
                'Total Monthly Savings (USD)': 156.30,
                'High Priority Recommendations': 0,
                'Optimization Priority': 'Medium'
            },
            {
                'Project ID': 'sample-project-001',
                'Number of Instances': 1,
                'Total Monthly Savings (USD)': 142.50,
                'High Priority Recommendations': 1,
                'Optimization Priority': 'Medium'
            }
        ]
        
        df_project_analysis = pd.DataFrame(project_analysis_data)
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
    
    print(f"✅ Sample Excel report generated: {filename}")
    return filename

if __name__ == "__main__":
    print("🔄 Generating sample VM right-sizing recommendations Excel report...")
    filename = generate_sample_excel_report()
    print(f"📊 Report includes comprehensive details around:")
    print("   • VM instance names and current configurations")
    print("   • Current vs recommended instance types")
    print("   • CPU and memory utilization patterns")
    print("   • Detailed cost savings calculations")
    print("   • Priority-based recommendations")
    print("   • Implementation guidance")
    print("   • Executive summary with key metrics")
    print("   • Project-wise and instance-type analysis")
    print(f"📁 Open {filename} to see the comprehensive analysis format")
