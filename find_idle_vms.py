import os
import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from google.cloud import billing_v1, monitoring_v3, compute_v1

# --- Configuration ---
BILLING_ACCOUNT_ID = "01227B-3F83E7-AC2416"
IDLE_CPU_THRESHOLD_PERCENT = 5.0
IDLE_DURATION_MINUTES = 2880

# --- Excel Report Generation ---
def generate_excel_report(all_results, billing_account_id, cpu_threshold, duration_minutes):
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"idle_vms_report_{billing_account_id}_{timestamp}.xlsx"
        print(f"\n📊 Generating Excel report: {filename}")
        
        idle_instances_data = []
        project_summary_data = []
        total_idle = 0
        total_instances = 0
        
        for result in all_results:
            project_id = result['project_id']
            project_total = result['total_instances']
            project_idle = len(result['idle_instances'])
            total_instances += project_total
            total_idle += project_idle
            
            project_summary_data.append({
                'Project ID': project_id,
                'Total Instances': project_total,
                'Idle Instances': project_idle,
                'Idle Percentage': f"{(project_idle/project_total*100):.1f}%" if project_total > 0 else "0.0%",
                'Status': result['status']
            })
            
            for instance in result['idle_instances']:
                idle_instances_data.append({
                    'Project ID': project_id,
                    'Instance Name': instance['name'],
                    'Zone': instance['zone'],
                    'CPU Utilization (%)': f"{instance['cpu_utilization']*100:.2f}%",
                    'Static IP Address': instance['static_ip'],
                    'Has Disks': instance['has_disks'],
                    'Analysis Date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            exec_summary_data = {
                'Metric': [
                    'Billing Account ID',
                    'Analysis Date',
                    'Total Projects Analyzed',
                    'Total VM Instances Scanned',
                    'Total Idle Instances Found',
                    'Overall Idle Rate (%)'
                ],
                'Value': [
                    billing_account_id,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    len(all_results),
                    total_instances,
                    total_idle,
                    f"{(total_idle/total_instances*100):.1f}%" if total_instances > 0 else "0.0%"
                ]
            }
            
            pd.DataFrame(exec_summary_data).to_excel(writer, sheet_name='Executive Summary', index=False)
            
            if idle_instances_data:
                pd.DataFrame(idle_instances_data).to_excel(writer, sheet_name='Idle Instances', index=False)
            
            pd.DataFrame(project_summary_data).to_excel(writer, sheet_name='Project Summary', index=False)
        
        print(f"✅ Excel report generated: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Error generating Excel report: {e}")
        return None

# --- Main Processing Function ---
def process_single_project(project_id, billing_account_id, cpu_threshold, duration_minutes):
    try:
        compute_client = compute_v1.InstancesClient()
        monitoring_client = monitoring_v3.MetricServiceClient()
        
        results = {
            'project_id': project_id,
            'idle_instances': [],
            'total_instances': 0,
            'errors': [],
            'status': 'processing'
        }
        
        # Get static IPs
        static_ips = set()
        try:
            addresses_client = compute_v1.AddressesClient()
            aggregated_addresses = addresses_client.aggregated_list(project=project_id)
            for region, addresses_scoped_list in aggregated_addresses:
                if addresses_scoped_list.addresses:
                    for addr in addresses_scoped_list.addresses:
                        if addr.status == compute_v1.Address.Status.RESERVED:
                            static_ips.add(addr.address)
        except Exception:
            pass

        # Get instances
        try:
            aggregated_list = compute_client.aggregated_list(project=project_id)
            
            for zone, scope in aggregated_list:
                if scope.instances:
                    results['total_instances'] += len(scope.instances)
                    zone_name = zone.split('/')[-1] if '/' in zone else zone
                    
                    for instance in scope.instances:
                        try:
                            has_active_disk = bool(instance.disks)
                            
                            # Check for static IP
                            has_static_ip = False
                            static_ip_address = None
                            for network_interface in instance.network_interfaces:
                                for access_config in network_interface.access_configs:
                                    external_ip = access_config.nat_i_p
                                    if external_ip and external_ip in static_ips:
                                        has_static_ip = True
                                        static_ip_address = external_ip
                                        break
                                if has_static_ip:
                                    break

                            # Check CPU utilization
                            is_idle = False
                            avg_utilization = 0
                            
                            try:
                                end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                                start_time = end_time - datetime.timedelta(minutes=duration_minutes)

                                from google.protobuf import timestamp_pb2
                                
                                end_timestamp = timestamp_pb2.Timestamp()
                                end_timestamp.FromDatetime(end_time)
                                
                                start_timestamp = timestamp_pb2.Timestamp()
                                start_timestamp.FromDatetime(start_time)
                                
                                request = monitoring_v3.ListTimeSeriesRequest(
                                    name=f"projects/{project_id}",
                                    filter=f'metric.type="compute.googleapis.com/instance/cpu/utilization" AND resource.labels.instance_id="{instance.id}"',
                                    interval=monitoring_v3.TimeInterval(
                                        end_time=end_timestamp,
                                        start_time=start_timestamp
                                    ),
                                    view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
                                )

                                response = monitoring_client.list_time_series(request=request)
                                
                                total_utilization = 0
                                point_count = 0
                                
                                for time_series in response:
                                    for point in time_series.points:
                                        total_utilization += point.value.double_value
                                        point_count += 1
                                
                                if point_count > 0:
                                    avg_utilization = total_utilization / point_count
                                    if avg_utilization < (cpu_threshold / 100):
                                        is_idle = True
                                else:
                                    continue
                                    
                            except Exception:
                                continue

                            # Check if instance meets all criteria
                            if is_idle and has_active_disk and has_static_ip:
                                idle_instance = {
                                    'name': instance.name,
                                    'zone': zone_name,
                                    'cpu_utilization': avg_utilization,
                                    'static_ip': static_ip_address,
                                    'has_disks': has_active_disk
                                }
                                results['idle_instances'].append(idle_instance)
                                print(f"🎯 IDLE INSTANCE: {project_id}/{instance.name} - CPU: {avg_utilization:.2%} - IP: {static_ip_address}")
                                
                        except Exception:
                            continue
                            
        except Exception:
            results['status'] = 'failed'
            return results

        results['status'] = 'completed'
        return results
        
    except Exception:
        return {
            'project_id': project_id,
            'idle_instances': [],
            'total_instances': 0,
            'errors': [],
            'status': 'failed'
        }

def list_idle_instances(billing_account_id, cpu_threshold, duration_minutes, batch_size=100, max_workers=20):
    try:
        billing_client = billing_v1.CloudBillingClient()
        print("✅ Successfully initialized Google Cloud billing client")
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        return

    billing_account_name = f"billingAccounts/{billing_account_id}"
    
    try:
        request = billing_v1.ListProjectBillingInfoRequest(name=billing_account_name)
        response = billing_client.list_project_billing_info(request=request)
        projects = [project.project_id for project in response if project.billing_enabled]
        
        print(f"Found {len(projects)} projects - Processing in batches of {batch_size}")
        print(f"🎯 ONLY IDLE INSTANCES WILL BE DISPLAYED")
    except Exception as e:
        print(f"❌ Error listing projects: {e}")
        return

    all_results = []
    total_idle_instances = 0
    total_instances_scanned = 0
    
    project_batches = [projects[i:i + batch_size] for i in range(0, len(projects), batch_size)]
    
    print(f"\n🚀 Starting {len(project_batches)} batches with {max_workers} workers each")
    print("=" * 80)
    
    for batch_num, project_batch in enumerate(project_batches, 1):
        print(f"\n📦 Batch {batch_num}/{len(project_batches)} ({len(project_batch)} projects)")
        
        batch_results = []
        
        with ProcessPoolExecutor(max_workers=min(max_workers, len(project_batch))) as executor:
            future_to_project = {
                executor.submit(process_single_project, project_id, billing_account_id, cpu_threshold, duration_minutes): project_id
                for project_id in project_batch
            }
            
            for future in as_completed(future_to_project):
                try:
                    result = future.result(timeout=300)
                    batch_results.append(result)
                    
                    total_idle_instances += len(result['idle_instances'])
                    total_instances_scanned += result['total_instances']
                    
                except Exception as e:
                    project_id = future_to_project[future]
                    print(f"❌ {project_id}: Failed")
        
        all_results.extend(batch_results)
        batch_idle = sum(len(r['idle_instances']) for r in batch_results)
        print(f"📊 Batch {batch_num}: {batch_idle} idle instances found")
    
    # Generate Excel Report
    excel_filename = generate_excel_report(all_results, billing_account_id, cpu_threshold, duration_minutes)
    
    print("\n" + "=" * 80)
    print("🏁 FINAL RESULTS")
    print("=" * 80)
    print(f"📈 SUMMARY:")
    print(f"  - Total Projects: {len(projects)}")
    print(f"  - Total Instances: {total_instances_scanned}")
    print(f"  - Idle Instances: {total_idle_instances}")
    print(f"  - Excel Report: {excel_filename}")

if __name__ == "__main__":
    try:
        print("🚀 Parallel Idle VM Scanner - Batch Size: 100")
        print("=" * 80)
        
        list_idle_instances(
            billing_account_id=BILLING_ACCOUNT_ID, 
            cpu_threshold=IDLE_CPU_THRESHOLD_PERCENT, 
            duration_minutes=IDLE_DURATION_MINUTES,
            batch_size=100,
            max_workers=20
        )
        
    except KeyboardInterrupt:
        print("\n⚠️ Scan interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
