#!/usr/bin/env python3
"""
VM Right-sizing Background Monitor
Monitors the progress of the background VM right-sizing analysis.
"""

import os
import time
import subprocess
import re
from datetime import datetime

def check_process_status():
    """Check if the right-sizing process is still running."""
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        lines = result.stdout.split('\n')
        
        for line in lines:
            if 'right-sizing-compute.py' in line and 'grep' not in line:
                # Extract process info
                parts = line.split()
                if len(parts) >= 11:
                    pid = parts[1]
                    cpu = parts[2]
                    mem = parts[3]
                    time_running = parts[9]
                    return {
                        'running': True,
                        'pid': pid,
                        'cpu_percent': cpu,
                        'memory_percent': mem,
                        'time': time_running
                    }
        
        return {'running': False}
    except Exception as e:
        print(f"Error checking process: {e}")
        return {'running': False}

def analyze_log_file():
    """Analyze the current log file for progress."""
    log_files = [f for f in os.listdir('.') if f.startswith('vm_rightsizing_output') and f.endswith('.log')]
    
    if not log_files:
        return None
    
    # Get the most recent log file
    latest_log = max(log_files, key=os.path.getctime)
    
    try:
        with open(latest_log, 'r') as f:
            lines = f.readlines()
        
        stats = {
            'total_lines': len(lines),
            'projects_found': 0,
            'projects_with_instances': [],
            'api_errors': 0,
            'permission_denied': 0,
            'latest_activity': 'Unknown',
            'batches_completed': 0,
            'recommendations_found': 0,
            'total_savings': 0.0
        }
        
        for line in lines:
            # Count projects with instances
            if 'running instances' in line and 'INFO' in line:
                match = re.search(r'Project ([^:]+): (\d+) running instances', line)
                if match:
                    project_id, instance_count = match.groups()
                    stats['projects_with_instances'].append((project_id, int(instance_count)))
                    stats['projects_found'] += 1
            
            # Count errors
            if 'API call failed after 3 attempts' in line:
                stats['api_errors'] += 1
            
            if 'Permission denied' in line:
                stats['permission_denied'] += 1
            
            # Track batch progress
            if 'Batch' in line and 'completed' in line:
                match = re.search(r'Batch (\d+)', line)
                if match:
                    stats['batches_completed'] = max(stats['batches_completed'], int(match.group(1)))
            
            # Track recommendations
            if 'Found' in line and 'recommendations' in line:
                match = re.search(r'Found (\d+) recommendations', line)
                if match:
                    stats['recommendations_found'] += int(match.group(1))
            
            # Track savings
            if 'potential monthly savings' in line:
                match = re.search(r'\$([0-9.]+) potential monthly savings', line)
                if match:
                    stats['total_savings'] += float(match.group(1))
            
            # Get latest timestamp
            if line.strip():
                stats['latest_activity'] = line[:19]  # Extract timestamp
        
        return stats
    
    except Exception as e:
        print(f"Error reading log file: {e}")
        return None

def main():
    """Main monitoring function."""
    print("🔍 VM Right-sizing Background Monitor")
    print("=" * 50)
    
    while True:
        # Clear screen
        os.system('clear' if os.name == 'posix' else 'cls')
        
        print("🔍 VM Right-sizing Background Analysis Monitor")
        print("=" * 60)
        print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Check process status
        process_info = check_process_status()
        
        if process_info['running']:
            print(f"✅ Process Status: RUNNING (PID: {process_info['pid']})")
            print(f"   CPU Usage: {process_info['cpu_percent']}%")
            print(f"   Memory Usage: {process_info['memory_percent']}%")
            print(f"   Runtime: {process_info['time']}")
        else:
            print("❌ Process Status: NOT RUNNING")
        
        print()
        
        # Analyze log file
        stats = analyze_log_file()
        
        if stats:
            print("📊 ANALYSIS PROGRESS")
            print("-" * 30)
            print(f"Projects with Instances Found: {stats['projects_found']}")
            print(f"Total Log Lines: {stats['total_lines']}")
            print(f"API Errors: {stats['api_errors']}")
            print(f"Permission Denied: {stats['permission_denied']}")
            print(f"Batches Completed: {stats['batches_completed']}")
            print(f"Recommendations Found: {stats['recommendations_found']}")
            print(f"Total Potential Savings: ${stats['total_savings']:.2f}")
            print(f"Latest Activity: {stats['latest_activity']}")
            
            print()
            
            # Show top projects by instance count
            if stats['projects_with_instances']:
                top_projects = sorted(stats['projects_with_instances'], key=lambda x: x[1], reverse=True)[:10]
                print("🏆 TOP PROJECTS BY INSTANCE COUNT")
                print("-" * 40)
                for i, (project, count) in enumerate(top_projects, 1):
                    print(f"{i:2d}. {project[:30]:<30} {count:3d} instances")
                
                total_instances = sum(count for _, count in stats['projects_with_instances'])
                print(f"\nTotal Instances Discovered: {total_instances}")
        else:
            print("📊 No log data available yet...")
        
        print()
        print("Press Ctrl+C to exit monitoring")
        print("Refreshing in 30 seconds...")
        
        try:
            time.sleep(30)
        except KeyboardInterrupt:
            print("\n\n👋 Monitoring stopped.")
            break

if __name__ == "__main__":
    main()
