#!/usr/bin/env python3
import subprocess
import time
import curses
import argparse
from collections import defaultdict
from datetime import datetime

def get_slurm_jobs(workflow_id=None):
    """Get Slurm job information using sacct"""
    cmd = ["sacct", "-n", "-o", "JobID,JobName,State,Start,End,Elapsed,MaxRSS,NCPUS", "--parsable2"]
    if workflow_id:
        cmd.extend(["-W", workflow_id])
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.strip().split('\n')

def parse_job_info(job_line):
    """Parse a single line of sacct output"""
    if not job_line.strip():
        return None
    
    fields = job_line.strip().split('|')
    if len(fields) < 8:
        return None
        
    return {
        'job_id': fields[0],
        'name': fields[1],
        'state': fields[2],
        'start': fields[3],
        'end': fields[4],
        'elapsed': fields[5],
        'memory': fields[6],
        'cpus': fields[7]
    }

def format_memory(memory_str):
    """Format memory string to human readable format"""
    if not memory_str:
        return "0"
    return memory_str

class SnakemakeMonitor:
    def __init__(self, workflow_id=None, refresh_rate=30):  # Changed default to 30 seconds
        self.workflow_id = workflow_id
        self.refresh_rate = refresh_rate
        self.screen = None
        self.job_stats = defaultdict(int)
        
        # Column width definitions
        self.col_widths = {
            'job_id': 15,
            'name': 25,
            'state': 12,
            'elapsed': 12,
            'memory': 10,
            'cpus': 6
        }
        
    def update_job_stats(self, jobs):
        """Update job statistics"""
        self.job_stats.clear()
        # Count only parent jobs (ignore .batch jobs)
        for job in jobs:
            if job and not job['job_id'].endswith('.batch'):
                self.job_stats[job['state']] += 1
    
    def draw_header(self):
        """Draw dashboard header"""
        self.screen.addstr(0, 0, "Snakemake Slurm Monitor", curses.A_BOLD)
        self.screen.addstr(1, 0, f"Workflow ID: {self.workflow_id or 'All'}")
        self.screen.addstr(2, 0, f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    def draw_stats(self, start_line):
        """Draw job statistics"""
        self.screen.addstr(start_line, 0, "Job Statistics:", curses.A_BOLD)
        line = start_line + 1
        for state, count in self.job_stats.items():
            self.screen.addstr(line, 2, f"{state}: {count}")
            line += 1
        return line + 1
    
    def format_table_row(self, values, widths, align_left=None):
        """Format a row with proper padding and alignment"""
        if align_left is None:
            align_left = [True] * len(values)  # Default all to left alignment
            
        row = []
        for value, width, left_align in zip(values, widths, align_left):
            value = str(value)
            if left_align:
                row.append(f"{value:<{width}}")
            else:
                row.append(f"{value:>{width}}")
        return "  ".join(row)
    
    def draw_job_table(self, start_line, jobs):
        """Draw job information table"""
        headers = ["JobID", "Name", "State", "Elapsed", "Memory", "CPUs"]
        widths = [self.col_widths['job_id'], 
                 self.col_widths['name'], 
                 self.col_widths['state'],
                 self.col_widths['elapsed'],
                 self.col_widths['memory'],
                 self.col_widths['cpus']]
        
        # Define which columns should be left-aligned (True) or right-aligned (False)
        alignments = [True, True, True, True, False, False]  # JobID now left-aligned
        
        self.screen.addstr(start_line, 0, "Active Jobs:", curses.A_BOLD)
        
        # Draw headers
        header_str = self.format_table_row(headers, widths, alignments)
        self.screen.addstr(start_line + 1, 0, header_str, curses.A_BOLD)
        
        # Draw separator line
        separator = self.format_table_row(["-" * w for w in widths], widths)
        self.screen.addstr(start_line + 2, 0, separator)
        
        line = start_line + 3
        for job in jobs:
            if job and job['state'] in ['RUNNING', 'PENDING']:
                values = [
                    job['job_id'],
                    job['name'][:self.col_widths['name']],
                    job['state'],
                    job['elapsed'],
                    format_memory(job['memory']),
                    job['cpus']
                ]
                job_str = self.format_table_row(values, widths, alignments)
                self.screen.addstr(line, 0, job_str)
                line += 1
                
        return line + 1
    
    def run(self):
        """Main monitoring loop"""
        def _monitor(stdscr):
            self.screen = stdscr
            curses.start_color()
            curses.use_default_colors()
            
            while True:
                try:
                    # Clear screen
                    self.screen.clear()
                    
                    # Get job information
                    job_lines = get_slurm_jobs(self.workflow_id)
                    jobs = [parse_job_info(line) for line in job_lines]
                    jobs = [j for j in jobs if j]  # Remove None values
                    
                    # Update statistics
                    self.update_job_stats(jobs)
                    
                    # Draw dashboard components
                    self.draw_header()
                    current_line = 4
                    current_line = self.draw_stats(current_line)
                    current_line = self.draw_job_table(current_line, jobs)
                    
                    # Add help text
                    self.screen.addstr(current_line + 1, 0, "Press 'q' to quit")
                    
                    # Refresh screen
                    self.screen.refresh()
                    
                    # Check for quit command
                    self.screen.timeout(self.refresh_rate * 1000)
                    key = self.screen.getch()
                    if key == ord('q'):
                        break
                        
                except curses.error:
                    pass  # Handle screen resize errors
                    
        curses.wrapper(_monitor)

def main():
    parser = argparse.ArgumentParser(description='Monitor Snakemake jobs on Slurm')
    parser.add_argument('--workflow-id', help='Specific workflow ID to monitor')
    parser.add_argument('--refresh-rate', type=int, default=30,  # Changed default to 30 seconds
                      help='Refresh rate in seconds (default: 30)')
    args = parser.parse_args()
    
    monitor = SnakemakeMonitor(args.workflow_id, args.refresh_rate)
    monitor.run()

if __name__ == '__main__':
    main()
