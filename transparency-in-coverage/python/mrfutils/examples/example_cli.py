"""
Example for how to use the functions in mrfutils.py

>>> python3 example_cli.py --npi-files npi1.csv npi2.csv npi3.csv --code-file <csvfile> --url <your_url>

If you plan on importing files with separate URLs, you can feed the URL
to the URL parameter of `json_mrf_to_csv`. Just add an additional command
line argument to do that.
"""
import argparse
import logging
import os
import csv
from multiprocessing import Pool, cpu_count
from pathlib import Path
import time

from mrfutils.helpers import import_csv_to_set
from mrfutils.flatteners import in_network_file_to_csv

def setup_logger(npi_file):
    """Setup a separate logger for each process"""
    logger = logging.getLogger(f'mrfutils_{os.getpid()}')
    logger.setLevel(logging.INFO)
    
    # Create file handler
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_dir, f"{Path(npi_file).stem}.log"))
    fh.setLevel(logging.INFO)
    
    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger

def import_npis_from_csv(file_path):
    """Import NPIs from CSV file, skipping header"""
    npi_set = set()
    with open(file_path, 'r') as f:
        # Skip header row
        next(f)
        for line in f:
            try:
                # Assuming NPI is the first column
                npi = line.strip().split(',')[0]
                if npi.isdigit():  # Only process if it's a valid number
                    npi_set.add(int(npi))
            except (ValueError, IndexError):
                continue
    return npi_set

def process_single_npi(args):
    """Process a single NPI file with the given parameters"""
    npi_file, file, url, code_filter, base_out_dir = args
    
    # Setup logger for this process
    logger = setup_logger(npi_file)
    logger.info(f"Starting processing of {npi_file}")
    start_time = time.time()
    
    try:
        # Create a unique output directory for this NPI file
        npi_name = Path(npi_file).stem
        out_dir = os.path.join(base_out_dir, npi_name)
        
        # Import NPI filter from file
        npi_filter = import_npis_from_csv(npi_file)
        logger.info(f"Loaded {len(npi_filter)} valid NPIs from {npi_file}")
        
        in_network_file_to_csv(
            file=file,
            url=url,
            npi_filter=npi_filter,
            code_filter=code_filter,
            out_dir=out_dir
        )
        
        end_time = time.time()
        logger.info(f"Completed processing {npi_file} in {end_time - start_time:.2f} seconds")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {npi_file}: {str(e)}")
        return False

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out-dir', default='csv_output')
parser.add_argument('-c', '--code-file')
parser.add_argument('-n', '--npi-files', nargs='+', required=True,
                    help='One or more NPI filter files to process in parallel')
parser.add_argument('-p', '--processes', type=int, default=None,
                    help='Number of parallel processes to use. Defaults to CPU count.')

args = parser.parse_args()

url = args.url
base_out_dir = args.out_dir

if args.code_file:
    code_filter = import_csv_to_set(args.code_file)
else:
    code_filter = None

# Prepare arguments for parallel processing
process_args = [
    (npi_file, args.file, url, code_filter, base_out_dir)
    for npi_file in args.npi_files
]

# Use multiprocessing to process NPI files in parallel
if __name__ == '__main__':
    num_processes = args.processes or max(1, cpu_count() - 1)  # Leave one CPU free
    print(f"Starting processing with {num_processes} parallel processes")
    
    with Pool(processes=num_processes) as pool:
        results = pool.map(process_single_npi, process_args)
    
    successful = sum(1 for r in results if r)
    failed = len(results) - successful
    print(f"\nProcessing completed:")
    print(f"- Successfully processed: {successful} files")
    print(f"- Failed to process: {failed} files")
    print(f"- Total files: {len(results)} files")
