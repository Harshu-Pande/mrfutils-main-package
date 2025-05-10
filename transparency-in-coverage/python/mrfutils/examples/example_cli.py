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
from multiprocessing import Pool
from pathlib import Path

from mrfutils.helpers import import_csv_to_set
from mrfutils.flatteners import in_network_file_to_csv

logging.basicConfig()
log = logging.getLogger('mrfutils')
log.setLevel(logging.DEBUG)

def process_single_npi(args):
    """Process a single NPI file with the given parameters"""
    npi_file, file, url, code_filter, base_out_dir = args
    
    # Create a unique output directory for this NPI file
    npi_name = Path(npi_file).stem
    out_dir = os.path.join(base_out_dir, npi_name)
    
    try:
        # Import NPI filter from file and convert to proper format
        raw_npi_set = import_csv_to_set(npi_file)
        # Extract just the NPI numbers from tuples if present, or use the raw value if it's not a tuple
        npi_filter = set()
        for item in raw_npi_set:
            if isinstance(item, tuple):
                # If it's a tuple, take the first element (assuming NPI is first)
                npi = str(item[0])
            else:
                # If it's not a tuple, use the value directly
                npi = str(item)
            try:
                # Convert to integer and add to set
                npi_filter.add(int(npi))
            except ValueError:
                log.warning(f"Skipping invalid NPI value: {npi}")
                continue
        
        in_network_file_to_csv(
            file=file,
            url=url,
            npi_filter=npi_filter,
            code_filter=code_filter,
            out_dir=out_dir
        )
    except Exception as e:
        log.error(f"Error processing {npi_file}: {str(e)}")

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out-dir', default='csv_output')
parser.add_argument('-c', '--code-file')
parser.add_argument('-n', '--npi-files', nargs='+', required=True,
                    help='One or more NPI filter files to process in parallel')

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
    # Use number of CPUs available for parallel processing
    with Pool() as pool:
        pool.map(process_single_npi, process_args)
