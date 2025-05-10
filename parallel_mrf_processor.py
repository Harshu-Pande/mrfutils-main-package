import os
import glob
from multiprocessing import Pool, cpu_count
import subprocess
from pathlib import Path
import argparse

def process_npi_file(args):
    """
    Process a single NPI file using the example_cli.py script
    """
    npi_file, base_output_dir, url, input_file = args
    # Create output directory based on NPI filename
    npi_filename = Path(npi_file).stem
    output_dir = os.path.join(base_output_dir, f"output_{npi_filename}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Construct the command
    cmd = [
        "python3",
        "transparency-in-coverage/python/mrfutils/examples/example_cli.py",
        "--out-dir", output_dir,
        "--npi-file", npi_file,
        "--url", url,
        "--file", input_file
    ]
    
    try:
        # Run the command
        subprocess.run(cmd, check=True)
        print(f"Successfully processed {npi_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {npi_file}: {str(e)}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process multiple NPI files in parallel')
    parser.add_argument('--npi-dir', required=True, 
                      help='Directory containing NPI CSV files')
    parser.add_argument('--output-dir', required=True,
                      help='Base directory for output files')
    parser.add_argument('--url', required=True,
                      help='URL for the MRF data')
    parser.add_argument('--file', required=True,
                      help='Input file path')
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Get all CSV files in the directory
    npi_files = glob.glob(os.path.join(args.npi_dir, "*.csv"))
    
    if not npi_files:
        print(f"No NPI files found in the specified directory: {args.npi_dir}")
        return
    
    # Get the number of CPU cores
    num_processes = cpu_count()
    print(f"Found {len(npi_files)} NPI files to process")
    print(f"Using {num_processes} CPU cores for parallel processing")
    print(f"Output will be saved in: {args.output_dir}")
    print(f"Using URL: {args.url}")
    print(f"Using input file: {args.file}")
    
    # Create arguments for each process
    process_args = [(npi_file, args.output_dir, args.url, args.file) for npi_file in npi_files]
    
    # Create a pool of workers and process files in parallel
    with Pool(processes=num_processes) as pool:
        pool.map(process_npi_file, process_args)

if __name__ == "__main__":
    main() 