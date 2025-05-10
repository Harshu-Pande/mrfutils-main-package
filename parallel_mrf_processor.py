import os
import glob
from multiprocessing import Pool, cpu_count
import subprocess
from pathlib import Path

def process_npi_file(npi_file):
    """
    Process a single NPI file using the example_cli.py script
    """
    # Create output directory based on NPI filename
    npi_filename = Path(npi_file).stem
    output_dir = f"output_{npi_filename}"
    
    # Construct the command
    cmd = [
        "python3",
        "transparency-in-coverage/python/mrfutils/examples/example_cli.py",
        "--out-dir", output_dir,
        "--npi-file", npi_file,
        "--url", "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ASH/2024-10-05/inNetworkRates/2024-10-05_pl-4b-hr23_Aetna-Life-Insurance-Company.json.gz",
        "--file", "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/Med Reveal/Data Analysis/Unprocessed full Insurance Files/UHC_PPO_P3_3_1_25.json.gz"
    ]
    
    try:
        # Run the command
        subprocess.run(cmd, check=True)
        print(f"Successfully processed {npi_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {npi_file}: {str(e)}")

def main():
    # Path to your NPI files directory
    npi_files_dir = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/Med Reveal/Data Analysis/NPI_ONLY_FILES"
    
    # Get all CSV files in the directory
    npi_files = glob.glob(os.path.join(npi_files_dir, "*.csv"))
    
    if not npi_files:
        print("No NPI files found in the specified directory!")
        return
    
    # Get the number of CPU cores
    num_processes = cpu_count()
    print(f"Found {len(npi_files)} NPI files to process")
    print(f"Using {num_processes} CPU cores for parallel processing")
    
    # Create a pool of workers and process files in parallel
    with Pool(processes=num_processes) as pool:
        pool.map(process_npi_file, npi_files)

if __name__ == "__main__":
    main() 