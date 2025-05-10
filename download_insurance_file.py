import os
import subprocess
import shlex

# Ensure wget is installed
!apt-get install wget -qq

def download_gz_file(url, output_dir, filename):
    """
    Download a .gz file using wget with a progress bar.

    Args:
        url (str): URL of the .gz file.
        output_dir (str): Directory to save the file.
        filename (str): Name of the file to save.
    """
    # Fix filename: Replace invalid characters like slashes
    filename = filename.replace("/", "_")

    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create full output path
    output_path = os.path.join(output_dir, filename)

    try:
        print(f"Downloading file to: {output_path}")

        # Properly quote the URL and output path
        escaped_url = shlex.quote(url)
        escaped_path = shlex.quote(output_path)

        # Wget command with progress bar
        command = f"wget --progress=bar:force:noscroll {escaped_url} -O {escaped_path}"

        # Run command interactively so the progress bar is visible
        subprocess.run(command, shell=True, check=True)

        print("\nDownload completed successfully!")

    except subprocess.CalledProcessError as e:
        print(f"Error downloading file: {e}")

# Define URL and paths
url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-03-01/2025-03-01_United-Healthcare-Services-Inc-_Third-Party-Administrator_Options-PPO_P3_in-network-rates.json.gz?undefined"
output_dir = "/Users/harshupande/mrfutils-main-package"
filename = "UHC_PPO_P3_3_1_25.json.gz"  # ✅ Fixed filename

download_gz_file(url, output_dir, filename)