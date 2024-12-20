"""
This script is designed to recursively fetch, process, and save log files from a given base URL. 
It navigates through folder structures, filters specific log files, extracts command outputs in JSON format, 
and stores them in a structured way for further analysis.

Features:
1. Folder Navigation: Allows the user to interactively navigate through nested folder structures on the web server.
2. Log File Fetching: Recursively fetches all `.log` files from the selected folder and its subdirectories.
3. Command Extraction: Extracts specific commands (`radosgw-admin`) and their associated JSON outputs from log files.
4. Output Saving: Saves the extracted commands and outputs in a JSON file organized by subcommands.
5. Custom Filtering: Allows the user to filter logs based on specific subcomponents (e.g., `rgw`).

How the Script Works:
1. Interactive Navigation:
   - The `navigation_folder` function provides an interface for the user to navigate through the directory structure.
   - The user can select a folder, exit navigation, or specify a filter for processing subcomponents.

2. Fetching Log Links:
   - The `fetch_log_links` function recursively collects links to `.log` files from the base URL.
   - The links are stored in a dictionary for processing.

3. Processing Log Files:
   - The `process_log_file` function downloads each `.log` file, extracts commands, parses JSON outputs, and saves them in a structured format.
   - Commands already processed are skipped to avoid duplication.

4. Saving Outputs:
   - The `save_to_json` function organizes outputs into files named after subcommands (e.g., `radosgw-admin bucket outputs`) and adds new data while preserving existing content.

5. Folder Listing and Navigation:
   - The `fdf` function retrieves a list of folders in the current directory.
   - This supports the `navigation_folder` function in presenting folder options to the user.

Key Functions:
- `fetch_log_links(url, base_url, allow)`: Recursively fetches `.log` file links from the given URL.
- `process_log_file(file_url, pc)`: Processes a single `.log` file and extracts JSON outputs.
- `save_to_json(command, output, ceph_version)`: Saves extracted commands and outputs to a JSON file.
- `fdf(base_url)`: Fetches a list of folders from the current URL.
- `navigation_folder(base_url)`: Facilitates user interaction to navigate folders.

Prerequisites:
- Install the required Python libraries (`requests`, `BeautifulSoup`).
- Ensure the base URL is accessible from the environment where the script is executed.

Folder Structure:
- The script creates a folder named "Suriya" in the working directory.
- JSON output files are saved within this folder, organized by subcommands.
"""
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import os
import re
import hashlib  # For generating hashes to track unique outputs

# Dictionary to keep track of log links found
log_links_dict = {}
global_output_hashes = set()


# Function to fetch all .log file links recursively from a given URL
def fetch_log_links(url, base_url=None, allow=True):
    if base_url is None:
        base_url = url

    # Initialize the log links for the given URL if not already present
    if url not in log_links_dict:
        log_links_dict[url] = []

    try:
        # Make a request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP requests that fail
        page_content = response.text

        # Parse the page content using BeautifulSoup
        soup = BeautifulSoup(page_content, 'html.parser')

        # Iterate over all <a> tags with an href attribute
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(url, href)  # Convert relative URLs to absolute URLs

            # If the link is to a .log file, add it to the log_links_dict
            if href.endswith('.log'):
                if not any(log["opt_in"] == absolute_url for log in log_links_dict[url]):
                    log_links_dict[url].append({"opt_in": absolute_url})

            # If the link is a directory, recursively fetch links (only once per directory)
            elif href.endswith('/') and allow:
                fetch_log_links(absolute_url, base_url, False)

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")

# Function to save command outputs to a JSON file, ensuring no duplicate outputs

def save_to_json(command, output, ceph_version):
    global global_output_hashes
    global complete_url

    # Base directory
    base_dir = "Suriya"
    os.makedirs(base_dir, exist_ok=True)

    # Extract OpenStack version, RHEL version, and Ceph version
    url_parts = complete_url.split("/")
    openstack_version = url_parts[7]  # Example: "openstack", "openstack-v2"
    rhel_version = url_parts[8]      # Example: "RH8.6", "rh9.0"
    ceph_version_full = url_parts[10]  # Example: "16.2.10", "19.3.0-12"

    # Construct directory hierarchy
    openstack_dir = os.path.join(base_dir, openstack_version)  # e.g., "Suriya/openstack"
    rhel_version_dir = os.path.join(openstack_dir, rhel_version)  # e.g., "Suriya/openstack/RH8.6"
    ceph_version_dir = os.path.join(rhel_version_dir, ceph_version_full)  # e.g., "Suriya/openstack/RH8.6/16.2.10"

    # Create directories if they do not exist
    os.makedirs(ceph_version_dir, exist_ok=True)

    # Determine subcommand from command string
    match = re.search(r'radosgw-admin (\w+)', command)
    if match:
        subcommand = match.group(1)
        file_name = os.path.join(ceph_version_dir, f"{subcommand}_outputs.json")

        # Load existing JSON data or initialize a new one
        if os.path.exists(file_name):
            with open(file_name, 'r') as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError:
                    print(f"Invalid JSON in {file_name}. Initializing a new file.")
                    data = {"ceph_version": ceph_version_full, "outputs": []}
        else:
            data = {"ceph_version": ceph_version_full, "outputs": []}

        # Calculate a hash for the output
        output_hash = hashlib.sha256(json.dumps(output, sort_keys=True).encode('utf-8')).hexdigest()

        # Check for duplicates
        if output_hash in global_output_hashes:
            print(f"Duplicate output globally for hash {output_hash}. Skipping.")
            return

        if not any(entry["output_hash"] == output_hash for entry in data["outputs"]):
            data["outputs"].append({
                "command": command,
                "output": output,
                "output_hash": output_hash,
            })

            # Save updated data back to the JSON file
            with open(file_name, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"Saved output to {file_name}")

            # Add to global hashes
            global_output_hashes.add(output_hash)
        else:
            print(f"Duplicate output in file {file_name} for hash {output_hash}. Skipping.")



# Function to process a single log file URL
def process_log_file(file_url, pc=set()):
    response = requests.get(file_url)  # Fetch the log file content
    file_path = 'temp_file.log'  # Temporary file to store downloaded log content

    try:
        response.raise_for_status()  # Check for HTTP request errors
        with open(file_path, 'wb') as file:
            file.write(response.content)  # Save log file content locally

        ceph_version = None  # Variable to store the Ceph version found in the log

        with open(file_path, 'r') as file:
            lines = file.readlines()  # Read all lines from the log file
            print("=" * 50)
            print(file_url)  # Log the file being processed
            print("=" * 50)

            # Iterate through lines to find commands and their outputs
            for i in range(len(lines)):
                if 'cephadm shell -- radosgw-admin' in lines[i]:
                    # Extract Ceph version from the log
                    version_line = lines[i + 2].strip()
                    ceph_version = ".".join(version_line.split()[5].split(".")[6:9])

                    # Extract the radosgw-admin command
                    index = lines[i].find('radosgw-admin')
                    command = lines[i][index:].rstrip("\n")

                    # Skip already processed commands
                    if command in pc:
                        continue

                    stack = []  # Stack to track JSON structure
                    json_start = None  # Track the start of JSON content
                    json_content = ""

                    # Find and extract JSON output for the command
                    for j in range(i + 1, len(lines)):
                        for char in lines[j]:
                            if char == '{':
                                if not stack:
                                    json_start = j
                                stack.append('{')
                            if stack:
                                json_content += char
                            if char == '}':
                                stack.pop()
                                if not stack:
                                    break
                        if not stack and json_content:
                            break

                    if json_content:
                        # Clean and parse JSON content
                        cleaned_output_line = (
                            json_content.replace("'", "\"")
                            .replace("True", "true")
                            .replace("False", "false")
                            .strip()
                        )

                        try:
                            json_output = json.loads(cleaned_output_line)  # Parse JSON content
                        except json.JSONDecodeError:
                            json_output = None

                        # Save the JSON output if valid
                        if json_output:
                            save_to_json(command, json_output, ceph_version)
                            pc.add(command)

        os.remove(file_path)  # Remove the temporary log file

    except requests.RequestException as e:
        print(f"Failed to download file at url {file_url}: {e}")

# Function to process all log files starting from the base URL
def process_all_log_files(url, subcomponent_filter=None):
    fetch_log_links(url)  # Fetch all .log file links recursively
    pc = set()  # Set to track processed commands

    for directories in log_links_dict:
        log_files_dict_list = log_links_dict[directories]

        for log_files_dict in log_files_dict_list:
            # Extract the folder name to match the subcomponent filter
            folder_name = log_files_dict["opt_in"].split("/")[-2]

            # Skip processing if the folder name doesn't match the filter
            if subcomponent_filter and subcomponent_filter.lower() not in folder_name.lower():
                continue

            process_log_file(log_files_dict["opt_in"], pc)

# Function to fetch folder names from a given base URL
def fdf(base_url):
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        folder = []
        for i in soup.find_all('a', href=True):
            href = i['href']
            if href.endswith('/') and href != '../':
                folder.append(href.strip('/'))  # Append folder names without trailing slashes
        
        return folder
    except requests.RequestException as e:
        print("Request is not accepted", e)
        return []

# Function for interactive folder navigation
def navigation_folder(base_url):
    c = base_url
    subcomponent_filter = None

    while True:
        print(f"\nCurrent URL: {c}")
        folders = fdf(c)
        if not folders:
            print("No further folders available.")
            break

        print("\nAvailable Folders:")
        for i, folder in enumerate(folders, start=1):
            print(f"{i}. {folder}")
        print("0. TO run")

        choice = input("\nFolder to navigate (press Enter to select the last folder): ").strip()

        if choice == '0':
            print("Exiting navigation.")
            break

        if choice == "":
            selected_folder = folders[-1]
            c = urljoin(c, f"{selected_folder}/")
            print(f"Automatically selecting the last folder: {selected_folder}")
        else:
            try:
                ci = int(choice) - 1
                if 0 <= ci < len(folders):
                    selected_folder = folders[ci]
                    c = urljoin(c, f"{selected_folder}/")
                else:
                    print("Invalid choice.")
            except ValueError:
                print("Invalid input.")

    subcomponent_filter = input("\nEnter a subcomponent filter (like rgw,rbd,rados): ").strip()

    return c, subcomponent_filter

# Base URL to start processing logs
url = "http://magna002.ceph.redhat.com/cephci-jenkins/results/openstack/RH/"

# Start interactive folder navigation and log file processing
complete_url, sb = navigation_folder(url)
print("\nFinal URL:", complete_url)

# Process all log files at the selected URL with the specified filter
process_all_log_files(complete_url, sb)
