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
    global global_output_hashes  # Use the global hash set

    if not os.path.exists("Suriya"):
        try:
            os.makedirs("Suriya")  # Create directory if it doesn't exist
            print("Folder 'Suriya' created successfully.")
        except Exception as e:
            print(f"Failed to create 'Suriya' folder: {e}")
            return

    match = re.search(r'radosgw-admin (\w+)', command)
    if match:
        subcommand = match.group(1)  # Extract the specific subcommand (e.g., "realm")
        file_name = f"Suriya/{subcommand}_outputs.json"

        # Load existing data or initialize new structure
        if os.path.exists(file_name):
            with open(file_name, 'r') as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError:
                    print(f"File {file_name} contains invalid JSON. Reinitializing.")
                    data = {"ceph_version": ceph_version, "outputs": []}
        else:
            data = {"ceph_version": ceph_version, "outputs": []}

        if "outputs" not in data:
            data["outputs"] = []

        # Generate hash based only on the output
        output_hash = hashlib.sha256(json.dumps(output, sort_keys=True).encode('utf-8')).hexdigest()

        # Check for duplicates globally and within the file
        if output_hash in global_output_hashes:
            print(f"Global duplicate output found for output_hash: {output_hash} - skipping.")
            return

        duplicate_found = any(entry["output_hash"] == output_hash for entry in data["outputs"])

        if not duplicate_found:
            # Append the new output
            data["outputs"].append({
                "command": command,
                "output": output,
                "output_hash": output_hash,
            })

            # Save back to the JSON file
            try:
                with open(file_name, 'w') as file:
                    json.dump(data, file, indent=4)
                print(f"Output saved to {file_name}")
            except Exception as e:
                print(f"Error saving to {file_name}: {e}")

            # Add the hash to the global set
            global_output_hashes.add(output_hash)
        else:
            print(f"File-level duplicate output found for output_hash: {output_hash} - skipping.")



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
                if 'Execute cephadm shell -- radosgw-admin' in lines[i]:
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

    subcomponent_filter = input("\nEnter a subcomponent filter (or press Enter to skip): ").strip()

    return c, subcomponent_filter

# Base URL to start processing logs
url = "http://magna002.ceph.redhat.com/cephci-jenkins/results/openstack/RH/"

# Start interactive folder navigation and log file processing
complete_url, sb = navigation_folder(url)
print("\nFinal URL:", complete_url)

# Process all log files at the selected URL with the specified filter
process_all_log_files(complete_url, sb)
