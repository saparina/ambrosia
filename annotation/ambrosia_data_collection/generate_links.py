import os
import argparse

import os
import argparse

def generate_links(directory_name, print_mode):
    links_info = []  # List to store tuples of (userid, sessionid)

    # Traverse the directory
    for root, dirs, files in os.walk(directory_name):
        for dir_name in dirs:
            # Split the directory name by underscore
            parts = dir_name.split('_')
            # Check if the split parts match the expected format
            if len(parts) == 2 and all(part.isalnum() for part in parts) and os.path.exists(os.path.join(root, dir_name, 'annotated_instances.jsonl')):
                userid, sessionid = parts
                links_info.append((userid, sessionid))

    # Sort the list by userid
    sorted_links_info = sorted(links_info, key=lambda x: x[0])

    # Print based on the mode specified
    for idx, (userid, sessionid) in enumerate(sorted_links_info):
        if print_mode == "link":
            link = f"http://localhost:8000/?PROLIFIC_PID={userid}&SESSION_ID={sessionid}"
            print(link)
        elif print_mode == "pid":
            print(userid)
        elif print_mode == "session":
            print(sessionid)
        elif print_mode == "idx":
            print(idx + 1)

def main():
    parser = argparse.ArgumentParser(description="Generate links, user IDs, or session IDs from directory names.")
    parser.add_argument('directory_name', type=str, help='The path to the directory to traverse.')
    parser.add_argument('mode', type=str, choices=['link', 'pid', 'session', 'idx'],
                        help='What to print: "link" for full links, "pid" for user IDs, or "session" for session IDs')
    
    args = parser.parse_args()
    
    generate_links(args.directory_name, args.mode)

if __name__ == '__main__':
    main()