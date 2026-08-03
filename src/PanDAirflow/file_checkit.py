from moveitAPI.moveit import auth_move_it, available_files
from airflow.sdk import Variable
from airflow.sdk import PokeReturnValue
from airflow.sdk import task
import pandas as pd
from typing import List, Optional


@task.sensor(mode="reschedule", poke_interval=43200, timeout=604800)
def check_moveit(file_name, folder_path=None, file_types: Optional[List[str]] = None):
    print("---- DEBUG: check_moveit sensor starting ----")
    print(f"Target file name: {file_name}")
    print(f"Target folder path: {folder_path}")
    print(f"Target file types: {file_types}")

    condition_met = False
    calls_filename = None
    base_url = "alleghenycounty.us"

    # ---------------------------------------------------------
    # Print credentials BEFORE making API call
    # ---------------------------------------------------------
    try:
        username = Variable.get("moveit_username")
        password = Variable.get("moveit_password")
        print(f"MoveIt username (from Variable): {username}")
    except Exception as e:
        print("ERROR: Could not load MoveIt credentials from Airflow Variables")
        print(e)
        return PokeReturnValue(is_done=False, xcom_value=None)

    # Build auth payload
    auth = f"grant_type=password&username={username}&password={password}"

    # ---------------------------------------------------------
    # Call MoveIt API
    # ---------------------------------------------------------
    print("Calling MoveIt auth API...")
    moveit_tokens = auth_move_it(base_url, auth)
    print("MoveIt tokens returned")

    print("Calling MoveIt available_files API...")
    moveit_files = available_files(base_url, moveit_tokens)
    print(f"MoveIt returned {len(moveit_files)} files.")

    # ---------------------------------------------------------
    # Folder filtering
    # ---------------------------------------------------------
    if folder_path is None:
        print("No folder_path provided, using all files returned.")
        name_k = moveit_files
    else:
        print(f"Filtering files by folder path startswith({folder_path}) ...")
        name_k = [d for d in moveit_files if d["path"].startswith(folder_path)]
        print(f"Files matching folder path: {len(name_k)}")

    # ---------------------------------------------------------
    # Filename filtering
    # ---------------------------------------------------------
    print(f"Filtering files for names containing '{file_name}'...")
    current_files = [d for d in name_k if file_name in d["name"]]
    print(f"Files matching file_name: {current_files}")

    current_month_fil = []

    # ---------------------------------------------------------
    # File type filtering
    # ---------------------------------------------------------
    if file_types is not None:
        print(f"Filtering file types: {file_types}")
        for file_type in file_types:
            matches = [d for d in current_files if d["name"].endswith(file_type)]
            print(f"Matches for file type '{file_type}': {matches}")
            current_month_fil.extend(matches)
    else:
        current_month_fil = current_files

    print(f"Final matching files after type filter: {current_month_fil}")

    # ---------------------------------------------------------
    # Decision logic
    # ---------------------------------------------------------
    if len(name_k) == 1:
        print("Folder group size == 1 case")
        if len(current_month_fil) != 0:
            condition_met = True
            calls_filename = name_k[0].get("name")
            print(f"SUCCESS: Found match: {calls_filename}")
        else:
            print("No matches found in single-folder group.")
            condition_met = False
            calls_filename = None

    elif len(name_k) > 1:
        print("Folder group size > 1 case")
        if len(current_month_fil) == 1:
            condition_met = True
            calls_filename = current_month_fil[0].get("name")
            print(f"SUCCESS: Found unique match: {calls_filename}")
        elif len(current_month_fil) > 1:
            print("ERROR: Multiple matches – ambiguous naming or repeated uploads")
            condition_met = False
        elif len(current_month_fil) == 0:
            print("No matches found in multi-folder group.")
            condition_met = False

    else:
        print("No files found matching folder_path filter.")
        print("Still waiting for file upload…")
        condition_met = False

    print(f"---- DEBUG: Sensor result: is_done={condition_met}, xcom_value={calls_filename} ----")
    return PokeReturnValue(is_done=condition_met, xcom_value=calls_filename)
