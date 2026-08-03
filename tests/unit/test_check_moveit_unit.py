import pytest
import os
from unittest.mock import patch
from PanDAirflow.file_checkit import check_moveit


def test_check_moveit_unit():
    """
    Unit test for check_moveit that allows choosing:
      - mock MoveIt API calls (default)
      - OR run real MoveIt API calls

    Control via environment variable:
        USE_REAL_MOVEIT=true/false
    """

    # ----------------------------------------------------------------------
    # Load arguments from environment variables (PyCharm Run Config)
    # ----------------------------------------------------------------------
    file_name = os.getenv("FILENAME", "testfile")
    folder_path = os.getenv("FOLDER_PATH", None)

    file_types = os.getenv("FILE_TYPES", None)

    mi_user = os.getenv("MI_USER")
    mi_pass = os.getenv("MI_PASS")

    use_real_moveit = os.getenv("USE_REAL_MOVEIT", "false").lower() == "true"

    # ----------------------------------------------------------------------
    # ALWAYS mock Airflow Variable.get
    # (because unit tests do not run inside Airflow runtime context)
    # ----------------------------------------------------------------------
    with patch("airflow.sdk.definitions.variable.Variable.get") as mock_var_get:
        mock_var_get.side_effect = lambda key: {
            "moveit_username": mi_user,
            "moveit_password": mi_pass,
        }[key]

        # ------------------------------------------------------------------
        # If using real MoveIt API, DO NOT mock auth_move_it or available_files
        # ------------------------------------------------------------------
        if not use_real_moveit:
            # Mock both network/API calls
            with patch("PanDAirflow.file_checkit.auth_move_it") as mock_auth, \
                 patch("PanDAirflow.file_checkit.available_files") as mock_files:

                mock_auth.return_value = {"token": "mock_token"}
                mock_files.return_value = [
                    {"path": folder_path, "name": f"{file_name}{file_types[0]}"},
                ]

                # Run underlying Python logic
                result = check_moveit.__wrapped__(
                    file_name=file_name,
                    folder_path=folder_path,
                    file_types=file_types,
                )

        else:
            # ------------------------------------------------------------------
            # REAL MoveIt API CALLS HAPPEN HERE
            #
            # No mocks applied — uses your real PanDAirflow MoveIt endpoints.
            # Requires remote VM interpreter & correct network access.
            # ------------------------------------------------------------------
            result = check_moveit.__wrapped__(
                file_name=file_name,
                folder_path=folder_path,
                file_types=file_types,
            )

        print("DONE:", result.is_done)
        print("XCOM:", result.xcom_value)