import requests
from bs4 import BeautifulSoup
import urllib.parse
import time


class AirflowAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.jwt_token = None
        self.token_expiry = None

    def login(self, username: str, password: str):
        """
        Logs into Airflow using FABAuthManager, retrieves the CSRF token,
        submits username/password, extracts JWT `_token`.
        """
        login_url = f"{self.base_url}/auth/login/?next=%2F"

        # STEP 1 — GET login page to retrieve CSRF token
        login_page = self.session.get(login_url)
        soup = BeautifulSoup(login_page.text, "html.parser")

        csrf_token = soup.find("input", {"name": "csrf_token"})["value"]

        # STEP 2 — Encode password exactly as browser
        encoded_password = urllib.parse.quote(password)

        # STEP 3 — Construct correct form payload (NO html-escaped &amp;)
        payload = (
            f"csrf_token={csrf_token}"
            f"&username={username}"
            f"&password={encoded_password}"
        )

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": login_url
        }

        # STEP 4 — POST login request
        response = self.session.post(
            login_url,
            data=payload,
            headers=headers,
            allow_redirects=True
        )

        if response.status_code not in (200, 302):
            raise Exception(f"Login failed with HTTP {response.status_code}")

        # STEP 5 — Extract JWT
        token = self.session.cookies.get("_token")
        if not token:
            raise Exception("Login failed: No JWT token returned. Check credentials.")

        self.jwt_token = token
        # Optional: decode exp from JWT if needed
        # self.token_expiry = <parse JWT exp>
        print("Login successful.")

    def _auth_headers(self):
        """Returns Authorization header needed for API calls."""
        if not self.jwt_token:
            raise Exception("Not authenticated. Call login() first.")
        return {"Authorization": f"Bearer {self.jwt_token}"}

    def get(self, path: str):
        """Generic GET request helper."""
        url = f"{self.base_url}{path}"
        response = self.session.get(url, headers=self._auth_headers())
        response.raise_for_status()
        return response.json()

    def post(self, path: str, json=None):
        """Generic POST request helper."""
        url = f"{self.base_url}{path}"
        response = self.session.post(url, json=json, headers=self._auth_headers())
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------
    # Convenience API methods
    # ------------------------------------------------------

    def get_pools(self):
        return self.get("/api/v2/pools")

    def get_dags(self):
        return self.get("/api/v2/dags")

    def get_dag(self, dag_id: str):
        return self.get(f"/api/v2/dags/{dag_id}")

    def get_dag_runs(self, dag_id: str):
        return self.get(f"/api/v2/dags/{dag_id}/dagRuns")

    def trigger_dag(self, dag_id: str, conf: dict = None):
        """Trigger a DAG run."""
        data = {"conf": conf or {}}
        return self.post(f"/api/v2/dags/{dag_id}/dagRuns", json=data)

    def get_task_instances(self, dag_id: str, dag_run_id: str):
        return self.get(f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")

    def get_task_details(self, dag_id: str, task_id: str):
        return self.get(f"/api/v2/dags/{dag_id}/tasks/{task_id}/details")


# client = AirflowAPIClient("https://devairflow.alleghenycounty.us:8080")
#
# client.login("SA00427", "#County621Acc")
#
# pools = client.get_pools()
# print(pools)