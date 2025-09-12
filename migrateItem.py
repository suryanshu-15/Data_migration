import requests
import json
import psycopg2
import logging

BASE_URL = "http://localhost:8080/server/api"
USERNAME = "admin@gmail.com"
PASSWORD = "admin"
COLLECTION_UUID = "b099e61d-572c-4034-b981-6f2e25a78da0"
FILE_PATH = "./example.pdf"

SRC_DB = {
    "dbname": "odisha_db",
    "user": "highcourt",
    "password": "highcourt",
    "host": "localhost",
    "port": 5432
}

COMMON_COLUMNS = [
    "item_id",
    "in_archive",
    "withdrawn",
    "last_modified",
    "discoverable",
    "uuid",
    "submitter_id",
    "owning_collection"
]

DEFAULTS = {
    "in_archive": True,
    "withdrawn": True,
    "discoverable": True,
    "last_modified": None,
    "item_id": 0,
    "uuid": "00000000-0000-0000-0000-000000000000",
    "submitter_id": "1",
    "owning_collection": COLLECTION_UUID
}

logging.basicConfig(
    filename="migration_full.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_logged_in_session():
    session = requests.Session()
    status_resp = session.post(
        f"{BASE_URL}/authn/status",
        headers={
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest"
        }
    )
    status_resp = session.get(f"{BASE_URL}/authn/status")
    
    if status_resp.status_code != 200:
        raise Exception(f"/authn/status failed: {status_resp.status_code} {status_resp.text}")

    csrf_token = session.cookies.get("DSPACE-XSRF-COOKIE") or session.cookies.get("XSRF-TOKEN") or session.cookies.get("csrftoken") 
    DSPACE_XSRF_TOKEN = session.cookies.get("DSPACE-XSRF-TOKEN")
    print(csrf_token)
    print(DSPACE_XSRF_TOKEN)
    if not csrf_token:
        raise Exception(f"No CSRF token found in cookies: {session.cookies.get_dict()}")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "X-XSRF-TOKEN": csrf_token,
        "DSPACE-XSRF-TOKEN": DSPACE_XSRF_TOKEN
    }
    data = {"user": USERNAME, "password": PASSWORD}
    login_resp = session.post(f"{BASE_URL}/authn/login", headers=headers, data=data)
    
    if login_resp.status_code != 200:
        raise Exception(f"Login failed: {login_resp.status_code} {login_resp.text}")

    jwt = login_resp.headers.get("Authorization")
    if not jwt:
        raise Exception("No Authorization JWT returned from login")
    session.headers.update({
        "Authorization": jwt,
        "X-XSRF-TOKEN": csrf_token
    })

    return session

def create_workspaceitem(session, item):
    """
    Create a workspace item in the configured collection.
    Assumes `session` already has Authorization and XSRF token in its headers.
    """
    ws_url = f"{BASE_URL}/submission/workspaceitems?owningCollection={COLLECTION_UUID}"

    csrf_token = session.cookies.get("DSPACE-XSRF-COOKIE")
    if not csrf_token:
        raise Exception(f"No CSRF token in session cookies before workspace create: {session.cookies.get_dict()}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": csrf_token,
        "Authorization": session.headers.get("Authorization")
    }
    # print()
    resp = session.post(ws_url, item ,headers=headers)

    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to create workspace item: {resp.status_code} {resp.text}")

    ws_json = resp.json()
    ws_id = ws_json.get("id")
    logging.info(f"Workspace item created: {ws_id}")
    return ws_id, ws_json


import os

def upload_bitstream(session, workspace_id, file_path):
    filename = os.path.basename(file_path)
    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}/bitstreams?name={filename}"

    csrf_token = (
        session.cookies.get("DSPACE-XSRF-COOKIE")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("csrftoken")
    )

    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf_token,
        "Accept": "application/json"
    }

    with open(file_path, "rb") as f:
        files = {"file": (filename, f)}
        resp = session.post(url, headers=headers, files=files)

    if resp.status_code not in (200, 201):
        raise Exception(f"File upload failed: {resp.status_code} {resp.text}")

    logging.info(f"Bitstream uploaded to workspace item {workspace_id}")
    return resp.json()


def submit_to_workflow(session, workspace_id):
    url = f"{BASE_URL}/workflow/workflowitems?embed=item,sections,collection"
    payload = {"workspaceItem": f"/server/api/submission/workspaceitems/{workspace_id}"}
    resp = session.post(url, json=payload)
    if resp.status_code not in (200, 201):
        raise Exception(f"Submit to workflow failed: {resp.status_code} {resp.text}")
    wf_json = resp.json()
    wf_id = wf_json.get("id")
    logging.info(f"Workflow item created: {wf_id} from workspace {workspace_id}")
    return wf_id, wf_json


def get_db_rows(limit=None):
    conn = psycopg2.connect(**SRC_DB)
    cur = conn.cursor()
    q = f"SELECT {', '.join(COMMON_COLUMNS)} FROM item"
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def migrate(limit=None):
    try:
        session = get_logged_in_session()
        rows = get_db_rows(limit=limit)

        for row in rows:
            processed = {
                col: (val if val is not None else DEFAULTS.get(col))
                for col, val in zip(COMMON_COLUMNS, row)
            }

            # Default metadata payload you showed above:
            metadata_payload = {
                "submissionDefinition": "traditional",
                "owningCollection": processed.get("owning_collection") or COLLECTION_UUID,
                "sections": {
                    "traditionalpageone": {
                        "dc.contributor.author": [
                            {"value": "Odisha state higher education council", "language": None}
                        ],
                        "dc.title.alternative": [
                            {"value": "102", "language": None}
                        ],
                        "dc.publisher": [
                            {"value": "file3", "language": None}
                        ],
                        "dc.date.issued": [
                            {"value": "2025-09-12", "language": None}
                        ],
                        "dc.relation.ispartofseries": [
                            {"value": "108", "language": None}
                        ],
                        "dc.identifier.citation": [
                            {"value": "respondent2", "language": None}
                        ],
                        "dc.identifier": [
                            {"value": "pet101", "language": None}
                        ],
                    }
                },
            }

            try:
                ws_id, ws_json = create_workspaceitem(session, metadata_payload)
                if FILE_PATH:
                    upload_bitstream(session, ws_id, FILE_PATH)
                wf_id, wf_json = submit_to_workflow(session, ws_id)
                logging.info(
                    f"Successfully migrated DB item {processed['item_id']} → workflow item {wf_id}"
                )
            except Exception as e:
                logging.error(
                    f"Error migrating DB item {processed['item_id']}: {str(e)}"
                )

        logging.info("Migration run complete.")
    except Exception as top_e:
        logging.error(f"Migration failed at top level: {str(top_e)}")
        print("Error: check log for details.")

if __name__ == "__main__":
    migrate(limit=1)
