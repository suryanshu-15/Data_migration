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
    ws_url = f"{BASE_URL}/submission/workspaceitems"

    csrf_token = session.cookies.get("DSPACE-XSRF-COOKIE")
    if not csrf_token:
        raise Exception(f"No CSRF token in session cookies before workspace create: {session.cookies.get_dict()}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": csrf_token,
        "Authorization": session.headers.get("Authorization")
    }
    resp = session.post(ws_url, json=item, headers=headers)

    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to create workspace item: {resp.status_code} {resp.text}")

    ws_json = resp.json()
    ws_id = ws_json.get("id")
    logging.info(f"Workspace item created: {ws_id}")
    return ws_id, ws_json


import os

def upload_bitstream(session, workspace_id, file_path):
    filename = os.path.basename(file_path)
    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"
    # server/api/workflow/workflowitems?embed=item,sections,collection

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

# def submit_to_workflow(session, workspace_id):
#     """
#     Submit a workspace item to workflow in DSpace 7/10.
#     """
#     csrf_token = (
#         session.cookies.get("DSPACE-XSRF-COOKIE")
#         or session.cookies.get("XSRF-TOKEN")
#         or session.cookies.get("csrftoken")
#     )
#     if not csrf_token:
#         raise Exception(f"No CSRF token found: {session.cookies.get_dict()}")

#     auth = session.headers.get("Authorization")

#     url = f"{BASE_URL}/workflow/workflowitems"
#     body = f"/server/api/submission/workspaceitems/{workspace_id}"

#     headers = {
#         "Authorization": auth,
#         "X-XSRF-TOKEN": csrf_token,
#         "Accept": "application/json",
#         "Content-Type": "text/uri-list",
#     }

#     resp = session.post(url, headers=headers, data=body)
#     if resp.status_code in (200, 201):
#         wf_json = resp.json()
#         return wf_json.get("id"), wf_json

#     raise Exception(f"Submit to workflow failed: {resp.status_code} {resp.text}")



def get_db_rows(limit=None):
    conn = psycopg2.connect(**SRC_DB)
    cur = conn.cursor()
    q = f"SELECT {', '.join(COMMON_COLUMNS)} FROM item"
    print(q)
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q)
    rows = cur.fetchall()
    print(rows)
    cur.close()
    conn.close()
    return rows

def create_workspace_payload(processed):
    """
    Build workspace item payload with all required sections.
    """
    return {
        "submissionDefinition": "traditional",
        "owningCollection": processed.get("owning_collection") or COLLECTION_UUID,
        "sections": {
            "traditionalpageone": {
                "metadata": {
                    "dc.contributor.author": [
                        {"value": processed.get("submitter_id") or "Unknown author", "language": None}
                    ],
                    "dc.title.alternative": [
                        {"value": str(processed.get("item_id")), "language": None}
                    ],
                    "dc.publisher": [
                        {"value": processed.get("owning_collection") or "Unknown publisher", "language": None}
                    ],
                    "dc.date.issued": [
                        {
                            "value": processed.get("last_modified").date().isoformat()
                            if processed.get("last_modified") else "2025-09-12",
                            "language": None
                        }
                    ],
                    "dc.relation.ispartofseries": [
                        {"value": processed.get("uuid"), "language": None}
                    ],
                    "dc.identifier.citation": [
                        {"value": "respondent " + str(processed.get("item_id")), "language": None}
                    ],
                    "dc.identifier": [
                        {"value": "pet" + str(processed.get("item_id")), "language": None}
                    ],
                }
            },
            # Add required sections: license and upload
            "license": {"granted": True},
            "upload": {"files": []}  # we upload separately
        }
    }

def submit_to_workflow(session, workspace_id):
    """
    Submit workspace item to workflow in DSpace 10 Next.
    Returns (workflow_id, workflow_json) on success.
    """
    csrf_token = (
        session.cookies.get("DSPACE-XSRF-COOKIE")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("csrftoken")
    )
    if not csrf_token:
        raise Exception(f"No CSRF token found: {session.cookies.get_dict()}")

    auth = session.headers.get("Authorization")
    url = f"{BASE_URL}/workflow/workflowitems"
    body = f"/server/api/submission/workspaceitems/{workspace_id}"

    headers = {
        "Authorization": auth,
        "X-XSRF-TOKEN": csrf_token,
        "Accept": "application/json",
        "Content-Type": "text/uri-list",
    }

    resp = session.post(url, headers=headers, data=body)
    if resp.status_code in (200, 201):
        wf_json = resp.json()
        return wf_json.get("id"), wf_json

    raise Exception(f"Submit to workflow failed: {resp.status_code} {resp.text}")

def migrate(limit=None):
    try:
        session = get_logged_in_session()
        rows = get_db_rows(limit=limit)
        for row in rows:
            processed = {
                col: (val if val is not None else DEFAULTS.get(col))
                for col, val in zip(COMMON_COLUMNS, row)
            }

            metadata_payload = create_workspace_payload(processed)

            try:
                ws_id, ws_json = create_workspaceitem(session, metadata_payload)
                if FILE_PATH:
                    upload_bitstream(session, ws_id, FILE_PATH)
                wf_id, wf_json = submit_to_workflow(session, ws_id)
                print(f"Workflow item created: {wf_id}")
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
