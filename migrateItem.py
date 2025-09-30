import os
import json
import logging
import requests
import psycopg2

BaseException  # quiet lint if unused in some environments

BASE_URL = "http://localhost:8080/server/api"
USERNAME = "admin@gmail.com"
PASSWORD = "admin"
COLLECTION_UUID = "ff5a7efe-6537-441f-a7d8-453fc1ddf0f4"
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


# ---------------------------
# Keep your original login function UNCHANGED (exact logic preserved)
# ---------------------------

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


# ---------------------------
# Helper to fetch/refresh CSRF token from session cookies (used inside the functions we modify)
# ---------------------------
def _get_csrf_from_session(session):
    """
    Try to return a usable CSRF token. If not present, attempt a quick GET to /authn/status
    to allow the server to set cookies, then re-read. Returns the token string or None.
    """
    token = (
        session.cookies.get("DSPACE-XSRF-COOKIE")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("csrftoken")
    )
    if token:
        return token

    # Try hitting status to refresh cookies (safe, does not change login)
    try:
        rr = session.get(f"{BASE_URL}/authn/status", headers={"Accept": "application/json"})
        logging.info(f"Refreshed authn/status {rr.status_code}")
    except Exception as e:
        logging.info(f"authn/status refresh failed: {e}")

    token = (
        session.cookies.get("DSPACE-XSRF-COOKIE")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("csrftoken")
    )
    return token


# ---------------------------
# Modified: create_workspaceitem()
# (keeps same signature as your original but robustly re-reads CSRF cookie before request)
# ---------------------------
def create_workspaceitem(session, item):
    """
    Create a workspace item. This function will re-check the session cookies for a valid CSRF token
    and refresh /authn/status once if necessary.
    """
    ws_url = f"{BASE_URL}/submission/workspaceitems"

    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception(f"No CSRF token in session cookies before workspace create: {session.cookies.get_dict()}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": csrf_token,
        "Authorization": session.headers.get("Authorization")
    }

    logging.info(f"Creating workspace item with payload: {json.dumps(item)}")
    resp = session.post(ws_url, json=item, headers=headers)
    # logging.info(f"Create workspace resp {resp.status_code}: {resp.text}")
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to create workspace item: {resp.status_code} {resp.text}")

    try:
        ws_json = resp.json()
    except ValueError:
        ws_json = None

    ws_id = ws_json.get("id") if ws_json else None
    logging.info(f"Workspace item created: {ws_id}")
    return ws_id, ws_json




def patch_metadata(session, workspace_id, metadata_dict):
    """
    Replace the traditionalpageone metadata block for the workspace item using JSON-Patch.
    metadata_dict should be a mapping like:
      { "dc.title": [{ "value": "My title", "language": None }, ...], ... }
    """
    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception(f"No CSRF token found before patch: {session.cookies.get_dict()}")

    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"
    patch_body = [
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.contributor.author",
        "value": [{"value": "Default Author", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.title",
        "value": [{"value": "Default Case Number", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.casetype",
        "value": [{"value": "Default Case Type", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.caseyear",
        "value": [{"value": "2025", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.cino",
        "value": [{"value": "Default CINO", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.judge.name",
        "value": [{"value": "Default Judge", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.pname",
        "value": [{"value": "Default Petitioner", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.rname",
        "value": [{"value": "Default Respondent", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.raname",
        "value": [{"value": "Default RA", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.paname",
        "value": [{"value": "Default PA", "language": None}]
    },
    {
        "op": "add",
        "path": "/sections/license/granted",
        "value": True
    },
    {
        "op": "add",
        "path": "/sections/traditionalpageone/dc.date.issued",
        "value": [{"value": "2025-09-30", "language": None}]    
    }
]


    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf_token,
        "Content-Type": "application/json-patch+json",
        "Accept": "application/json"
    }

    logging.info(f"Patching metadata for workspace {workspace_id}: {json.dumps(patch_body)}")
    resp = session.patch(url, headers=headers, json=patch_body)
    logging.info(f"Patch metadata resp {resp.status_code}: {resp.text}")

    if resp.status_code not in (200, 201):
        raise Exception(f"Patch metadata failed: {resp.status_code} {resp.text}")

    try:
        return resp.json()
    except ValueError:
        return None


# ---------------------------
# Keep upload_bitstream and other logic as-is (only minor CSRF read added)
# ---------------------------
def upload_bitstream(session, workspace_id, file_path):
    filename = os.path.basename(file_path)
    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"  # keep original endpoint as you had it

    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception(f"No CSRF token in session cookies before upload: {session.cookies.get_dict()}")

    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf_token,
        "Accept": "application/json"
    }

    with open(file_path, "rb") as f:
        files = {"file": (filename, f)}
        resp = session.post(url, headers=headers, files=files)

    logging.info(f"Bitstream upload resp {resp.status_code}: {resp.text}")
    if resp.status_code not in (200, 201):
        raise Exception(f"File upload failed: {resp.status_code} {resp.text}")

    logging.info(f"Bitstream uploaded to workspace item {workspace_id}")
    try:
        return resp.json()
    except ValueError:
        return None


# ---------------------------
# DB helper (unchanged)
# ---------------------------
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


# ---------------------------
# Utility to build metadata dict from a 'processed' row (you can adapt fields as needed)
# ---------------------------
def build_metadata_dict(processed):
    """
    Build the metadata mapping for /sections/traditionalpageone/metadata
    using all fields from your traditionalpageone form.
    Each value is a list of objects with at least 'value' and optional 'language'.
    Defaults are provided if processed dict has no value.
    """
    last_mod = processed.get("last_modified")
    date_str = None
    if last_mod:
        try:
            date_str = last_mod.date().isoformat()
        except Exception:
            date_str = str(last_mod)

    return {
        "dc.contributor.author": [{"value": processed.get("dc.contributor.author", "Default Author"), "language": None}],
        "dc.title": [{"value": processed.get("dc.title", "Default Title"), "language": None}],
        "dc.title.alternative": [{"value": processed.get("dc.title.alternative", "Default Alt Title"), "language": None}],
        "dc.casetype": [{"value": processed.get("dc.casetype", "Default Case Type"), "language": None}],
        "dc.caseyear": [{"value": processed.get("dc.caseyear", "2025"), "language": None}],
        "dc.cino": [{"value": processed.get("dc.cino", "Default CINO"), "language": None}],
        "dc.judge.name": [{"value": processed.get("dc.judge.name", "Default Judge"), "language": None}],
        "dc.pname": [{"value": processed.get("dc.pname", "Default Petitioner"), "language": None}],
        "dc.rname": [{"value": processed.get("dc.rname", "Default Respondent"), "language": None}],
        "dc.raname": [{"value": processed.get("dc.raname", "Default RA"), "language": None}],
        "dc.paname": [{"value": processed.get("dc.paname", "Default PA"), "language": None}],
        "dc.case.cnrno": [{"value": processed.get("dc.case.cnrno", "Default CNR"), "language": None}],
        "dc.district": [{"value": processed.get("dc.district", "Default District"), "language": None}],
        "dc.date.scan": [{"value": processed.get("dc.date.scan", date_str or "2025-01-01"), "language": None}],
        "dc.case.approveby": [{"value": processed.get("dc.case.approveby", "Default Approver"), "language": None}],
        "dc.date.verification": [{"value": processed.get("dc.date.verification", "2025-01-01"), "language": None}],
        "dc.barcode": [{"value": processed.get("dc.barcode", "Default Barcode"), "language": None}],
        "dc.batch-number": [{"value": processed.get("dc.batch-number", "Default Batch"), "language": None}],
        "dc.size": [{"value": processed.get("dc.size", "Default Size"), "language": None}],
        "dc.date.disposal": [{"value": processed.get("dc.date.disposal", "2025-01-01"), "language": None}],
        "dc.char-count": [{"value": processed.get("dc.char-count", "0"), "language": None}],
        "dc.date.issued": [{"value": processed.get("dc.date.issued", date_str or "2025-01-01"), "language": None}],
        "dc.publisher": [{"value": processed.get("dc.publisher", "Default Publisher"), "language": None}],
        "dc.identifier.citation": [{"value": processed.get("dc.identifier.citation", "Default Citation"), "language": None}],
        "dc.relation.ispartofseries": [{"value": processed.get("dc.relation.ispartofseries", "Default Series"), "language": None}],
        "dc.identifier": [{"value": processed.get("dc.identifier", "Default Identifier"), "language": None}],
        "dc.language.iso": [{"value": processed.get("dc.language.iso", "en"), "language": None}]
    }



# ---------------------------
# submit_to_workflow (kept mostly as your original)
# ---------------------------
def submit_to_workflow(session, workspace_id):
    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception(f"No CSRF token found: {session.cookies.get_dict()}")

    auth = session.headers.get("Authorization")
    url = f"{BASE_URL}/workflow/workflowitems?embed=item,sections,collection"
    body = f"/server/api/submission/workspaceitems/{workspace_id}"

    headers = {
        "Authorization": auth,
        "X-XSRF-TOKEN": csrf_token,
        "Accept": "application/json",
        "Content-Type": "text/uri-list",
    }

    resp = session.post(url, headers=headers, data=body)
    logging.info(f"Submit to workflow resp {resp.status_code}: {resp.text}")
    if resp.status_code in (200, 201):
        try:
            wf_json = resp.json()
            return wf_json.get("id"), wf_json
        except ValueError:
            return None, None

    raise Exception(f"Submit to workflow failed: {resp.status_code} {resp.text}")


# ---------------------------
# Main migration flow (uses the modified create_workspaceitem + patch_metadata)
# ---------------------------
def migrate(limit=None):
    try:
        session = get_logged_in_session()
        rows = get_db_rows(limit=limit)
        for row in rows:
            processed = {
                col: (val if val is not None else DEFAULTS.get(col))
                for col, val in zip(COMMON_COLUMNS, row)
            }

            # create a minimal workspace item (license only) so the API creates the sections
            create_payload = {
                "submissionDefinition": "traditional",
                "owningCollection": processed.get("owning_collection") or COLLECTION_UUID,
                "sections": {
                    "license": {"granted": True}
                }
            }

            try:
                ws_id, ws_json = create_workspaceitem(session, create_payload)

                # build metadata and patch it
                metadata_dict = build_metadata_dict(processed)
                patch_metadata(session, ws_id, metadata_dict)

                # upload file (keeps your original upload logic)
                if FILE_PATH:
                    upload_bitstream(session, ws_id, FILE_PATH)

                # submit to workflow (your original logic)
                wf_id, wf_json = submit_to_workflow(session, ws_id)
                logging.info(
                    f"Successfully migrated DB item {processed['item_id']} → workflow item {wf_id}"
                )
                print(f"Workflow item created: {wf_id}")
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
