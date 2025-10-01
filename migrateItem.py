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

    # logging.info(f"Creating workspace item with payload: {json.dumps(item)}")
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
    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception(f"No CSRF token found before patch: {session.cookies.get_dict()}")

    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"

    allowed_fields = {
        "dc.contributor.author",
        "dc.title",
        "dc.title.alternative",
        "dc.casetype",
        "dc.caseyear",
        "dc.cino",
        "dc.judge.name",
        "dc.pname",
        "dc.rname",
        "dc.raname",
        "dc.paname",
        "dc.case.cnrno",
        "dc.district",
        "dc.date.scan",
        "dc.case.approveby",
        "dc.date.verification",
        "dc.barcode",
        "dc.batch-number",
        "dc.size",
        "dc.date.disposal",
        "dc.char-count",
        "dc.date.issued",
        "dc.publisher",
        "dc.identifier.citation",
        "dc.relation.ispartofseries",
        "dc.identifier",
        "dc.language.iso",
    }


    patch_body = []

    for field, values in metadata_dict.items():
        if field not in allowed_fields:
            logging.warning(f"Skipping unsupported metadata field: {field}")
            continue

        # sanitize values: convert empty language to None
        clean_values = []
        for v in values:
            clean_values.append({
                "value": v.get("value"),
                "language": v.get("language") if v.get("language") else None
            })

        patch_body.append({
            "op": "add",
            "path": f"/sections/traditionalpageone/{field}",
            "value": clean_values
        })
        patch_body.append({
            "op": "add",
            "path": "/sections/traditionalpageone/dc.date.issued",
            "value": [{"value": "2017-01-01", "language": None}]
        })
        patch_body.append({
            "op": "add",
            "path": "/sections/traditionalpageone/dc.cino",
            "value": [{"value": "INVALID12345", "language": None}]
        })

    # always add license granted
    patch_body.append({
        "op": "add",
        "path": "/sections/license/granted",
        "value": True
    })

    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf_token,
        "Content-Type": "application/json-patch+json",
        "Accept": "application/json"
    }

    logging.info(f"Patching metadata for workspace {workspace_id}")
    resp = session.patch(url, headers=headers, json=patch_body)
    logging.info(f"Patch metadata resp {resp.status_code}")

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

    # logging.info(f"Bitstream upload resp {resp.status_code}: {resp.text}")
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
def fetch_item_metadata(item_id):
    conn = psycopg2.connect(**SRC_DB)
    cur = conn.cursor()
    query = """
    SELECT
        ms.short_id || '.' || mf.element || 
        COALESCE('.' || mf.qualifier, '') AS metadata_field,
        mv.text_value,
        mv.text_lang
    FROM item i
    JOIN metadatavalue mv ON i.uuid = mv.dspace_object_id
    JOIN metadatafieldregistry mf ON mv.metadata_field_id = mf.metadata_field_id
    JOIN metadataschemaregistry ms ON mf.metadata_schema_id = ms.metadata_schema_id
    WHERE i.item_id = %s
    ORDER BY metadata_field, mv.place
    """
    cur.execute(query, (item_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    metadata = {}
    for field, value, language in rows:
        entry = {"value": value, "language": language}
        if field in metadata:
            metadata[field].append(entry)
        else:
            metadata[field] = [entry]
    return metadata

def get_db_rows(limit=None):
    conn = psycopg2.connect(**SRC_DB)
    cur = conn.cursor()
    q = f"SELECT {', '.join(COMMON_COLUMNS)} FROM item"
    # print(q)
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q)
    rows = cur.fetchall()
    # print(rows)
    cur.close()
    conn.close()
    return rows


# ---------------------------
# submit_to_workflow (kept mostly as your original)
# ---------------------------
def submit_to_workflow(session, workspace_id):
    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception(f"No CSRF token found: {session.cookies.get_dict()}")

    auth = session.headers.get("Authorization")
    url = f"{BASE_URL}/workflow/workflowitems?embed=item,sections,collection"
    body = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"

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
                if not ws_id:
                    raise Exception("No workspace ID returned after creation")
                data = fetch_item_metadata(processed["item_id"])
                # logging.info(data)
                # print(data)
                patch_metadata(session, ws_id, data)

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
