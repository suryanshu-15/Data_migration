import requests

# ---------- CONFIG ----------
BASE_URL = "http://your-dspace-server/api"   # e.g. http://localhost:8080/server/api
USERNAME = "admin"
PASSWORD = "admin"
COLLECTION_UUID = "your-collection-uuid"     # The collection where the item goes
FILE_PATH = "example.pdf"                    # Local file you want to upload
# ----------------------------

# 1. Authenticate and get a token
login_url = f"{BASE_URL}/authn/login"
session = requests.post(login_url, data={
    "user": USERNAME,
    "password": PASSWORD
})

if session.status_code != 200:
    raise Exception(f"Login failed: {session.text}")

token = session.headers.get("Authorization")
headers = {"Authorization": token, "Content-Type": "application/json"}

print("✅ Logged in successfully!")

# 2. Create a workspace item with metadata
workspace_url = f"{BASE_URL}/submission/workspaceitems?owningCollection={COLLECTION_UUID}"

metadata = {
    "metadata": {
        "dc.title": [{"value": "My First Item", "language": "en"}],
        "dc.contributor.author": [{"value": "Sachinv, User"}],
        "dc.date.issued": [{"value": "2025-09-09"}],
        "dc.description.abstract": [{"value": "This is an item created via API"}]
    }
}

workspace = requests.post(workspace_url, headers=headers, json=metadata)

if workspace.status_code not in (200, 201):
    raise Exception(f"Item creation failed: {workspace.text}")

workspace_item = workspace.json()
workspace_id = workspace_item["id"]
print(f"Workspace item created: {workspace_id}")

# 3. Upload a bitstream (file)
bitstream_url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}/bitstreams"
files = {"file": open(FILE_PATH, "rb")}

upload_headers = {"Authorization": token}
upload = requests.post(bitstream_url, headers=upload_headers, files=files)

if upload.status_code not in (200, 201):
    raise Exception(f"File upload failed: {upload.text}")

print("✅ File uploaded successfully!")

# 4. Submit the workspace item (finalize)
submit_url = f"{BASE_URL}/workspaceitems/{workspace_id}/submit"
submit = requests.post(submit_url, headers=headers)

if submit.status_code not in (200, 201):
    raise Exception(f"Submit failed: {submit.text}")

final_item = submit.json()
print(f"Item created successfully! Item UUID: {final_item['id']}")