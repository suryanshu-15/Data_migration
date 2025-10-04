import requests
import logging

# --- CONFIGURATION ---
BASE_URL = "http://localhost:8080/server/api"
USERNAME = "admin@gmail.com"
PASSWORD = "admin"

USERS = [
    {"email": "enhira@dspace.com", "firstname": "Enhira", "lastname": "User", "canLogin": True},
    {"email": "samiya@dspace.com", "firstname": "Samiya", "lastname": "User", "canLogin": True}
]


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
    print(session.cookies.get_dict())
    
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

def create_eperson(session, user):
    url = f"{BASE_URL}/eperson/epersons"

    # refresh CSRF token just in case
    csrf_token = _get_csrf_from_session(session)
    headers = {
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": csrf_token,
        "Authorization": session.headers.get("Authorization"),
        "Accept": "application/json"
    }

    payload = {
        "email": user["email"],
        "metadata": {
            "eperson.firstname": [{"value": user["firstname"]}],
            "eperson.lastname": [{"value": user["lastname"]}],
        },
        "canLogIn": user.get("canLogin", True),
        "requireCertificate": False,
    }

    resp = session.post(url, headers=headers, json=payload)
    print(resp.status_code, resp.text)
    try:
        data = resp.json()
    except Exception:
        data = resp.text

    if resp.status_code in [200, 201]:
        print(f"✅ Created user {user['email']} -> {data}")
    else:
        print(f"❌ Failed for {user['email']}: {resp.status_code} -> {data}")

def main():
    session = get_logged_in_session()
    for user in USERS:
        create_eperson(session, user)


if __name__ == "__main__":
    main()
