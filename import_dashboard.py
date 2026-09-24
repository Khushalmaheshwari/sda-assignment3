import base64
import json
import urllib.error
import urllib.request

DATA_SOURCE_UID = "cfz5f5bjqs5c0d"
DASHBOARD_PATH = "grafana/dashboard_ev_risk.json"

with open(DASHBOARD_PATH, encoding="utf-8") as f:
    dash = json.load(f)


def replace_uid(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "uid" and v == "${DS_EV_STATIONS}":
                out[k] = DATA_SOURCE_UID
            else:
                out[k] = replace_uid(v)
        return out
    if isinstance(obj, list):
        return [replace_uid(x) for x in obj]
    return obj


dash = replace_uid(dash)
dash.pop("__inputs", None)
dash.pop("__requires", None)
dash.pop("id", None)

payload = json.dumps({"dashboard": dash, "overwrite": True}).encode()

req = urllib.request.Request(
    "http://localhost:3000/api/dashboards/db",
    data=payload,
    method="POST",
    headers={
        "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(),
        "Content-Type": "application/json",
    },
)

try:
    resp = urllib.request.urlopen(req)
    print(resp.read().decode())
except urllib.error.HTTPError as e:
    print("HTTP", e.code, e.read().decode())