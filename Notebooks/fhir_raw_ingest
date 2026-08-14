import requests
import json
from datetime import datetime, timezone, timedelta


# PARAMETERS CELL

resource_types    = ["Patient","Encounter","Observation","Condition"]
days_back         = 3                # rolling incremental window, in days, ending "now"
page_size         = 50
base_url          = "https://hapi.fhir.org/baseR4"
raw_root_path     = "Files/raw"      # relative to the attached Lakehouse

# Compute a dynamic window ending at run time instead of hardcoded calendar
# dates — this is what makes the ingestion genuinely "incremental" each run,
# and avoids querying a fixed range that may hold little/no data on a shared
# public test server.
_now = datetime.now(timezone.utc)
lastUpdated_from = (_now - timedelta(days=days_back)).strftime("%Y-%m-%d")
lastUpdated_to   = _now.strftime("%Y-%m-%d")

# --------------------------------------------------------------
# Helpers
# --------------------------------------------------------------

def build_initial_url(resource_type: str, date_from: str, date_to: str, count: int) -> str:
    """Build the first-page FHIR search URL with an inclusive _lastUpdated window."""
    return (
        f"{base_url}/{resource_type}"
        f"?_lastUpdated=ge{date_from}"
        f"&_lastUpdated=le{date_to}"
        f"&_count={count}"
    )


def get_next_link(bundle: dict) -> str | None:
    """Pull the 'next' page URL out of a FHIR Bundle's link array, if present."""
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def fetch_bundle(url: str) -> tuple[dict, str]:
    """Call the FHIR API and return (parsed_json, raw_text). Raises on HTTP error."""
    response = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=60)
    response.raise_for_status()
    return response.json(), response.text


def write_raw_page(raw_text: str, resource_type: str, extract_date: str, page_num: int, source_url: str) -> str:
   
    file_path = f"{raw_root_path}/{resource_type}/{extract_date}/page_{page_num:03d}.json"

    # Wrap the payload with a couple of provenance fields alongside the raw body,
    # without touching the raw body itself — downstream Bronze reads .resource.body untouched.
    envelope = {
        "api_url_or_params": source_url,
        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_response": json.loads(raw_text),
    }

    notebookutils.fs.put(file_path, json.dumps(envelope), overwrite=True)
    return file_path


# --------------------------------------------------------------
# Main ingestion loop
# --------------------------------------------------------------

def ingest_resource(resource_type: str, date_from: str, date_to: str, count: int) -> list[str]:
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = build_initial_url(resource_type, date_from, date_to, count)

    written_files = []
    page_num = 1

    while url:
        print(f"[{resource_type}] fetching page {page_num}: {url}")
        bundle, raw_text = fetch_bundle(url)

        path = write_raw_page(raw_text, resource_type, extract_date, page_num, url)
        written_files.append(path)
        print(f"[{resource_type}] wrote {path} ({len(bundle.get('entry', []))} entries)")

        url = get_next_link(bundle)
        page_num += 1

    print(f"[{resource_type}] done — {page_num - 1} page(s), {len(written_files)} file(s) written.")
    return written_files



if __name__ == "__main__":
    results = {}
    for rt in resource_types:
        results[rt] = ingest_resource(rt, lastUpdated_from, lastUpdated_to, page_size)

    # Surface the written file list as the notebook's output value so the
    # orchestrating pipeline can log it or pass it to the next activity.
    notebookutils.notebook.exit(json.dumps({
        "resource_types": resource_types,
        "window": {"from": lastUpdated_from, "to": lastUpdated_to},
        "files_written": results,
    }))
