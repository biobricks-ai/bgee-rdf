#!/usr/bin/env python3
"""
Download Bgee gene expression RDF data.

Bgee (https://bgee.org/) is the "Web of Biological Data" — a database of
gene expression patterns across animal species. It provides integrated
comparative data from curated experiments (RNA-Seq, microarray, in-situ,
EST) and is a key resource for understanding where genes are expressed.

The RDF dump follows the Bgee ontology and links to Gene Ontology, Uberon
anatomy ontology, and NCBI taxonomy.

Current size: ~2-5 GB compressed.
Source: https://bgee.org/data/
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

# Bgee RDF files — release 15.2 (latest as of 2025)
BGEE_VERSION = "15_2"
BASE_URL = f"https://bgee.org/ftp/bgee_v{BGEE_VERSION}"

# Key RDF files in the Bgee release
RDF_FILES = [
    f"rdf_data/bgee_{BGEE_VERSION}_rdf.ttl.gz",
]

# Fallback: the SPARQL endpoint data export
SPARQL_BASE = "https://bgee.org/sparql"

CHUNK_SIZE = 8 * 1024 * 1024


def check_url(url):
    """Check if a URL is accessible."""
    try:
        r = requests.head(url, timeout=30, allow_redirects=True)
        return r.status_code == 200, r.headers.get("content-length", 0)
    except Exception:
        return False, 0


def download_file(url, dest: Path, expected_size: int = 0):
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = dest.stat().st_size if dest.exists() else 0

    if existing and existing == int(expected_size):
        print(f"  Already complete: {dest.name}")
        return True

    headers = {}
    mode = "wb"
    if existing and expected_size and existing < int(expected_size):
        print(f"  Resuming from {existing:,} bytes: {dest.name}")
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"

    try:
        r = requests.get(url, headers=headers, stream=True, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return False

    total = int(expected_size) or int(r.headers.get("content-length", 0))
    bar = tqdm(
        total=total,
        initial=existing,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=dest.name,
    )
    with open(dest, mode) as f:
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))
    bar.close()
    return True


def try_bgee_ftp():
    """Try downloading from official Bgee FTP/HTTP."""
    download_dir = Path("download")
    metadata = {
        "version": BGEE_VERSION,
        "source": BASE_URL,
        "files": [],
        "download_date": datetime.now().isoformat(),
    }

    # Try primary RDF files
    for rel_path in RDF_FILES:
        url = f"{BASE_URL}/{rel_path}"
        fname = Path(rel_path).name
        dest = download_dir / fname

        print(f"Checking {url}...")
        ok, size = check_url(url)
        if ok:
            print(f"  Found: {fname} ({int(size)/1e6:.0f} MB)")
            if download_file(url, dest, size):
                metadata["files"].append({"name": fname, "url": url, "size_bytes": int(size)})
                return metadata
        else:
            print(f"  Not found at primary URL, trying alternatives...")

    # Try alternate naming conventions
    alt_patterns = [
        f"rdf/bgee_v{BGEE_VERSION}_rdf.ttl.gz",
        f"bgee_v{BGEE_VERSION}_rdf.ttl.gz",
        f"rdf/bgee_{BGEE_VERSION}.ttl.gz",
    ]
    for rel_path in alt_patterns:
        url = f"{BASE_URL}/{rel_path}"
        ok, size = check_url(url)
        if ok:
            fname = Path(rel_path).name
            dest = download_dir / fname
            print(f"  Found alt: {url}")
            if download_file(url, dest, size):
                metadata["files"].append({"name": fname, "url": url, "size_bytes": int(size)})
                return metadata

    return None


def try_bgee_index():
    """Parse the Bgee FTP index to find RDF files."""
    index_url = f"{BASE_URL}/"
    print(f"Scanning Bgee FTP index: {index_url}")
    try:
        r = requests.get(index_url, timeout=30)
        r.raise_for_status()
        # Find .ttl.gz or .rdf.gz links
        files = re.findall(r'href="([^"]+\.(?:ttl|rdf|nt|owl)\.gz)"', r.text)
        return files
    except Exception as e:
        print(f"  Could not read index: {e}")
        return []


def main():
    download_dir = Path("download")
    download_dir.mkdir(exist_ok=True)

    print(f"Bgee gene expression RDF download (version {BGEE_VERSION})")
    print(f"Source: {BASE_URL}")
    print()

    # Try to download
    metadata = try_bgee_ftp()

    if not metadata:
        # Try scanning the index
        rdf_files = try_bgee_index()
        if rdf_files:
            metadata = {
                "version": BGEE_VERSION,
                "source": BASE_URL,
                "files": [],
                "download_date": datetime.now().isoformat(),
            }
            for fname in rdf_files:
                url = f"{BASE_URL}/{fname}"
                dest = download_dir / Path(fname).name
                ok, size = check_url(url)
                if ok:
                    if download_file(url, dest, size):
                        metadata["files"].append({"name": Path(fname).name, "url": url, "size_bytes": int(size)})

    if not metadata or not metadata.get("files"):
        print(
            "\nERROR: Could not find Bgee RDF files at the expected locations.\n"
            "Please check https://bgee.org/data/ for the current download location\n"
            f"and update BASE_URL / RDF_FILES in this script.\n"
            f"Tried: {BASE_URL}",
            file=sys.stderr,
        )
        sys.exit(1)

    meta_path = download_dir / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    total = sum(f.get("size_bytes", 0) for f in metadata["files"])
    print(f"\nDownload complete: {len(metadata['files'])} files, {total/1e9:.2f} GB")
    print(f"Metadata: {meta_path}")


if __name__ == "__main__":
    main()
