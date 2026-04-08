#!/usr/bin/env python3
"""
Download Bgee gene expression RDF data.

Bgee (https://bgee.org/) is the "Web of Biological Data" — a database of
gene expression patterns across animal species. It provides integrated
comparative data from curated experiments (RNA-Seq, microarray, in-situ,
EST) and is a key resource for understanding where genes are expressed.

The RDF dump (rdf_easybgee.zip) follows the Bgee ontology and links to
Gene Ontology, Uberon anatomy ontology, and NCBI taxonomy.

Current size: ~28 GB (rdf_easybgee.zip).
Source: https://www.bgee.org/ftp/bgee_v15_2/
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

BGEE_VERSION = "15_2"
BASE_URL = f"https://www.bgee.org/ftp/bgee_v{BGEE_VERSION}"

RDF_FILES = [
    ("rdf_easybgee.zip", "Full Bgee RDF dump (EasyBgee subset)"),
]

CHUNK_SIZE = 8 * 1024 * 1024


def download_file(url, dest: Path, expected_size: int = 0):
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = dest.stat().st_size if dest.exists() else 0

    if existing and expected_size and existing == expected_size:
        print(f"  Already complete: {dest.name}")
        return

    headers = {}
    mode = "wb"
    if existing and expected_size and 0 < existing < expected_size:
        print(f"  Resuming from {existing/1e9:.2f} GB ({existing:,} bytes)")
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"

    r = requests.get(url, headers=headers, stream=True, timeout=60)
    r.raise_for_status()

    total = expected_size or int(r.headers.get("content-length", 0))
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


def main():
    download_dir = Path("download")
    download_dir.mkdir(exist_ok=True)

    # Remove any bad HTML stub files from previous attempts
    for f in download_dir.iterdir():
        if f.suffix in (".gz", ".zip") and f.stat().st_size < 10_000:
            print(f"Removing stub file: {f.name} ({f.stat().st_size} bytes)")
            f.unlink()

    print(f"Bgee {BGEE_VERSION} RDF download")
    print(f"Source: {BASE_URL}\n")

    metadata = {
        "version": BGEE_VERSION,
        "source": BASE_URL,
        "files": [],
        "download_date": datetime.now().isoformat(),
    }

    for fname, description in RDF_FILES:
        url = f"{BASE_URL}/{fname}"
        dest = download_dir / fname

        print(f"Downloading {fname}...")
        print(f"  {description}")

        # Get size
        try:
            r = requests.head(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            size = int(r.headers.get("content-length", 0))
            print(f"  Size: {size/1e9:.2f} GB")
        except Exception as e:
            print(f"  WARNING: could not get size: {e}")
            size = 0

        try:
            download_file(url, dest, size)
            metadata["files"].append({"name": fname, "url": url, "size_bytes": size, "description": description})
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            sys.exit(1)

    meta_path = download_dir / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    total = sum(f["size_bytes"] for f in metadata["files"])
    print(f"\nDownload complete: {len(metadata['files'])} files, {total/1e9:.2f} GB total")
    print(f"Metadata: {meta_path}")


if __name__ == "__main__":
    main()
