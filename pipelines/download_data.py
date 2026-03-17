"""
download_data.py
Pulls the CMS Medicare Physician & Other Practitioners (MUP) dataset.
Latest available: 2022 service year (released 2024).
Source: https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners
"""

import os
import requests
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# CMS open data API — MUP by Provider 2022 (most recent public release)
# Full dataset download link from data.cms.gov
DATASETS = {
    "mup_phy_r24_p05_v10_dy22_prvdr": {
        "url": "https://data.cms.gov/sites/default/files/2024-06/ef35c5c2-a282-4e28-92fe-f28f56f71ec1/MUP_PHY_R24_P05_V10_DY22_PRVDR.csv",
        "description": "MUP Provider-level 2022 (~10M rows)",
    },
    "mup_phy_r24_p05_v10_dy22_npi_svc": {
        "url": "https://data.cms.gov/sites/default/files/2024-06/6e536af0-b76e-4f91-a2a6-fcfe2e6f5b9a/MUP_PHY_R24_P05_V10_DY22_NPI_SVC.csv",
        "description": "MUP NPI+Service-level 2022 (~9M rows)",
    },
}


# ─── Download Helper ──────────────────────────────────────────────────────────

def download_file(url: str, dest: Path, description: str) -> None:
    if dest.exists():
        print(f"[SKIP] Already exists: {dest.name}")
        return

    print(f"[DOWNLOAD] {description}")
    print(f"  → {url}")

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))

    with open(dest, "wb") as f, tqdm(
        desc=dest.name,
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))

    print(f"  ✓ Saved to {dest}\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("CMS Medicare MUP 2022 — Data Download")
    print("=" * 60 + "\n")

    for name, meta in DATASETS.items():
        dest = RAW_DIR / f"{name}.csv"
        download_file(meta["url"], dest, meta["description"])

    print("\nAll files downloaded. Check data/raw/")
    print("Next step: run notebooks/01_exploration.ipynb")


if __name__ == "__main__":
    main()