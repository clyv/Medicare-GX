"""
download_data.py
Pulls the CMS Medicare Physician & Other Practitioners (MUP) dataset.
Latest available: 2023 service year (released December 2025).
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

# CMS open data API — MUP 2023 service year (most recent public release)
# Full dataset download link from data.cms.gov
DATASETS = {
    "mup_phy_r25_p05_v20_d23_prov": {
        "url": "https://data.cms.gov/sites/default/files/2025-04/22edfd1e-d17a-4478-ad6b-92cac2a5a3c4/MUP_PHY_R25_P05_V20_D23_Prov.csv",
        "description": "MUP by Provider — 2023 service year (released Dec 2025)",
    },
    "mup_phy_r25_p05_v20_d23_prov_svc": {
        "url": "https://data.cms.gov/sites/default/files/2025-04/MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv",
        "description": "MUP by Provider + Service — 2023 service year (released Dec 2025)",
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
    print("CMS Medicare MUP 2023 — Data Download")
    print("=" * 60 + "\n")

    for name, meta in DATASETS.items():
        dest = RAW_DIR / f"{name}.csv"
        download_file(meta["url"], dest, meta["description"])

    print("\nAll files downloaded. Check data/raw/")
    print("Next step: run notebooks/01_exploration.ipynb")


if __name__ == "__main__":
    main()