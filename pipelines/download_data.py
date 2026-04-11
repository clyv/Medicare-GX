"""
download_data.py
Pulls CMS Medicare Physician & Other Practitioners (MUP) — 2023 service year.
Most recent public release (Dec 2025). Source: data.cms.gov
"""

import requests
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

DATASETS = {
    "mup_phy_r25_p05_v20_d23_prov": {
        "url": "https://data.cms.gov/sites/default/files/2025-04/22edfd1e-d17a-4478-ad6b-92cac2a5a3c4/MUP_PHY_R25_P05_V20_D23_Prov.csv",
        "description": "MUP by Provider — 2023 service year (released Dec 2025)",
    },
}


def download_file(url: str, dest: Path, description: str) -> None:
    if dest.exists():
        print(f"[SKIP] Already exists: {dest.name}")
        return

    print(f"[DOWNLOAD] {description}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))

    with open(dest, "wb") as f, tqdm(
        desc=dest.name, total=total, unit="B", unit_scale=True, unit_divisor=1024
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))

    print(f"  ✓ Saved to {dest}\n")


def main():
    print("=" * 60)
    print("CMS Medicare MUP 2023 — Data Download")
    print("=" * 60 + "\n")

    for name, meta in DATASETS.items():
        dest = RAW_DIR / f"{name}.csv"
        download_file(meta["url"], dest, meta["description"])

    print("\nDone. Check data/raw/")


if __name__ == "__main__":
    main()