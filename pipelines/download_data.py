"""
download_data.py
Pulls CMS Medicare Physician & Other Practitioners (MUP) — 2023 service year.
Most recent public release (Dec 2025). Source: data.cms.gov

    python pipelines/download_data.py                    # by-provider (default)
    python pipelines/download_data.py --dataset service  # by-provider-and-service
    python pipelines/download_data.py --dataset all
"""

import argparse
import sys

import requests
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")

DATASETS = {
    "provider": {
        "filename": "mup_phy_r25_p05_v20_d23_prov.csv",
        "url": "https://data.cms.gov/sites/default/files/2025-04/22edfd1e-d17a-4478-ad6b-92cac2a5a3c4/MUP_PHY_R25_P05_V20_D23_Prov.csv",
        "description": "MUP by Provider — 2023 service year (~1.26M rows, ~500MB)",
    },
    "service": {
        "filename": "mup_phy_r25_p05_v20_d23_prov_svc.csv",
        "url": "https://data.cms.gov/sites/default/files/2025-04/e3f823f8-db5b-4cc7-ba04-e7ae92b99757/MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv",
        "description": "MUP by Provider and Service — 2023 service year (~10M rows, ~2GB)",
    },
}


def dataset_path(name: str) -> Path:
    return RAW_DIR / DATASETS[name]["filename"]


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        choices=[*DATASETS, "all"],
        default="provider",
        help="which CMS extract to pull (default: provider)",
    )
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    wanted = list(DATASETS) if args.dataset == "all" else [args.dataset]

    print("=" * 60)
    print("CMS Medicare MUP 2023 — Data Download")
    print("=" * 60 + "\n")

    for name in wanted:
        meta = DATASETS[name]
        download_file(meta["url"], dataset_path(name), meta["description"])

    print("\nDone. Check data/raw/")


if __name__ == "__main__":
    # ✓ in the output would blow up a cp1252 Windows console otherwise
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    main()
