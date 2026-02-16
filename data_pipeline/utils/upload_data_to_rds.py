import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.utils.upload_data_to_local_db import run_upload_common


def main() -> None:
    run_upload_common(
        description="Upload local CSV/Parquet files to AWS RDS Postgres",
        fixed_mode="aws",
        include_rds_sslmode=True,
    )


if __name__ == "__main__":
    main()
