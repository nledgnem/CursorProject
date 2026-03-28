import pandas as pd
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).parent.parent

    # Check Bronze
    bronze_path = repo_root / "data" / "curated" / "data_lake" / "fact_funding.parquet"
    if bronze_path.exists():
        df_bronze = pd.read_parquet(bronze_path)
        print(f"BRONZE (fact_funding.parquet) Max Date: {df_bronze['date'].max()}")
    else:
        print("BRONZE NOT FOUND.")

    # Check Silver
    silver_path = repo_root / "data" / "curated" / "silver_fact_funding.parquet"
    if silver_path.exists():
        df_silver = pd.read_parquet(silver_path)
        print(f"SILVER (silver_fact_funding.parquet) Max Date: {df_silver['date'].max()}")
    else:
        print("SILVER NOT FOUND.")


if __name__ == "__main__":
    main()

