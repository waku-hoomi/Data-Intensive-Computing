"""CLI entry point: build the integrated_taxi_trips gold Delta table (Task 5)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from urban_platform.integration.integrate import write_integrated_taxi_trips
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

CONFIG_PATH = str(Path(__file__).resolve().parent.parent / "config" / "datasets.yaml")


def main() -> None:
    platform_cfg = load_config(CONFIG_PATH)
    spark = get_spark(app_name="urban-platform-integration")

    output_path = write_integrated_taxi_trips(spark, platform_cfg)
    df = spark.read.format("delta").load(output_path)
    total = df.count()
    print(f"[SUCCESS] integrated_taxi_trips -> {output_path} ({total} rows)")

    null_weather = df.filter(df.temp_c.isNull()).count()
    null_aq = df.filter(df.pm25_ug_m3.isNull()).count()
    print(f"    rows missing weather: {null_weather} ({null_weather/total:.2%})")
    print(f"    rows missing air_quality: {null_aq} ({null_aq/total:.2%})")

    print("Sample rows:")
    df.show(5, truncate=False, vertical=True)

    spark.stop()


if __name__ == "__main__":
    main()