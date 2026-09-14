import argparse
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from urban_platform.analytics.products import PRODUCTS, build_products
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--products",nargs="+",choices=PRODUCTS,default=PRODUCTS)
    args=parser.parse_args()
    spark=get_spark("lab2-data-products")
    try:
        records=build_products(spark,load_config(str(ROOT/"config/datasets.yaml")),args.products)
        output=ROOT/"artifacts/products.json"
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(records,indent=2))
        print(json.dumps(records,indent=2),flush=True)
    finally:
        spark.stop()
