"""供命令、账号授权和后续抽取共用的指标契约。"""
import json
from pathlib import Path


CONTRACT_PATH = Path(__file__).with_name("contracts") / "sales_metrics.json"


def sales_contract():
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


SOURCE_TABLES = tuple(sales_contract()["source_tables"])
