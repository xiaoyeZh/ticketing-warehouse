"""数仓连接配置；普通运行不使用初始化管理员账号。"""
from dataclasses import dataclass, field
import os

import psycopg
from psycopg.rows import dict_row


ROLE_DEFAULTS = {
    "reader": "ticketing_wh_reader",
    "writer": "ticketing_wh_writer",
    "reporter": "ticketing_wh_reporter",
}


@dataclass(frozen=True)
class Role:
    name: str
    password: str = field(repr=False)


def role_config(kind):
    prefix = "WAREHOUSE_" + kind.upper()
    password = os.environ.get(prefix + "_PASSWORD", "")
    if not password.strip():
        raise RuntimeError(f"缺少 {prefix}_PASSWORD，请按数仓文档配置独立密码")
    return Role(os.environ.get(prefix + "_USER", ROLE_DEFAULTS[kind]), password)


def connect_role(kind):
    role = role_config(kind)
    return psycopg.connect(
        host=os.environ.get("PGHOST", "postgres"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "ticketing"),
        user=role.name, password=role.password,
        autocommit=True, row_factory=dict_row, connect_timeout=10,
        application_name="warehouse-" + kind,
        options="-c timezone=UTC -c lock_timeout=5000 -c statement_timeout=30000",
    )
