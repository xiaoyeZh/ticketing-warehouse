"""当前只开放指标、运行环境和账号初始化命令。"""
import argparse
import json
import subprocess
import sys

import psycopg

from .contracts import sales_contract


def main():
    parser = argparse.ArgumentParser(description='票务离线数仓：第八阶段前两步')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('metrics', help='显示 sales-v1 指标契约，不访问数据库')
    commands.add_parser('check-env', help='用离线样例验证 Java、Spark、Decimal、时区和 Parquet')
    commands.add_parser('init-db', help='管理员：迁移数仓命名空间并配置三类受限账号')
    args = parser.parse_args()
    try:
        if args.command == 'metrics':
            result = sales_contract()
        elif args.command == 'check-env':
            from .runtime import check_environment
            result = check_environment()
        else:
            from .bootstrap import initialize
            result = initialize()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (RuntimeError, OSError, ImportError, subprocess.SubprocessError, psycopg.Error) as exc:
        # 数据库和外部进程异常可能带连接信息；此处只输出异常类型。
        error = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
        print(json.dumps({'status': 'failed', 'error': error}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
