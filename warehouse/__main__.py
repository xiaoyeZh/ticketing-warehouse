"""指标、运行环境、账号初始化和 ODS 一致性抽取命令。"""
import argparse
import json
import subprocess
import sys
from uuid import UUID

import psycopg

from .contracts import sales_contract
from .errors import ExtractionError


def main():
    parser = argparse.ArgumentParser(description='票务离线数仓：指标、运行环境与一致性抽取')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('metrics', help='显示 sales-v1 指标契约，不访问数据库')
    commands.add_parser('check-env', help='用离线样例验证 Java、Spark、Decimal、时区和 Parquet')
    commands.add_parser('init-db', help='管理员：迁移数仓命名空间并配置三类受限账号')
    extraction = commands.add_parser('extract', help='全量一致性抽取七张业务表到新的 ODS 批次')
    extraction.add_argument('--chunk-size', type=int, default=5000, help='每次读取的行数，1～100000')
    verification = commands.add_parser('verify-batch', help='验证成功批次的清单、文件校验值、类型和行数')
    verification.add_argument('--batch', type=UUID, required=True, help='抽取返回的 source_batch_id')
    args = parser.parse_args()
    try:
        if args.command == 'metrics':
            result = sales_contract()
        elif args.command == 'check-env':
            from .runtime import check_environment
            result = check_environment()
        elif args.command == 'init-db':
            from .bootstrap import initialize
            result = initialize()
        elif args.command == 'extract':
            from .extract import extract
            result = extract(chunk_size=args.chunk_size)
        else:
            from .extract import verify_batch
            result = verify_batch(args.batch)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except ExtractionError as exc:
        print(json.dumps({'status': 'failed', 'error': exc.code,
                          'source_batch_id': exc.batch_id}, ensure_ascii=False), file=sys.stderr)
        return 1
    except (RuntimeError, OSError, ImportError, subprocess.SubprocessError, psycopg.Error) as exc:
        # 数据库和外部进程异常可能带连接信息；此处只输出异常类型。
        error = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
        print(json.dumps({'status': 'failed', 'error': error}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
