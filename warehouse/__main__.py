"""指标、运行环境、账号初始化和 ODS 一致性抽取命令。"""
import argparse
from datetime import date
import json
import subprocess
import sys
from uuid import UUID

import psycopg

from .contracts import sales_contract
from .errors import ExtractionError, TransformationError, WarehouseError


def main():
    parser = argparse.ArgumentParser(description='票务离线数仓：抽取、建模、日报发布与查询')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('metrics', help='显示 sales-v1 指标契约，不访问数据库')
    commands.add_parser('check-env', help='用离线样例验证 Java、Spark、Decimal、时区和 Parquet')
    commands.add_parser('init-db', help='管理员：迁移数仓命名空间并配置三类受限账号')
    extraction = commands.add_parser('extract', help='全量一致性抽取七张业务表到新的 ODS 批次')
    extraction.add_argument('--chunk-size', type=int, default=5000, help='每次读取的行数，1～100000')
    verification = commands.add_parser('verify-batch', help='验证成功批次的清单、文件校验值、类型和行数')
    verification.add_argument('--batch', type=UUID, required=True, help='抽取返回的 source_batch_id')
    transformation = commands.add_parser('transform', help='用 Spark 将已验证 ODS 加工为维度和票级销售事实')
    transformation.add_argument('--source-batch', type=UUID, required=True, help='成功的 ODS source_batch_id')
    model_check = commands.add_parser('verify-transform', help='验证成功加工批次的血缘、清单和三表文件')
    model_check.add_argument('--run', type=UUID, required=True, help='transform 返回的 run_id')
    aggregation = commands.add_parser('aggregate', help='从已验证建模批次生成日期×场次日报并装载 DWS')
    aggregation.add_argument('--transform-run', type=UUID, required=True)
    aggregate_check = commands.add_parser('verify-aggregate', help='校验 DWS 批次的血缘、行数、金额与内容散列')
    aggregate_check.add_argument('--run', type=UUID, required=True)
    publication = commands.add_parser('publish', help='事务发布完整日报批次；相同批次重复发布不产生新版本')
    publication.add_argument('--run', type=UUID, required=True)
    publication.add_argument('--expected-generation', type=int)
    publication.add_argument('--allow-older', action='store_true', help='显式允许恢复旧来源快照')
    reporting = commands.add_parser('report', help='reporter 专用：查询当前已发布的日报')
    reporting.add_argument('--start-date', type=date.fromisoformat, help='上海支付日期，包含该日')
    reporting.add_argument('--end-date', type=date.fromisoformat, help='上海支付日期，不包含该日')
    reporting.add_argument('--session', type=int, dest='session_id')
    commands.add_parser('run', help='全量抽取→建模→汇总→发布；保护并发期间的新版本')
    replay = commands.add_parser('replay', help='从指定 ODS 重建和校验，默认不发布、不读取实时业务表')
    replay.add_argument('--source-batch', type=UUID, required=True)
    replay.add_argument('--publish', action='store_true', help='验证成功后发布')
    replay.add_argument('--allow-older', action='store_true', help='与 --publish 一起显式允许恢复旧快照')
    status = commands.add_parser('pipeline-status', help='查询完整流水线的阶段、状态与各层批次编号')
    status.add_argument('--run', type=UUID, required=True)
    args = parser.parse_args()
    if args.command == 'replay' and args.allow_older and not args.publish:
        parser.error('--allow-older 必须与 --publish 一起使用')
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
        elif args.command == 'verify-batch':
            from .extract import verify_batch
            result = verify_batch(args.batch)
        elif args.command == 'transform':
            from .transform import transform
            result = transform(args.source_batch)
        elif args.command == 'verify-transform':
            from .transform import verify_transform
            result = verify_transform(args.run)
        elif args.command == 'aggregate':
            from .aggregate import aggregate
            result = aggregate(args.transform_run)
        elif args.command == 'verify-aggregate':
            from .aggregate import verify_aggregate
            result = verify_aggregate(args.run)
        elif args.command == 'publish':
            from .publication import publish
            result = publish(args.run, expected_generation=args.expected_generation, allow_older=args.allow_older)
        elif args.command == 'report':
            from .report import report
            result = report(start_date=args.start_date, end_date=args.end_date, session_id=args.session_id)
        elif args.command in ('run', 'replay'):
            from .pipeline import run_pipeline
            result = (run_pipeline() if args.command == 'run' else
                      run_pipeline(source_batch_id=args.source_batch, should_publish=args.publish, allow_older=args.allow_older))
        elif args.command == 'pipeline-status':
            from .pipeline import PipelineStore
            result = PipelineStore().get(args.run)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except WarehouseError as exc:
        print(json.dumps({'status': 'failed', 'error': exc.code, **exc.identifiers}, ensure_ascii=False), file=sys.stderr)
        return 1
    except TransformationError as exc:
        print(json.dumps({'status': 'failed', 'error': exc.code, 'source_batch_id': exc.source_batch_id,
                          'run_id': exc.run_id}, ensure_ascii=False), file=sys.stderr)
        return 1
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
