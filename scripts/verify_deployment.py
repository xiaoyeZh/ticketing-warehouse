"""从源码副本、随机密码和全新数据卷验收部署；仅清理本次临时 Compose 项目。"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from uuid import uuid4

from api_demo import run as api_demo


ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify(report_dir, *, include_warehouse=False):
    report_dir.mkdir(parents=True, exist_ok=True)
    project = 'ticketing-verify-' + uuid4().hex[:16]
    image = 'ticketing-verify-python:' + project
    warehouse_image = 'ticketing-verify-spark:' + project
    warehouse_test_image = 'ticketing-verify-warehouse-tests:' + project
    passwords = [secrets.token_hex(24) for _ in range(5)]
    # 不继承开发库连接配置、Compose 覆盖文件或服务选择。
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('COMPOSE_', 'POSTGRES_', 'TICKETING_', 'WAREHOUSE_', 'PG'))
           and k != 'API_PORT'}
    env['COMPOSE_ANSI'] = 'never'
    summary = {'project': project, 'platform': platform.platform(),
               'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'failed'}

    def sanitize(value):
        for password in passwords:
            value = value.replace(password, '[REDACTED]')
        return value

    with tempfile.TemporaryDirectory(prefix=project + '-') as directory:
        checkout = Path(directory)
        # 与 Dockerfile 所需源码一致；不复制 .git、个人 .env、数据或验收产物。
        for name in ('Dockerfile', 'compose.yaml', 'requirements.txt', 'requirements-warehouse.txt', '.dockerignore',
                     'ticketing', 'warehouse', 'docker', 'migrations', 'seeds', 'tests', 'scripts'):
            source = ROOT / name
            if source.is_dir():
                shutil.copytree(source, checkout / name,
                                ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '.env', '.env.*'))
            else:
                shutil.copy2(source, checkout / name)
        config = checkout / '.env'
        config.write_text('\n'.join((
            'POSTGRES_DB=ticketing_verify', 'POSTGRES_USER=ticketing_admin',
            'POSTGRES_PASSWORD=' + passwords[0], 'TICKETING_APP_PASSWORD=' + passwords[1],
            'WAREHOUSE_READER_PASSWORD=' + passwords[2],
            'WAREHOUSE_WRITER_PASSWORD=' + passwords[3],
            'WAREHOUSE_REPORTER_PASSWORD=' + passwords[4],
            'POSTGRES_PORT=0', 'API_PORT=0', 'TICKETING_IMAGE=' + image,
            'WAREHOUSE_IMAGE=' + warehouse_image, 'WAREHOUSE_TEST_IMAGE=' + warehouse_test_image, '')))
        config.chmod(0o600)
        compose = ['docker', 'compose', '--project-name', project,
                   '--project-directory', str(checkout), '--env-file', str(config),
                   '-f', str(checkout / 'compose.yaml')]

        def command(label, args, *, input=None, timeout=600, check=True):
            print(label, flush=True)
            try:
                result = subprocess.run(args, cwd=checkout, env=env, input=input,
                                        text=True, capture_output=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                output = ''.join(v.decode(errors='replace') if isinstance(v, bytes) else (v or '')
                                 for v in (exc.stdout, exc.stderr))
                (report_dir / (label + '.txt')).write_text(sanitize(output))
                raise RuntimeError(label + ' timed out') from exc
            output = sanitize(result.stdout + result.stderr)
            (report_dir / (label + '.txt')).write_text(output)
            if check and result.returncode:
                print(output[-8000:], file=sys.stderr)
                raise RuntimeError(f'{label} failed (exit {result.returncode})')
            return result

        def dc(label, *args, **kwargs):
            return command(label, compose + list(args), **kwargs)

        def cli(label, *args):
            return json.loads(dc(label, 'run', '--rm', '-T', 'app', *args).stdout)

        def sql(label, statement):
            return dc(label, 'exec', '-T', 'postgres', 'psql', '-X', '-A', '-t',
                      '-U', 'ticketing_admin', '-d', 'ticketing_verify',
                      '-v', 'ON_ERROR_STOP=1', input=statement).stdout.strip()

        try:
            command('docker-version', ['docker', 'version'])
            dc('compose-version', 'version')
            dc('build', 'build', 'db-admin', timeout=1200)
            dc('postgres-start', 'up', '-d', '--wait', '--wait-timeout', '120', 'postgres')
            dc('initialize', 'run', '--rm', '-T', 'db-admin')
            dc('initialize-again', 'run', '--rm', '-T', 'db-admin')
            expected_migrations = len(list((checkout / 'migrations').glob('[0-9][0-9][0-9]_*.sql')))
            require(sql('migration-count', 'SELECT count(*) FROM schema_migration;') == str(expected_migrations),
                    '迁移版本数量不符')
            warehouse = json.loads(dc('warehouse-init', '--profile', 'warehouse', 'run', '--rm', '-T', 'warehouse-admin').stdout)
            repeated = json.loads(dc('warehouse-init-again', '--profile', 'warehouse', 'run', '--rm', '-T', 'warehouse-admin').stdout)
            require(warehouse == repeated and warehouse['status'] == 'ready', '数仓账号重复初始化结果不一致')
            require(sql('warehouse-restricted-source',
                        "SELECT has_table_privilege('ticketing_wh_reader','booking_order','SELECT') "
                        "AND NOT has_table_privilege('ticketing_wh_reader','auth_credential','SELECT') "
                        "AND NOT has_table_privilege('ticketing_wh_reader','booking_order','UPDATE');") == 't',
                    '数仓来源账号权限不符')
            first = json.loads(dc('seed', 'run', '--rm', '-T', 'db-admin', 'demo').stdout)
            second = json.loads(dc('seed-again', 'run', '--rm', '-T', 'db-admin', 'demo').stdout)
            require(first['session_ids'] == second['session_ids'], '重复初始化产生额外场次')
            dc('python-tests', 'run', '--rm', '-T', 'tests')
            if include_warehouse:
                dc('warehouse-build', '--profile', 'warehouse', 'build', 'warehouse', 'warehouse-tests', timeout=1200)
                environment = json.loads(command('warehouse-environment', ['docker', 'run', '--rm',
                    '--network', 'none', '--hostname', 'localhost', warehouse_image, 'check-env']).stdout)
                require(environment['status'] == 'passed', 'Spark 环境验证失败')
                dc('warehouse-tests', '--profile', 'warehouse', 'run', '--rm', '-T', 'warehouse-tests', timeout=600)
            for filename in ('001_core_tables.sql', '002_session_seat.sql'):
                sql('sql-' + filename, (checkout / 'tests/sql' / filename).read_text())
            require(sql('test-database-cleanup',
                        "SELECT count(*) FROM pg_database WHERE datname LIKE 'ticketing_test_%';") == '0',
                    '测试数据库未清理')
            require(sql('restricted-role',
                        "SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname='ticketing_app';") == 'f',
                    '应用账号权限超出预期')
            dc('services-start', '--profile', 'workers', 'up', '-d', '--wait',
               '--wait-timeout', '120', 'api', 'reservation-worker')
            address = dc('api-port', 'port', 'api', '8000').stdout.strip()
            require(address.startswith('127.0.0.1:'), 'API 未绑定本机地址')
            base_url = 'http://' + address
            for path, content in (('/health/ready', b'"ready"'), ('/', b'type="module"'),
                                  ('/assets/i18n.mjs', b'zh-Hant'), ('/openapi.json', b'/orders')):
                with urllib.request.urlopen(base_url + path, timeout=10) as response:
                    require(response.status == 200 and content in response.read(), 'HTTP 验收失败: ' + path)
            receipt = api_demo(base_url)
            (report_dir / 'http-demo.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
            if include_warehouse:
                extracted = [json.loads(dc(f'warehouse-extract-{i}', '--profile', 'warehouse', 'run', '--rm', '-T',
                    'warehouse', 'extract', '--chunk-size', '1').stdout) for i in (1, 2)]
                require(all(batch['status'] == 'SUCCEEDED' for batch in extracted), 'ODS 抽取失败')
                require(extracted[0]['source_batch_id'] != extracted[1]['source_batch_id'], '新抽取覆盖旧批次')
                for i, batch in enumerate(extracted, 1):
                    verified = json.loads(dc(f'warehouse-verify-{i}', '--profile', 'warehouse', 'run', '--rm', '-T',
                        'warehouse', 'verify-batch', '--batch', batch['source_batch_id']).stdout)
                    require(verified['files_verified'] == 7, 'ODS 批次校验失败')
                modeled = [json.loads(dc(f'warehouse-transform-{i}', '--profile', 'warehouse', 'run', '--rm', '-T',
                    'warehouse', 'transform', '--source-batch', extracted[0]['source_batch_id'], timeout=300).stdout) for i in (1, 2)]
                require(all(run['status'] == 'SUCCEEDED' for run in modeled), 'Spark 业务加工失败')
                require(modeled[0]['run_id'] != modeled[1]['run_id'], '加工重跑覆盖旧结果')
                require(all(run['measured_totals'] == extracted[0]['reference_totals'] for run in modeled), '加工与来源合计不平')
                for i, run in enumerate(modeled, 1):
                    checked = json.loads(dc(f'warehouse-verify-transform-{i}', '--profile', 'warehouse', 'run', '--rm', '-T',
                        'warehouse', 'verify-transform', '--run', run['run_id']).stdout)
                    require(checked['tables_verified'] == 3, '加工结果文件校验失败')
                summary.update(warehouse_extraction='passed', warehouse_environment='passed', warehouse_transformation='passed')
            user = next(u['user_id'] for u in first['users'] if u['email'] == 'buyer-a@example.test')
            seat = receipt['inventory_ids'][0]
            order = cli('worker-reserve', 'reserve', '--user', str(user), '--session',
                        str(receipt['session_id']), '--seats', str(seat), '--hold-seconds', '1')
            deadline = time.monotonic() + 20
            while True:
                current = cli('worker-order', 'order', '--user', str(user), '--order', str(order['order_id']))
                if current['status'] == 'EXPIRED':
                    break
                require(time.monotonic() < deadline, '后台清理未在预期时间内处理订单')
                time.sleep(1)
            require(sql('worker-inventory', f'SELECT status FROM session_seat WHERE session_seat_id={int(seat)};') == 'AVAILABLE',
                    '过期订单未释放座位')
            summary.update(status='passed', migration_count=expected_migrations, seed_idempotent=True,
                           warehouse_foundation='passed',
                           http_payment_refund_resale='passed', worker_expiration='passed')
        except Exception as exc:
            summary['error'] = sanitize(str(exc))
            raise
        finally:
            dc('service-logs', 'logs', '--no-color', '--tail', '250', check=False, timeout=60)
            cleanup = dc('cleanup', '--profile', 'tools', '--profile', 'workers', '--profile', 'warehouse',
                         'down', '--volumes', '--remove-orphans', check=False, timeout=120)
            remaining = command('remaining-containers', ['docker', 'ps', '-aq', '--filter',
                                'label=com.docker.compose.project=' + project], check=False)
            volumes = command('remaining-volumes', ['docker', 'volume', 'ls', '-q', '--filter',
                              'label=com.docker.compose.project=' + project], check=False)
            summary['cleanup_passed'] = (cleanup.returncode == remaining.returncode == volumes.returncode == 0
                                         and not remaining.stdout.strip() and not volumes.stdout.strip())
            command('image-cleanup', ['docker', 'image', 'rm', image], check=False, timeout=60)
            if include_warehouse:
                command('warehouse-image-cleanup', ['docker', 'image', 'rm', warehouse_image, warehouse_test_image],
                        check=False, timeout=60)
            if not summary['cleanup_passed']:
                summary['status'] = 'failed'
            summary['finished_at'] = datetime.now(timezone.utc).isoformat()
            (report_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        require(summary['cleanup_passed'], '临时验收项目清理失败，请查看 cleanup.txt')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-dir', type=Path, default=ROOT / 'artifacts/deployment')
    parser.add_argument('--warehouse', action='store_true', help='同时构建并验证 Spark 环境与 ODS 抽取')
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.report_dir.resolve(), include_warehouse=args.warehouse), ensure_ascii=False, indent=2))
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f'部署验收失败：{exc}；日志：{args.report_dir.resolve()}', file=sys.stderr)
        raise SystemExit(1)
