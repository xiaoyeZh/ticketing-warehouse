"""管理员按需配置三类受限账号；不向数仓工作进程传递管理员密码。"""
from psycopg import sql

from ticketing.bootstrap import migrate
from ticketing.db import connect

from .config import ROLE_DEFAULTS, role_config
from .contracts import SOURCE_TABLES


def grant_warehouse(conn, roles):
    if set(roles) != set(ROLE_DEFAULTS):
        raise RuntimeError("需要 reader、writer、reporter 三类账号")
    names = [role.name for role in roles.values()]
    if len(set(names)) != 3 or any(not role.name.strip() or not role.password.strip() for role in roles.values()):
        raise RuntimeError("数仓账号必须互不相同，且名称和密码不能为空")
    with conn.transaction():
        for kind, role in roles.items():
            existing = conn.execute(
                """SELECT oid, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,
                          shobj_description(oid, 'pg_authid') AS marker
                   FROM pg_roles WHERE rolname=%s""", (role.name,),
            ).fetchone()
            marker = "ticketing-warehouse managed " + kind
            if existing:
                if (existing['marker'] != marker or any(existing[key] for key in
                        ('rolsuper', 'rolcreatedb', 'rolcreaterole', 'rolreplication', 'rolbypassrls'))
                        or conn.execute("SELECT 1 FROM pg_auth_members WHERE member=%s", (existing['oid'],)).fetchone()):
                    raise RuntimeError(f"数仓账号 {role.name} 不符合本项目的受限账号约定，停止修改")
                conn.execute(sql.SQL("ALTER ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
                    sql.Identifier(role.name), sql.Literal(role.password)))
            else:
                conn.execute(sql.SQL("CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
                                     "NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}").format(
                    sql.Identifier(role.name), sql.Literal(role.password)))
                conn.execute(sql.SQL("COMMENT ON ROLE {} IS {}").format(sql.Identifier(role.name), sql.Literal(marker)))
        database = conn.execute("SELECT current_database() AS name, current_user AS owner").fetchone()
        for role in roles.values():
            conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database['name']), sql.Identifier(role.name)))

        reader, writer, reporter = (sql.Identifier(roles[k].name) for k in ('reader', 'writer', 'reporter'))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(reader))
        tables = sql.SQL(', ').join(sql.Identifier('public', table) for table in SOURCE_TABLES)
        conn.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(tables, reader))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA wh_meta, dws TO {}").format(writer))
        conn.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA wh_meta, dws TO {}").format(writer))
        conn.execute(sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA wh_meta, dws TO {}").format(writer))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA ads TO {}").format(reporter))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA ads TO {}").format(reporter))
        owner = sql.Identifier(database['owner'])
        conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA wh_meta, dws "
                             "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}").format(owner, writer))
        conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA wh_meta, dws "
                             "GRANT USAGE, SELECT ON SEQUENCES TO {}").format(owner, writer))
        conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA ads "
                             "GRANT SELECT ON TABLES TO {}").format(owner, reporter))


def initialize():
    roles = {kind: role_config(kind) for kind in ROLE_DEFAULTS}
    with connect(application_name="warehouse-admin") as conn:
        conn.execute("SELECT pg_advisory_lock(782314, 8)")
        try:
            migrate(conn)
            grant_warehouse(conn, roles)
        finally:
            conn.execute("SELECT pg_advisory_unlock(782314, 8)")
    return {"status": "ready", "roles": {kind: role.name for kind, role in roles.items()},
            "schemas": ["wh_meta", "dws", "ads"]}
