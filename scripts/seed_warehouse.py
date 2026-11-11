"""面向离线数仓的批量造数脚本。

与 seeds/001_ticketing_demo.sql 的分工：
- SQL 种子：最小可演示数据（1 个场馆、2 个座位、2 个场次），用于手工点页面。
- 本脚本：给 ODS/DWD/DWS 层准备有量级的交易数据，带真实的订单状态分布与时间分布。

同时满足两类用途：
1. 数仓分层：每张表都有万级记录，指标不再恒为 0 或 1；
2. 数据质量：金额、票、库存三者可对账，脚本结束时自动核对。

安全边界：
- 只写数据，不改表结构、不新增迁移，所有写入都走既有 CHECK / 外键 / 唯一约束；
- 需要管理员连接（ticketing_app 没有 venue / seat / activity_session / session_seat 的写权限）；
- 可重复执行：已有造数数据时默认拒绝运行，需要显式 --reset 重建或 --append 追加。

用法：
    python scripts/seed_warehouse.py --scale small
    python scripts/seed_warehouse.py --scale medium --reset
    python scripts/seed_warehouse.py --reset --scale tiny --seed 7
    python scripts/seed_warehouse.py --append --orders 500      # 在已有世界模型上再追加一批
    python scripts/seed_warehouse.py --scale small --dry-run    # 只演练，最后整体回滚
"""
import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import heapq
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid5

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ticketing.db import connect  # noqa: E402


SEED_DOMAIN = "seed.invalid"
SEED_TAG = "造数"
ORGANIZER_EMAIL = "seed.organizer@" + SEED_DOMAIN
NAMESPACE = UUID("5f3c9b7e-0d21-4a86-9c5e-3b1d7a2f4c69")
HOLD = timedelta(minutes=10)
# 场次时刻按业务时区（东八区）生成，库里仍是 timestamptz，前端显示才会是晚场而不是凌晨。
BUSINESS_TZ = timezone(timedelta(hours=8))

# 售卖窗口：开场前 30 天开票；窗口之外不产生订单。
SALE_DAYS = 30

# 下单时刻的时段权重：校园票务的晚间峰值。
HOURS = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2]
HOUR_WEIGHTS = [0.4, 1.0, 1.2, 1.1, 2.0, 1.6, 1.0, 1.0, 1.2, 1.6, 2.4, 3.2, 3.4, 2.8, 2.0, 1.2, 0.6, 0.3, 0.15]

# 座位数量的常见组合：单人票最常见，四人同行最少。
SEAT_COUNTS = [1, 2, 3, 4]
SEAT_WEIGHTS = [0.45, 0.34, 0.13, 0.08]

ACTIVITY_TITLES = [
    "校园音乐节",
    "毕业季交响音乐会",
    "话剧《雷雨》",
    "校园歌手大赛决赛",
    "相声专场",
    "舞蹈社年度汇演",
    "迎新晚会",
    "跨年音乐会",
    "戏剧社冬季公演",
    "校园电竞总决赛",
    "民乐专场",
    "合唱团专场",
]

ACTIVITY_PRICE_FACTORS = ["1.0", "1.2", "0.8", "1.5", "0.9", "1.1", "0.7", "1.3", "0.95", "1.15", "0.85", "1.05"]

REFUND_REASONS = ["行程冲突", "重复下单", "临时有事", "买错场次", "改约其他场次", "价格调整"]

# 单个用户的订单量级：多数人只买一两单，少数重度用户反复购买。
USER_ORDER_COUNTS = [1, 2, 3, 4, 6]
USER_ORDER_WEIGHTS = [0.46, 0.27, 0.14, 0.09, 0.04]

# (退款, 超时, 主动取消) 的概率；剩余部分为 PAID。
# PENDING 不在这里给：只有创建时间落在最近 10 分钟内，订单才可能仍在占用座位，
# 其余未付订单一律落到 EXPIRED / CANCELLED。
ORDER_OUTCOME = (0.09, 0.13, 0.03)

# 付款过程中先失败一次的概率：真实支付有网关抖动、余额不足等失败。
PAYMENT_FAILURE_RATE = 0.06


@dataclass(frozen=True)
class Scale:
    venues: int
    seats_per_venue: int
    activities: int
    sessions_per_activity: int
    orders: int


SCALES = {
    "tiny": Scale(venues=2, seats_per_venue=60, activities=3, sessions_per_activity=3, orders=200),
    "small": Scale(venues=3, seats_per_venue=200, activities=6, sessions_per_activity=4, orders=2400),
    "medium": Scale(venues=4, seats_per_venue=250, activities=8, sessions_per_activity=6, orders=6000),
    "large": Scale(venues=5, seats_per_venue=300, activities=10, sessions_per_activity=8, orders=12500),
}


class _Rollback(Exception):
    """--dry-run 用异常触发事务回滚。"""


# ---------------------------------------------------------------- 基础工具


def _uuid(*parts):
    return uuid5(NAMESPACE, ":".join(str(part) for part in parts))


def _row_label(index):
    """0 -> A, 25 -> Z, 26 -> AA。seat.row_label 限长 16。"""
    label = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        label = chr(65 + remainder) + label
    return label


def _insert_many(conn, statement, rows):
    if rows:
        with conn.cursor() as cursor:
            cursor.executemany(statement, rows)


def _random_moment(rng, start, end):
    """在 [start, end) 内取一个时刻：日期偏向临近开场，小时按晚间峰值加权。"""
    span = (end - start).total_seconds()
    if span <= 0:
        return None
    moment = start + timedelta(seconds=rng.random() ** 0.7 * span)
    day = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    moment = day + timedelta(hours=rng.choices(HOURS, weights=HOUR_WEIGHTS)[0],
                             minutes=rng.randrange(60), seconds=rng.randrange(60))
    if moment >= end:
        moment = end - timedelta(seconds=rng.randrange(60, 3600))
    if moment < start:
        moment = start + timedelta(seconds=rng.randrange(60, 3600))
    return moment


class SeatPool:
    """单个场次的座位可用性时间线。

    用最小堆维护「该座位何时重新可用」：已售出永不复用（不回到堆里），
    退款与超时按释放时刻回到堆里，因此同一座位可以被二次售出，
    与 payment.refund 把库存改回 AVAILABLE 的行为一致。
    """

    __slots__ = ("heap", "meta")

    def __init__(self, rows):
        self.heap = [(float("-inf"), row["session_seat_id"]) for row in rows]
        heapq.heapify(self.heap)
        self.meta = {
            row["session_seat_id"]: (row["price"], row["row_label"], row["seat_number"]) for row in rows
        }

    def take(self, rng, count, moment):
        """取 count 个在 moment 时空闲的座位；不足则返回 None 且不改变状态。"""
        if len(self.heap) < count:
            return None
        stamp = moment.timestamp()
        candidates = []
        while self.heap and self.heap[0][0] <= stamp and len(candidates) < count + 12:
            candidates.append(heapq.heappop(self.heap))
        if len(candidates) < count:
            for item in candidates:
                heapq.heappush(self.heap, item)
            return None
        # 优先同一排的连座，找不到再退化为任意可用座位。
        by_row = defaultdict(list)
        for item in candidates:
            by_row[self.meta[item[1]][1]].append(item)
        best_row = max(by_row.values(), key=len)
        if len(best_row) >= count:
            chosen = sorted(best_row, key=lambda item: self.meta[item[1]][2])[:count]
        else:
            chosen = candidates[:count]
        taken = {item[1] for item in chosen}
        for item in candidates:
            if item[1] not in taken:
                heapq.heappush(self.heap, item)
        return [item[1] for item in chosen]

    def release(self, seat_ids, moment):
        stamp = moment.timestamp()
        for seat_id in seat_ids:
            heapq.heappush(self.heap, (stamp, seat_id))


# ---------------------------------------------------------------- 世界模型


def _ensure_organizer(conn):
    conn.execute(
        "INSERT INTO app_user (email, display_name, role) VALUES (%s, %s, 'ORGANIZER') "
        "ON CONFLICT (lower(btrim(email))) DO NOTHING",
        (ORGANIZER_EMAIL, f"{SEED_TAG}-主办方"),
    )
    row = conn.execute(
        "SELECT user_id FROM app_user WHERE lower(btrim(email)) = %s AND role IN ('ORGANIZER', 'ADMIN')",
        (ORGANIZER_EMAIL,),
    ).fetchone()
    if not row:
        raise RuntimeError("造数主办方账号状态异常，请检查 app_user")
    return row["user_id"]


def _create_world(conn, scale, rng, now, organizer_id, days_back, days_ahead):
    """创建或复用场馆、座位、活动、场次与库存，全部按名称幂等。"""
    venues = []
    for index in range(scale.venues):
        name = f"{SEED_TAG}-场馆{index + 1}"
        address = f"{SEED_TAG}校区 {index + 1} 号楼"
        row = conn.execute("SELECT venue_id, status FROM venue WHERE name = %s AND address = %s",
                           (name, address)).fetchone()
        if not row:
            row = conn.execute("INSERT INTO venue (name, address) VALUES (%s, %s) RETURNING venue_id, status",
                               (name, address)).fetchone()
        if row["status"] != "ACTIVE":
            raise RuntimeError(f"造数场馆 {name} 已停用；请先恢复，或 --reset 重建")
        venues.append(row["venue_id"])

    seat_rows = {}
    for venue_id in venues:
        payload = []
        for index in range(scale.seats_per_venue):
            label = _row_label(index // 20)
            number = index % 20 + 1
            payload.append((venue_id, f"{label}{number}", label, number))
        _insert_many(conn,
                     "INSERT INTO seat (venue_id, seat_code, row_label, seat_number) VALUES (%s, %s, %s, %s) "
                     "ON CONFLICT (venue_id, seat_code) DO NOTHING",
                     payload)
        seat_rows[venue_id] = conn.execute(
            "SELECT seat_id, row_label FROM seat WHERE venue_id = %s AND status = 'ACTIVE' "
            "ORDER BY row_label, seat_number", (venue_id,)).fetchall()

    titles = ACTIVITY_TITLES[:scale.activities]
    while len(titles) < scale.activities:
        titles.append(f"{SEED_TAG}演出 {len(titles) + 1}")

    activities = []
    for index, title in enumerate(titles):
        row = conn.execute("SELECT activity_id, status FROM activity WHERE title = %s AND organizer_id = %s",
                           (title, organizer_id)).fetchone()
        if not row:
            row = conn.execute(
                "INSERT INTO activity (title, description, organizer_id, status) "
                "VALUES (%s, %s, %s, 'PUBLISHED') RETURNING activity_id, status",
                (title, f"{SEED_TAG}批量数据：{title}", organizer_id),
            ).fetchone()
        if row["status"] != "PUBLISHED":
            raise RuntimeError(f"造数活动「{title}」未发布，不自动修改")
        activities.append((row["activity_id"], venues[index % len(venues)],
                           Decimal(ACTIVITY_PRICE_FACTORS[index % len(ACTIVITY_PRICE_FACTORS)])))

    # 场次：每个活动都跨过去与未来，过去场次收尾为 FINISHED，未来场次为 ON_SALE。
    past_count = max(1, scale.sessions_per_activity - max(1, round(scale.sessions_per_activity * 0.3)))
    future_count = max(0, scale.sessions_per_activity - past_count)
    now_local = now.astimezone(BUSINESS_TZ)
    payload = []
    for activity_id, venue_id, _ in activities:
        offsets = [-day for day in sorted(rng.sample(range(1, days_back + 1), past_count))]
        if future_count:
            offsets += sorted(rng.sample(range(1, days_ahead + 1), future_count))
        for offset in offsets:
            start = (now_local + timedelta(days=offset)).replace(
                hour=rng.choice([14, 19, 20]), minute=0, second=0, microsecond=0)
            if now <= start <= now + HOLD:
                continue  # 开场在即，既不能卖票也留不下有意义的历史
            payload.append((activity_id, venue_id, start, start + timedelta(hours=rng.choice([2, 2, 3])),
                            "ON_SALE" if start > now else "FINISHED"))
    _insert_many(conn,
                 "INSERT INTO activity_session (activity_id, venue_id, start_time, end_time, status) "
                 "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (activity_id, venue_id, start_time) DO NOTHING",
                 payload)

    # 开演时间已过的场次统一收尾；重复运行时也能修正上次留下的在售状态。
    conn.execute(
        "UPDATE activity_session s SET status = 'FINISHED' FROM activity a "
        "WHERE a.activity_id = s.activity_id AND a.organizer_id = %s "
        "AND s.start_time <= clock_timestamp() AND s.status = 'ON_SALE'", (organizer_id,))

    # 库存：每个场次 × 该场馆全部座位；价格按排数分档，再乘活动系数。
    sessions = _load_sessions(conn, organizer_id)
    inventory = []
    factors = {activity_id: factor for activity_id, _, factor in activities}
    for session in sessions:
        factor = factors[session["activity_id"]]
        for seat in seat_rows[session["venue_id"]]:
            row = seat["row_label"]
            tier = 380 if row <= "C" else 280 if row <= "F" else 180 if row <= "J" else 120
            inventory.append((session["session_id"], seat["seat_id"], session["venue_id"],
                              (Decimal(tier) * factor).quantize(Decimal("0.01"))))
    _insert_many(conn,
                 "INSERT INTO session_seat (session_id, seat_id, venue_id, price) VALUES (%s, %s, %s, %s) "
                 "ON CONFLICT (session_id, seat_id) DO NOTHING", inventory)


def _load_sessions(conn, organizer_id):
    return conn.execute(
        "SELECT s.session_id, s.activity_id, s.venue_id, s.start_time, s.status "
        "FROM activity_session s JOIN activity a USING (activity_id) "
        "WHERE a.organizer_id = %s ORDER BY s.start_time", (organizer_id,)).fetchall()


def _load_pools(conn, sessions):
    """只把当前 AVAILABLE 的库存放进池子；已锁定/已售出的不再参与分配。"""
    inventory = conn.execute(
        "SELECT ss.session_seat_id, ss.session_id, ss.price, p.row_label, p.seat_number "
        "FROM session_seat ss JOIN seat p USING (seat_id) "
        "WHERE ss.session_id = ANY(%s) AND ss.status = 'AVAILABLE'",
        ([session["session_id"] for session in sessions],)).fetchall()
    grouped = defaultdict(list)
    for row in inventory:
        grouped[row["session_id"]].append(row)
    return {session_id: SeatPool(rows) for session_id, rows in grouped.items()}


def _ensure_users(conn, count):
    """生成下单用户；邮箱固定在 seed.invalid，便于 --reset 精确清理。"""
    users = []
    for index in range(count):
        users.append({
            "email": f"seed.u{index:05d}@{SEED_DOMAIN}",
            "name": f"{SEED_TAG}用户{index:05d}",
        })
    _insert_many(conn,
                 "INSERT INTO app_user (email, display_name, role) VALUES (%s, %s, 'CUSTOMER') "
                 "ON CONFLICT (lower(btrim(email))) DO NOTHING",
                 [(user["email"], user["name"]) for user in users])
    found = {row["email"].lower(): row["user_id"] for row in conn.execute(
        "SELECT user_id, email FROM app_user WHERE email = ANY(%s)",
        ([user["email"] for user in users],))}
    for user in users:
        user["user_id"] = found[user["email"].lower()]
    return users


def _load_users(conn):
    rows = conn.execute("SELECT user_id, email, display_name FROM app_user WHERE email LIKE %s ORDER BY user_id",
                        (f"seed.u%@{SEED_DOMAIN}",)).fetchall()
    if not rows:
        raise RuntimeError("--append 需要先执行一次完整造数")
    return [{"user_id": row["user_id"], "email": row["email"], "name": row["display_name"]} for row in rows]


# ---------------------------------------------------------------- 订单生成


def _sale_window(session, now, days_back):
    start = max(session["start_time"] - timedelta(days=SALE_DAYS), now - timedelta(days=days_back))
    end = min(session["start_time"] - HOLD - timedelta(minutes=1), now)
    return (start, end) if start < end else None


def _pick_outcome(rng, created_at, now):
    if created_at + HOLD > now:
        return "PENDING"
    refund, expired, cancelled = ORDER_OUTCOME
    roll = rng.random()
    if roll < refund:
        return "REFUNDED"
    if roll < refund + expired:
        return "EXPIRED"
    if roll < refund + expired + cancelled:
        return "CANCELLED"
    return "PAID"


def _plan_orders(rng, users, sessions, now, days_back, target, activity_heat):
    """先排「谁、在什么时刻、订哪场、几人」，之后统一按时间顺序分配座位。"""
    weight = {}
    for session in sessions:
        window = _sale_window(session, now, days_back)
        weight[session["session_id"]] = 0 if not window else 1.0 + activity_heat.get(session["activity_id"], 0.0)
    candidates = [session for session in sessions if weight[session["session_id"]] > 0]
    if not candidates:
        raise RuntimeError("没有可售场次；请增大 --days-ahead，或改用 --reset 重建")
    pool_weights = [weight[session["session_id"]] for session in candidates]

    base_counts = [rng.choices(USER_ORDER_COUNTS, weights=USER_ORDER_WEIGHTS)[0] for _ in users]
    factor = target / max(1, sum(base_counts))
    counts = []
    for base in base_counts:
        # 随机取整：期望总和正好落在 --orders 上，小批量时自然不会人人有单。
        value = base * factor
        counts.append(int(value) + (1 if rng.random() < value - int(value) else 0))
    if not sum(counts):
        counts[0] = 1

    plans = []
    for user, count in zip(users, counts):
        if not count:
            continue
        for _ in range(count):
            session = rng.choices(candidates, weights=pool_weights)[0]
            window = _sale_window(session, now, days_back)
            if not window:
                continue
            plans.append({"user": user, "session": session,
                          "created_at": _random_moment(rng, window[0], window[1]),
                          "seats": rng.choices(SEAT_COUNTS, weights=SEAT_WEIGHTS)[0]})
    plans.sort(key=lambda plan: plan["created_at"])
    return plans


def _new_order(plan, seats, total, created_at):
    return {
        "user": plan["user"],
        "session_id": None,
        "created_at": created_at,
        "expires_at": created_at + HOLD,
        "status": None,
        "seats": seats,
        "total": total,
        "paid_at": None,
        "refunded_at": None,
        "attempts": [],
        "refund_attempts": [],
    }


def _materialize(plans, pools, sessions, rng, now, pending_target):
    """按时间顺序分配座位，并补齐支付、退款、票与状态历史。"""
    orders = []
    dropped = 0
    for plan in plans:
        moment = plan["created_at"]
        wanted = plan["seats"]
        chosen = None
        pool = pools.get(plan["session"]["session_id"])
        seats = pool.take(rng, wanted, moment) if pool else None
        if seats:
            chosen = (plan["session"], pool, seats)
        else:
            # 该场次售罄时换一场，和用户在页面上挑另一场的行为一致。
            for _ in range(3):
                alternative = sessions[rng.randrange(len(sessions))]
                other = pools.get(alternative["session_id"])
                if not other:
                    continue
                seats = other.take(rng, wanted, moment)
                if seats:
                    chosen = (alternative, other, seats)
                    break
        if not chosen:
            dropped += 1
            continue
        session, pool, seats = chosen
        order = _new_order(plan, [(seat_id, pool.meta[seat_id][0]) for seat_id in seats],
                           Decimal("0.00"), moment)
        order["session_id"] = session["session_id"]
        order["status"] = _pick_outcome(rng, moment, now)
        _settle(order, pool, rng, now)
        orders.append(order)

    # 补一批「进行中」的锁座订单，让库存里能看到 LOCKED，前端也有真实占用。
    live = [plan for plan in plans if plan["session"]["start_time"] > now + HOLD]
    rng.shuffle(live)
    added = 0
    for plan in live:
        if added >= pending_target:
            break
        pool = pools.get(plan["session"]["session_id"])
        if not pool:
            continue
        moment = now - timedelta(seconds=rng.randrange(30, 540))
        seats = pool.take(rng, min(plan["seats"], 4), moment)
        if not seats:
            continue
        order = _new_order(plan, [(seat_id, pool.meta[seat_id][0]) for seat_id in seats],
                           Decimal("0.00"), moment)
        order["session_id"] = plan["session"]["session_id"]
        order["status"] = "PENDING"
        _settle(order, pool, rng, now)
        orders.append(order)
        added += 1

    orders.sort(key=lambda order: order["created_at"])
    return orders, dropped


def _settle(order, pool, rng, now):
    """补齐支付/退款时间，并把座位的占用或释放写回池子。"""
    order["total"] = sum((price for _, price in order["seats"]), Decimal("0.00"))
    status = order["status"]

    if status in ("PAID", "REFUNDED"):
        paid_at = min(order["created_at"] + timedelta(seconds=rng.randrange(25, 540)),
                      order["expires_at"] - timedelta(seconds=1))
        if paid_at <= order["created_at"]:
            paid_at = order["created_at"] + timedelta(seconds=20)
        order["paid_at"] = paid_at

        if rng.random() < PAYMENT_FAILURE_RATE:
            order["attempts"].append(("FAILED", order["created_at"] + timedelta(seconds=rng.randrange(20, 300))))
        order["attempts"].append(("SUCCEEDED", paid_at))

        if status == "REFUNDED":
            refunded_at = paid_at + timedelta(seconds=rng.randrange(3600, 12 * 24 * 3600))
            if refunded_at > now:
                order["status"] = "PAID"  # 退款时间还没到，订单就还是已支付
            else:
                order["refunded_at"] = refunded_at
                if rng.random() < 0.08:
                    order["refund_attempts"].append(
                        ("FAILED", refunded_at - timedelta(seconds=rng.randrange(30, 300))))
                order["refund_attempts"].append(("SUCCEEDED", refunded_at))
                pool.release([seat_id for seat_id, _ in order["seats"]], refunded_at)

    if order["status"] in ("EXPIRED", "CANCELLED", "PENDING"):
        pool.release([seat_id for seat_id, _ in order["seats"]], order["expires_at"])


# ---------------------------------------------------------------- 落库


def _persist(conn, orders, rng, run_id):
    order_rows = []
    for index, order in enumerate(orders):
        order["order_no"] = "O" + _uuid("order", run_id, index).hex
        order_rows.append((
            order["order_no"], order["user"]["user_id"], order["session_id"],
            _uuid("request", run_id, "order", index), order["total"], order["status"],
            order["expires_at"], order["created_at"], order["paid_at"], order["refunded_at"],
        ))
    _insert_many(conn, (
        "INSERT INTO booking_order (order_no, user_id, session_id, request_id, total_amount, "
        "status, expires_at, created_at, paid_at, refunded_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"), order_rows)

    ids = {row["order_no"]: row["order_id"] for row in conn.execute(
        "SELECT order_id, order_no FROM booking_order WHERE order_no = ANY(%s)",
        ([row[0] for row in order_rows],))}
    for order in orders:
        order["order_id"] = ids[order["order_no"]]

    item_rows, ticket_index = [], []
    for order in orders:
        for seat_id, price in order["seats"]:
            item_rows.append((order["order_id"], order["session_id"], seat_id, price, order["created_at"]))
            ticket_index.append((order, seat_id))
    _insert_many(conn, (
        "INSERT INTO booking_item (order_id, session_id, session_seat_id, unit_price, created_at) "
        "VALUES (%s, %s, %s, %s, %s)"), item_rows)

    items = {(row["order_id"], row["session_seat_id"]): row["item_id"] for row in conn.execute(
        "SELECT item_id, order_id, session_seat_id FROM booking_item WHERE order_id = ANY(%s)",
        ([order["order_id"] for order in orders],))}

    ticket_rows = []
    for index, (order, seat_id) in enumerate(ticket_index):
        if order["paid_at"] is None:
            continue
        refunded = order["status"] == "REFUNDED"
        ticket_rows.append((
            _uuid("ticket", run_id, index), items[(order["order_id"], seat_id)], order["order_id"], seat_id,
            "REFUNDED" if refunded else "VALID",
            order["paid_at"] + timedelta(seconds=rng.randrange(1, 20)),
            order["refunded_at"] if refunded else None,
        ))
    _insert_many(conn, (
        "INSERT INTO ticket (ticket_no, item_id, order_id, session_seat_id, status, issued_at, refunded_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"), ticket_rows)

    payment_rows, succeeded_by_order = [], {}
    for index, order in enumerate(orders):
        for attempt_no, (outcome, moment) in enumerate(order["attempts"]):
            payment_no = "P" + _uuid("payment", run_id, index, attempt_no).hex
            payment_rows.append((payment_no, order["order_id"], _uuid("payreq", run_id, index, attempt_no),
                                 order["total"], outcome, moment))
            if outcome == "SUCCEEDED":
                succeeded_by_order[order["order_id"]] = payment_no
    _insert_many(conn, (
        "INSERT INTO payment_attempt (payment_no, order_id, request_id, amount, status, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)"), payment_rows)

    payments = {row["payment_no"]: row["payment_id"] for row in conn.execute(
        "SELECT payment_id, payment_no FROM payment_attempt WHERE payment_no = ANY(%s)",
        ([row[0] for row in payment_rows],))}

    refund_rows = []
    for index, order in enumerate(orders):
        if order["status"] != "REFUNDED":
            continue
        payment_id = payments[succeeded_by_order[order["order_id"]]]
        for attempt_no, (outcome, moment) in enumerate(order["refund_attempts"]):
            refund_rows.append((
                "R" + _uuid("refund", run_id, index, attempt_no).hex, payment_id, order["order_id"],
                _uuid("refreq", run_id, index, attempt_no), order["total"],
                rng.choice(REFUND_REASONS), outcome, moment,
            ))
    _insert_many(conn, (
        "INSERT INTO refund_attempt (refund_no, payment_id, order_id, request_id, amount, reason, status, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"), refund_rows)

    history_rows = []
    for order in orders:
        actor = order["user"]["user_id"]
        history_rows.append((order["order_id"], None, "PENDING", "CREATED", actor, order["created_at"]))
        if order["status"] == "PAID":
            history_rows.append((order["order_id"], "PENDING", "PAID", "SIMULATED_PAYMENT", actor,
                                 order["paid_at"]))
        elif order["status"] == "REFUNDED":
            history_rows.append((order["order_id"], "PENDING", "PAID", "SIMULATED_PAYMENT", actor,
                                 order["paid_at"]))
            history_rows.append((order["order_id"], "PAID", "REFUNDED", "SIMULATED_REFUND", actor,
                                 order["refunded_at"]))
        elif order["status"] == "EXPIRED":
            history_rows.append((order["order_id"], "PENDING", "EXPIRED", "TIMEOUT", actor,
                                 order["expires_at"]))
        elif order["status"] == "CANCELLED":
            history_rows.append((order["order_id"], "PENDING", "CANCELLED", "USER_CANCEL", actor,
                                 order["created_at"] + timedelta(seconds=rng.randrange(30, 300))))
    _insert_many(conn, (
        "INSERT INTO order_status_history (order_id, from_status, to_status, reason, actor_user_id, changed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)"), history_rows)

    # 库存终态：后发生的订单覆盖先前的占用，因此按生成顺序（已按时间排序）依次赋值。
    final = {}
    for order in orders:
        for seat_id, _ in order["seats"]:
            if order["status"] == "PAID":
                final[seat_id] = ("SOLD", order["order_id"], None)
            elif order["status"] == "PENDING":
                final[seat_id] = ("LOCKED", order["order_id"], order["expires_at"])
            else:
                final[seat_id] = None
    sold = [(seat_id, value[1]) for seat_id, value in final.items() if value and value[0] == "SOLD"]
    locked = [(seat_id, value[1], value[2]) for seat_id, value in final.items() if value and value[0] == "LOCKED"]
    if sold:
        conn.execute(
            "UPDATE session_seat AS ss SET status = 'SOLD', sold_order_id = v.order_id, "
            "locked_order_id = NULL, locked_until = NULL "
            "FROM (SELECT unnest(%s::bigint[]) AS seat_id, unnest(%s::bigint[]) AS order_id) AS v "
            "WHERE ss.session_seat_id = v.seat_id",
            ([item[0] for item in sold], [item[1] for item in sold]))
    if locked:
        conn.execute(
            "UPDATE session_seat AS ss SET status = 'LOCKED', locked_order_id = v.order_id, "
            "locked_until = v.until, sold_order_id = NULL "
            "FROM (SELECT unnest(%s::bigint[]) AS seat_id, unnest(%s::bigint[]) AS order_id, "
            "unnest(%s::timestamptz[]) AS until) AS v WHERE ss.session_seat_id = v.seat_id",
            ([item[0] for item in locked], [item[1] for item in locked], [item[2] for item in locked]))


# ---------------------------------------------------------------- 清理与核对


def _reset(conn):
    """只清理本脚本创建的数据；依赖顺序严格逆序，外部引用会直接报错而不是静默跳过。"""
    organizer = conn.execute("SELECT user_id FROM app_user WHERE lower(btrim(email)) = %s",
                             (ORGANIZER_EMAIL,)).fetchone()
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS seed_order_ids (order_id BIGINT PRIMARY KEY) ON COMMIT DROP")
    conn.execute("TRUNCATE seed_order_ids")
    conn.execute(
        "INSERT INTO seed_order_ids (order_id) "
        "SELECT o.order_id FROM booking_order o "
        "JOIN activity_session s ON s.session_id = o.session_id "
        "JOIN activity a ON a.activity_id = s.activity_id "
        "WHERE a.organizer_id = %s", (organizer["user_id"] if organizer else -1,))
    for table in ("refund_attempt", "payment_attempt", "ticket", "order_status_history", "booking_item"):
        conn.execute(f"DELETE FROM {table} WHERE order_id IN (SELECT order_id FROM seed_order_ids)")
    # 库存先解绑再删订单：sold_order_id / locked_order_id 是 ON DELETE RESTRICT 的外键，
    # 且状态形状约束要求 AVAILABLE 时三列同时为空，所以一条 UPDATE 里一起清空。
    conn.execute(
        "UPDATE session_seat SET status = 'AVAILABLE', sold_order_id = NULL, "
        "locked_order_id = NULL, locked_until = NULL "
        "WHERE sold_order_id IN (SELECT order_id FROM seed_order_ids) "
        "   OR locked_order_id IN (SELECT order_id FROM seed_order_ids)")
    conn.execute("DELETE FROM booking_order WHERE order_id IN (SELECT order_id FROM seed_order_ids)")
    if organizer:
        conn.execute(
            "DELETE FROM session_seat WHERE session_id IN (SELECT s.session_id FROM activity_session s "
            "JOIN activity a USING (activity_id) WHERE a.organizer_id = %s)", (organizer["user_id"],))
        conn.execute("DELETE FROM activity_session WHERE activity_id IN "
                     "(SELECT activity_id FROM activity WHERE organizer_id = %s)", (organizer["user_id"],))
        conn.execute("DELETE FROM activity WHERE organizer_id = %s", (organizer["user_id"],))
    conn.execute("DELETE FROM seat WHERE venue_id IN (SELECT venue_id FROM venue WHERE name LIKE %s)",
                 (f"{SEED_TAG}-%",))
    conn.execute("DELETE FROM venue WHERE name LIKE %s", (f"{SEED_TAG}-%",))
    conn.execute("DELETE FROM app_user WHERE email LIKE %s", (f"seed.%@{SEED_DOMAIN}",))


def _reconcile(conn, session_ids):
    """四条对账：金额、支付、票、库存。返回 (名称, 是否一致, 说明)。"""
    checks = []

    row = conn.execute(
        "SELECT (SELECT COALESCE(sum(total_amount), 0) FROM booking_order WHERE session_id = ANY(%s)) AS order_sum, "
        "(SELECT COALESCE(sum(unit_price), 0) FROM booking_item WHERE session_id = ANY(%s)) AS item_sum",
        (session_ids, session_ids)).fetchone()
    checks.append(("订单总额 = 明细合计", row["order_sum"] == row["item_sum"],
                   f"{row['order_sum']:,.2f} vs {row['item_sum']:,.2f}"))

    row = conn.execute(
        "SELECT (SELECT count(*) FROM booking_order WHERE session_id = ANY(%s) "
        "        AND status IN ('PAID', 'REFUNDED')) AS paid, "
        "       (SELECT count(*) FROM payment_attempt p JOIN booking_order o USING (order_id) "
        "        WHERE o.session_id = ANY(%s) AND p.status = 'SUCCEEDED') AS succeeded",
        (session_ids, session_ids)).fetchone()
    checks.append(("成功支付数 = 已付订单数", row["paid"] == row["succeeded"],
                   f"{row['paid']} vs {row['succeeded']}"))

    row = conn.execute(
        "SELECT (SELECT count(*) FROM ticket t JOIN booking_order o USING (order_id) "
        "        WHERE o.session_id = ANY(%s)) AS tickets, "
        "       (SELECT count(*) FROM booking_item i JOIN booking_order o USING (order_id) "
        "        WHERE o.session_id = ANY(%s) AND o.status IN ('PAID', 'REFUNDED')) AS paid_items, "
        "       (SELECT count(*) FROM ticket t JOIN booking_order o USING (order_id) "
        "        WHERE o.session_id = ANY(%s) AND t.status = 'VALID') AS valid, "
        "       (SELECT count(*) FROM booking_item i JOIN booking_order o USING (order_id) "
        "        WHERE o.session_id = ANY(%s) AND o.status = 'PAID') AS paid_only",
        (session_ids, session_ids, session_ids, session_ids)).fetchone()
    checks.append(("票数 = 已付明细数", row["tickets"] == row["paid_items"],
                   f"{row['tickets']} vs {row['paid_items']}"))
    checks.append(("有效票 = 未退款明细数", row["valid"] == row["paid_only"],
                   f"{row['valid']} vs {row['paid_only']}"))

    row = conn.execute(
        "SELECT (SELECT count(*) FROM session_seat WHERE session_id = ANY(%s) AND status = 'SOLD') AS sold, "
        "       (SELECT count(*) FROM booking_item i JOIN booking_order o USING (order_id) "
        "        WHERE o.session_id = ANY(%s) AND o.status = 'PAID') AS paid_items",
        (session_ids, session_ids)).fetchone()
    checks.append(("SOLD 库存 = 已付明细数", row["sold"] == row["paid_items"],
                   f"{row['sold']} vs {row['paid_items']}"))
    return checks


def _report(conn, orders, session_ids, scale, elapsed, batch=False):
    """报表一律按造数场次汇总，追加模式下的「本批」单独标注，避免口径混用。"""
    inventory = conn.execute(
        "SELECT count(*) AS total, count(*) FILTER (WHERE status = 'SOLD') AS sold, "
        "       count(*) FILTER (WHERE status = 'LOCKED') AS locked "
        "FROM session_seat WHERE session_id = ANY(%s)", (session_ids,)).fetchone()
    sessions = conn.execute(
        "SELECT count(*) AS total, count(*) FILTER (WHERE status = 'ON_SALE') AS on_sale "
        "FROM activity_session WHERE session_id = ANY(%s)", (session_ids,)).fetchone()
    money = conn.execute(
        "SELECT COALESCE(sum(total_amount), 0) AS gmv, "
        "       COALESCE(sum(total_amount) FILTER (WHERE status = 'PAID'), 0) AS net, "
        "       COALESCE(sum(total_amount) FILTER (WHERE status = 'REFUNDED'), 0) AS refunded "
        "FROM booking_order WHERE session_id = ANY(%s)", (session_ids,)).fetchone()
    tickets = conn.execute(
        "SELECT count(*) AS total, count(*) FILTER (WHERE t.status = 'VALID') AS valid "
        "FROM ticket t JOIN booking_order o USING (order_id) WHERE o.session_id = ANY(%s)",
        (session_ids,)).fetchone()
    payments = conn.execute(
        "SELECT count(*) AS total, count(*) FILTER (WHERE p.status = 'SUCCEEDED') AS ok "
        "FROM payment_attempt p JOIN booking_order o USING (order_id) WHERE o.session_id = ANY(%s)",
        (session_ids,)).fetchone()
    status_count = {row["status"]: row["total"] for row in conn.execute(
        "SELECT status, count(*) AS total FROM booking_order WHERE session_id = ANY(%s) GROUP BY 1",
        (session_ids,))}

    print(f"\n造数完成：scale={scale}，用时 {elapsed:.1f}s")
    if batch:
        print(f"  本批      {len(orders):>6} 单，覆盖 {len({order['user']['user_id'] for order in orders})} 个用户")
    print(f"  场次      {sessions['total']:>6}   在售 {sessions['on_sale']} / 已结束 "
          f"{sessions['total'] - sessions['on_sale']}")
    occupancy = inventory["sold"] / inventory["total"] * 100 if inventory["total"] else 0
    print(f"  库存      {inventory['total']:>6}   售出 {inventory['sold']} / 锁定 {inventory['locked']} "
          f"/ 可售 {inventory['total'] - inventory['sold'] - inventory['locked']}（售出率 {occupancy:.1f}%）")
    print(f"  订单      {sum(status_count.values()):>6}   "
          + "、".join(f"{key} {value}" for key, value in sorted(status_count.items())))
    print(f"  票        {tickets['total']:>6}   有效 {tickets['valid']}")
    print(f"  支付尝试  {payments['total']:>6}   成功 {payments['ok']} / 失败 {payments['total'] - payments['ok']}")
    print(f"  金额      GMV {money['gmv']:,.2f} / 净收入 {money['net']:,.2f} / 退款 {money['refunded']:,.2f}")

    print("\n对账：")
    failed = 0
    for name, ok, detail in _reconcile(conn, session_ids):
        print(f"  [{'OK' if ok else '!!'}] {name}：{detail}")
        failed += 0 if ok else 1
    return failed


def run(args):
    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    scale = SCALES[args.scale]
    target_orders = args.orders or scale.orders
    run_id = f"{args.seed}-{int(now.timestamp())}"
    started = now

    with connect() as conn:
        if not conn.execute("SELECT has_table_privilege(current_user, 'venue', 'INSERT') AS value").fetchone()["value"]:
            raise RuntimeError("造数需要管理员连接：ticketing_app 对 venue/seat/session_seat 只有查询权限")

        existing = conn.execute("SELECT count(*) AS value FROM app_user WHERE email LIKE %s",
                                (f"seed.%@{SEED_DOMAIN}",)).fetchone()["value"]
        if existing and not (args.reset or args.append):
            raise RuntimeError("已存在造数数据；请加 --reset 重建，或 --append 追加一批")

        with conn.transaction():
            conn.execute("SET LOCAL statement_timeout = '900s'")
            if args.reset:
                _reset(conn)
                print("已清理历史造数数据")

            organizer_id = _ensure_organizer(conn)
            if not args.append:
                _create_world(conn, scale, rng, now, organizer_id, args.days_back, args.days_ahead)
            sessions = _load_sessions(conn, organizer_id)
            if not sessions:
                raise RuntimeError("没有可用场次；请检查造数场馆与活动状态")

            users = _load_users(conn) if args.append else _ensure_users(conn, max(20, round(target_orders / 2.4)))
            pools = _load_pools(conn, sessions)
            activity_heat = {row["activity_id"]: rng.uniform(0.2, 1.6) for row in conn.execute(
                "SELECT activity_id FROM activity WHERE organizer_id = %s", (organizer_id,))}
            plans = _plan_orders(rng, users, sessions, now, 14 if args.append else args.days_back,
                                 target_orders, activity_heat)
            orders, dropped = _materialize(plans, pools, sessions, rng, now, args.pending)
            _persist(conn, orders, rng, run_id)
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            failed = _report(conn, orders, [session["session_id"] for session in sessions], args.scale,
                             elapsed, batch=args.append)
            if dropped:
                print(f"\n提示：{dropped} 笔订单因场次售罄被跳过（可增大 --days-ahead 或减少 --orders）")
            if failed:
                raise RuntimeError(f"{failed} 项对账未通过，已整体回滚；请把上面的输出贴给维护者")

            if args.dry_run:
                raise _Rollback()

    if args.dry_run:
        print("\n--dry-run：数据已回滚，未写入数据库")


def main(argv=None):
    parser = argparse.ArgumentParser(description="为离线数仓生成有量级的票务交易数据")
    parser.add_argument("--scale", choices=sorted(SCALES), default="small", help="预设规模，默认 small")
    parser.add_argument("--orders", type=int, help="覆盖预设的订单数")
    parser.add_argument("--seed", type=int, default=20261111, help="随机种子，默认 20261111")
    parser.add_argument("--days-back", type=int, default=120, help="历史数据回溯天数，默认 120")
    parser.add_argument("--days-ahead", type=int, default=45, help="未来场次天数，默认 45")
    parser.add_argument("--pending", type=int, default=20, help="额外构造的进行中锁座订单数，默认 20")
    parser.add_argument("--reset", action="store_true", help="先清理本脚本创建的数据")
    parser.add_argument("--append", action="store_true", help="在已有造数数据上追加一批订单")
    parser.add_argument("--dry-run", action="store_true", help="演练后整体回滚，不留下数据")
    args = parser.parse_args(argv)

    try:
        if args.dry_run:
            try:
                run(args)
            except _Rollback:
                pass
            return 0
        run(args)
    except RuntimeError as exc:
        print(f"造数失败：{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
