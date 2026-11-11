"""FastAPI 网络入口：同步数据库调用在线程池执行，复用既有短事务服务。"""
from typing import Annotated
from pathlib import Path as FilePath
import logging

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.exceptions import HTTPException as StarletteHTTPException
import mimetypes
import psycopg

from .auth import AuthError, AuthService
from .booking import BookingError, BookingService, RequestConflict, SeatUnavailable, ORDER_VIEW_SQL
from .db import connect
from .payment import PaymentService
from . import schemas as s


logger = logging.getLogger('ticketing.api')
OrderID = Annotated[int, Path(gt=0, le=9223372036854775807)]
Limit = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0, le=1000000)]
SESSION_SQL = '''SELECT x.session_id,x.activity_id,a.title,x.venue_id,v.name AS venue_name,x.start_time,x.end_time
    FROM activity_session x JOIN activity a USING (activity_id) JOIN venue v ON v.venue_id=x.venue_id
    WHERE a.status='PUBLISHED' AND x.status='ON_SALE' AND v.status='ACTIVE' AND x.start_time>clock_timestamp()'''


def error(status, code, message, *, details=None, headers=None):
    body = {'error': {'code': code, 'message': message}}
    if details is not None:
        body['error']['details'] = details
    return JSONResponse(body, status_code=status, headers=headers)


def create_app(connect_factory=connect):
    app = FastAPI(title='Ticketing Warehouse API', version='0.5.0',
        responses={code: {'model': s.ErrorResponse} for code in (401,404,409,422,429,503)},
        description='本地教学票务系统：注册登录、下单锁座、模拟支付、电子票与开场前整单退款。支付不涉及真实资金。')
    auth = AuthService(connect_factory)
    booking, payment = BookingService(connect_factory), PaymentService(connect_factory)
    bearer = HTTPBearer(auto_error=False, description='先调用 /auth/login，将返回的 access_token 粘贴到此处。')

    @app.exception_handler(AuthError)
    async def auth_error(request, exc):
        headers = {'WWW-Authenticate': 'Bearer'} if exc.status == 401 else None
        if exc.status == 429:
            headers = {'Retry-After': '60'}
        return error(exc.status, exc.code, str(exc), headers=headers)

    @app.exception_handler(BookingError)
    async def booking_error(request, exc):
        code = 'request_conflict' if isinstance(exc, RequestConflict) else 'seat_unavailable' if isinstance(exc, SeatUnavailable) else 'business_conflict'
        return error(409, code, str(exc))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # 不返回原始 input/body，尤其不能把密码和令牌带入错误响应。
        details = [{'location': list(e['loc']), 'type': e['type']} for e in exc.errors()]
        return error(422, 'invalid_request', '请求参数不符合接口要求', details=details)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        code = {401: 'authentication_required', 404: 'not_found', 405: 'method_not_allowed'}.get(exc.status_code, 'http_error')
        return error(exc.status_code, code, str(exc.detail), headers=exc.headers)

    @app.exception_handler(psycopg.Error)
    async def database_error(request, exc):
        logger.error('数据库操作失败: %s', type(exc).__name__)
        return error(503, 'database_unavailable', '数据库暂时不可用，请稍后重试')

    def credentials(value: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]):
        if value is None:
            raise HTTPException(401, '请先登录', headers={'WWW-Authenticate': 'Bearer'})
        return value.credentials

    def current_user(token: Annotated[str, Depends(credentials)]):
        return auth.authenticate(token)

    def owned_user(order_id: OrderID, user: Annotated[dict, Depends(current_user)]):
        with connect_factory() as conn:
            found = conn.execute('SELECT order_id FROM booking_order WHERE order_id=%s AND user_id=%s',
                                 (order_id, user['user_id'])).fetchone()
        if not found:
            # 不区分不存在和属于他人，避免泄漏别人的订单是否存在。
            raise HTTPException(404, '订单不存在')
        return user['user_id']

    web_root = FilePath(__file__).resolve().parent / 'web'
    # Windows 会用注册表覆盖 Python 内置 MIME 表，可能把 .mjs/.js 判成 text/plain，
    # 导致浏览器拒绝加载 ES Module；这里显式登记，保证跨平台一致。
    mimetypes.add_type('text/javascript', '.mjs')
    mimetypes.add_type('text/javascript', '.js')
    mimetypes.add_type('text/css', '.css')
    app.mount('/assets', StaticFiles(directory=web_root), name='web-assets')

    @app.middleware('http')
    async def web_headers(request, call_next):
        response = await call_next(request)
        if request.url.path == '/' or request.url.path.startswith('/assets/'):
            response.headers['Content-Security-Policy'] = ("default-src 'self'; script-src 'self'; style-src 'self'; "
                "img-src 'self' data:; connect-src 'self'; font-src 'self'; base-uri 'none'; "
                "frame-ancestors 'none'; form-action 'self'; object-src 'none'")
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['Referrer-Policy'] = 'same-origin'
            response.headers['Cache-Control'] = 'no-cache'
        return response

    @app.get('/', include_in_schema=False)
    def web_home():
        return FileResponse(web_root / 'index.html', media_type='text/html')

    @app.get('/health/live', tags=['健康检查'])
    def live():
        return {'status': 'ok'}

    @app.get('/health/ready', tags=['健康检查'])
    def ready():
        with connect_factory() as conn:
            found = conn.execute("SELECT 1 FROM schema_migration WHERE filename='005_api_auth.sql'").fetchone()
        if not found:
            return error(503, 'migration_required', '数据库尚未完成 API 迁移')
        return {'status': 'ready'}

    @app.post('/auth/register', response_model=s.User, status_code=201, tags=['登录'])
    def register(body: s.Registration):
        return auth.register(str(body.email), body.display_name, body.password.get_secret_value())

    @app.post('/auth/login', response_model=s.Token, tags=['登录'])
    def login(body: s.Login, response: Response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Pragma'] = 'no-cache'
        return auth.login(str(body.email), body.password.get_secret_value())

    @app.get('/auth/me', response_model=s.User, tags=['登录'])
    def me(user: Annotated[dict, Depends(current_user)]):
        return user

    @app.post('/auth/logout', status_code=204, tags=['登录'])
    def logout(token: Annotated[str, Depends(credentials)], user: Annotated[dict, Depends(current_user)]):
        auth.logout(token)
        return Response(status_code=204)

    @app.get('/activities', response_model=list[s.Activity], tags=['活动与库存'])
    def activities(limit: Limit = 20, offset: Offset = 0):
        with connect_factory() as conn:
            return conn.execute('''SELECT a.activity_id,a.title,a.description FROM activity a
                WHERE a.status='PUBLISHED' AND EXISTS (SELECT 1 FROM activity_session x
                    JOIN venue v USING (venue_id) WHERE x.activity_id=a.activity_id AND x.status='ON_SALE'
                    AND v.status='ACTIVE' AND x.start_time>clock_timestamp())
                ORDER BY a.activity_id LIMIT %s OFFSET %s''', (limit, offset)).fetchall()

    @app.get('/sessions', response_model=list[s.Session], tags=['活动与库存'])
    def sessions(activity_id: Annotated[int | None, Query(gt=0, le=9223372036854775807)] = None,
                 limit: Limit = 20, offset: Offset = 0):
        with connect_factory() as conn:
            return conn.execute(SESSION_SQL + ' AND (%s::bigint IS NULL OR x.activity_id=%s) ORDER BY x.start_time,x.session_id LIMIT %s OFFSET %s',
                                (activity_id, activity_id, limit, offset)).fetchall()

    @app.get('/sessions/{session_id}/seats', response_model=list[s.Inventory], tags=['活动与库存'])
    def seats(session_id: Annotated[int, Path(gt=0, le=9223372036854775807)], limit: Limit = 100, offset: Offset = 0):
        with connect_factory() as conn, conn.transaction():
            if not conn.execute(SESSION_SQL + ' AND x.session_id=%s', (session_id,)).fetchone():
                raise HTTPException(404, '可售场次不存在')
            return conn.execute('''SELECT ss.session_seat_id,p.seat_code,p.row_label,p.seat_number,
                ss.price,ss.status,ss.locked_until FROM session_seat ss JOIN seat p USING (seat_id)
                WHERE ss.session_id=%s AND p.status='ACTIVE' ORDER BY ss.session_seat_id LIMIT %s OFFSET %s''',
                (session_id, limit, offset)).fetchall()

    @app.post('/orders', response_model=s.Order, status_code=201, tags=['订单'])
    def reserve(body: s.Reservation, response: Response, user: Annotated[dict, Depends(current_user)]):
        order = booking.reserve(user['user_id'], body.session_id, body.inventory_ids, request_id=body.request_id)
        response.headers['Location'] = f"/orders/{order['order_id']}"
        return booking.get(user['user_id'], order['order_id'])

    @app.get('/orders', response_model=list[s.OrderSummary], tags=['订单'])
    def orders(user: Annotated[dict, Depends(current_user)], limit: Limit = 20, offset: Offset = 0):
        with connect_factory() as conn:
            return conn.execute(ORDER_VIEW_SQL + ' WHERE o.user_id=%s ORDER BY o.order_id DESC LIMIT %s OFFSET %s',
                                (user['user_id'], limit, offset)).fetchall()

    @app.get('/orders/{order_id}', response_model=s.Order, tags=['订单'])
    def order(order_id: OrderID, user_id: Annotated[int, Depends(owned_user)]):
        return booking.get(user_id, order_id)

    @app.post('/orders/{order_id}/cancel', response_model=s.Order, tags=['订单'])
    def cancel(order_id: OrderID, user_id: Annotated[int, Depends(owned_user)]):
        booking.cancel(user_id, order_id)
        return booking.get(user_id, order_id)

    @app.post('/orders/{order_id}/payments', response_model=s.PaymentResult, tags=['模拟支付与退款'])
    def pay(order_id: OrderID, body: s.PaymentInput, user_id: Annotated[int, Depends(owned_user)]):
        return payment.pay(user_id, order_id, body.amount, request_id=body.request_id, outcome=body.outcome)

    @app.post('/orders/{order_id}/refunds', response_model=s.RefundResult, tags=['模拟支付与退款'])
    def refund(order_id: OrderID, body: s.RefundInput, user_id: Annotated[int, Depends(owned_user)]):
        return payment.refund(user_id, order_id, reason=body.reason, request_id=body.request_id, outcome=body.outcome)

    return app


app = create_app()
