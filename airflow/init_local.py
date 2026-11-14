"""Tạo admin/pool cho metadata Airflow 2 riêng; không kết nối dữ liệu nghiệp vụ."""
import json
from pathlib import Path
from airflow.www.app import create_app
from airflow.models.pool import Pool
from airflow.utils.session import create_session

# Secret chỉ đọc từ file local đã mount; không đưa vào argv hoặc log.
passwords=json.loads(Path('/opt/airflow/auth/passwords.json').read_text())
app=create_app()
with app.app_context():
    manager=app.appbuilder.sm
    if not manager.find_user(username='hdg_admin'):
        user=manager.add_user(username='hdg_admin',first_name='HDG',last_name='Admin',
            email='hdg-admin@localhost',role=manager.find_role('Admin'),password=passwords['hdg_admin'])
        if not user:raise RuntimeError('HDG_ADMIN_CREATE_FAILED')
# Init chạy lại không reset password và không tạo pool trùng.
with create_session() as session:
    if not session.query(Pool).filter(Pool.pool=='hdg_read_only').first():
        session.add(Pool(pool='hdg_read_only',slots=1,description='HDG serial read-only checks',include_deferred=False))
