"""Cấu hình duy nhất của Superset HDG local; không import cấu hình DataHub."""
import os
from urllib.parse import quote

HDG_BI_INSTANCE = os.environ['HDG_BI_INSTANCE']
if HDG_BI_INSTANCE != 'hdg-c360':
    raise ValueError('WRONG_BI_INSTANCE')
APP_NAME = 'HDG C360'
SECRET_KEY = os.environ['HDG_SUPERSET_SECRET_KEY']
SQLALCHEMY_DATABASE_URI = ('postgresql+psycopg2://hdg_superset:'
    + quote(os.environ['HDG_METADATA_PASSWORD'], safe='')
    + '@metadata:5432/hdg_superset')
SQLALCHEMY_TRACK_MODIFICATIONS = False
WTF_CSRF_ENABLED = True
SESSION_COOKIE_NAME = 'hdg_c360_session'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
# Cache trong RAM đủ cho demo local, không cần Redis/Celery hay examples.
CACHE_CONFIG = {'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 60}
DATA_CACHE_CONFIG = {'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 60}
FEATURE_FLAGS = {'ALERT_REPORTS': False}
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = False
