import threading
from typing import Any, Dict, Optional

import pandas as pd
import oracledb

_oracle_init_lock = threading.Lock()
_oracle_initialized = False
_oracle_pool = None


class OracleClientConfig:
    def __init__(self, host: str, port: int, service: str, user: str, password: str):
        self.host = host
        self.port = int(port)
        self.service = service
        self.user = user
        self.password = password



def _make_dsn(cfg: OracleClientConfig) -> str:
    return f"{cfg.host}:{cfg.port}/{cfg.service}"



def init_oracle_thick_mode_once(lib_dir: str = r"c:\instantclient") -> None:
    global _oracle_initialized
    if _oracle_initialized:
        return
    with _oracle_init_lock:
        if _oracle_initialized:
            return
        oracledb.init_oracle_client(lib_dir=lib_dir)
        _oracle_initialized = True
        print("[ORACLE] thick client initialized")



def _build_pool(cfg: OracleClientConfig):
    dsn = _make_dsn(cfg)
    return oracledb.create_pool(
        user=cfg.user,
        password=cfg.password,
        dsn=dsn,
        min=1,
        max=4,
        increment=1,
    )



def ensure_oracle_pool(cfg: OracleClientConfig, *, lib_dir: str = r"c:\instantclient"):
    global _oracle_pool
    init_oracle_thick_mode_once(lib_dir=lib_dir)
    if _oracle_pool is not None:
        return _oracle_pool

    with _oracle_init_lock:
        if _oracle_pool is not None:
            return _oracle_pool
        try:
            _oracle_pool = _build_pool(cfg)
            print("[ORACLE] pool created")
        except Exception as e:
            _oracle_pool = None
            print(f"[ORACLE] pool create failed: {e}")
    return _oracle_pool



def run_oracle_query_compat(
    sql: str,
    params: Optional[Dict[str, Any]],
    *,
    host: str,
    port: int,
    service: str,
    user: str,
    password: str,
    lib_dir: str = r"c:\instantclient",
) -> pd.DataFrame:
    cfg = OracleClientConfig(host=host, port=port, service=service, user=user, password=password)
    pool = ensure_oracle_pool(cfg, lib_dir=lib_dir)

    if pool is not None:
        with pool.acquire() as con:
            return pd.read_sql(sql, con, params=params)

    dsn = _make_dsn(cfg)
    con = oracledb.connect(user=cfg.user, password=cfg.password, dsn=dsn)
    print("[ORACLE] fallback connect used")
    try:
        return pd.read_sql(sql, con, params=params)
    finally:
        try:
            con.close()
        except Exception:
            pass


def startup_initialize(
    *,
    host: str,
    port: int,
    service: str,
    user: str,
    password: str,
    lib_dir: str = r"c:\instantclient",
) -> None:
    cfg = OracleClientConfig(host=host, port=port, service=service, user=user, password=password)
    ensure_oracle_pool(cfg, lib_dir=lib_dir)
