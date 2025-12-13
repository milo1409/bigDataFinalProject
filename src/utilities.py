from __future__ import annotations

import json
import os
import time
import functools
from pathlib import Path
from typing import Any, Dict, Optional, Union, Callable, Iterable

try:
    import psutil
except Exception:
    psutil = None


class Utils:

    @staticmethod
    def timeit(logger: Optional[Callable[[str], None]] = None, label: Optional[str] = None):
        def deco(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                t0 = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    dt = time.perf_counter() - t0
                    msg = f"[timeit] {label or fn.__name__}: {dt:.3f}s"
                    (logger or print)(msg)
            return wrapper
        return deco
    

    @staticmethod
    def resourceit(logger: Optional[Callable[[str], None]] = None, label: Optional[str] = None):
        def deco(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                proc = psutil.Process(os.getpid()) if psutil else None
                mem0 = proc.memory_info().rss if proc else None
                t0 = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    dt = time.perf_counter() - t0
                    if proc:
                        mem1 = proc.memory_info().rss
                        dmem = (mem1 - mem0) / (1024 * 1024)
                        mem1_mb = mem1 / (1024 * 1024)
                        msg = f"[resourceit] {label or fn.__name__}: {dt:.3f}s | RSS {mem1_mb:.1f} MB (Δ {dmem:+.1f} MB)"
                    else:
                        msg = f"[resourceit] {label or fn.__name__}: {dt:.3f}s | psutil no disponible (solo tiempo)"
                    (logger or print)(msg)
            return wrapper
        return deco
    
    @staticmethod
    def read_json_config(
        source: Union[str, Path, Dict[str, Any]],
        *,
        defaults: Optional[Dict[str, Any]] = None,
        required_keys: Optional[Iterable[str]] = None,
        encoding: str = "utf-8"
    ) -> Dict[str, Any]:

        cfg = Utils._load_json(source, encoding=encoding)
        if not isinstance(cfg, dict):
            raise ValueError("La configuración debe ser un objeto JSON (dict).")

        if defaults:
            cfg = Utils._deep_merge(defaults, cfg)

        if required_keys:
            missing = [k for k in required_keys if not Utils._has_path(cfg, k)]
            if missing:
                raise ValueError(f"Faltan llaves requeridas en config: {missing}")

        return cfg
    
    @staticmethod
    def _load_json(source: Union[str, Path, Dict[str, Any]], *, encoding: str = "utf-8") -> Any:
        if isinstance(source, dict):
            return source

        s = str(source)
        if s.lstrip().startswith(("{", "[")):
            return json.loads(s)

        p = Path(s)
        if not p.exists():
            raise FileNotFoundError(f"No existe el archivo: {p}")
        return json.loads(p.read_text(encoding=encoding))