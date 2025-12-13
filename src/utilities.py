from __future__ import annotations

import json
import os
import time
import functools
from pathlib import Path
from typing import Any, Dict, Optional, Union, Callable, Iterable
import subprocess
import sys
from pyspark.sql import SparkSession
import pandas as pd

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
    
    @staticmethod
    def resolve_path(path: str, base_path: str | None = None) -> str:
        import os
        from pathlib import Path

        if path is None:
            raise ValueError("path no puede ser None")

        p = str(path).strip()

        if os.path.isabs(p):
            return str(Path(p).expanduser().resolve())

        if base_path:
            return str((Path(base_path).expanduser().resolve() / p).resolve())

        return str(Path(p).expanduser().resolve())

    @staticmethod
    def pip_install_requirements(requirements_file: str, *, base_path: Optional[str] = None, logger=None) -> int:
    
        req_path = Utils.resolve_path(requirements_file, base_path=base_path)

        # Usar el mismo python del runtime
        cmd = [sys.executable, "-m", "pip", "install", "-r", req_path]
        res = subprocess.run(cmd, capture_output=True, text=True)

        if res.stdout:
            (logger or print)(res.stdout)
        if res.returncode != 0:
            if res.stderr:
                (logger or print)(res.stderr)
            raise RuntimeError(f"pip install falló (code={res.returncode}).")

        return res.returncode

    @staticmethod
    def pip_install_requirements_from_config(cfg: Dict[str, Any],*,key: str = "load_requirements",base_key: Optional[str] = "base_path",fallback_base_path: Optional[str] = None,logger=None) -> int:
        if key not in cfg:
            raise KeyError(f"No existe la llave '{key}' en la configuración.")

        req = cfg[key]
        if not isinstance(req, str):
            raise TypeError(f"cfg['{key}'] debe ser string (ruta a requirements.txt).")

        base_path = None
        if base_key and isinstance(cfg.get(base_key), str) and cfg.get(base_key):
            base_path = cfg.get(base_key)
        else:
            base_path = fallback_base_path

        return Utils.pip_install_requirements(req, base_path=base_path, logger=logger)
    
    @staticmethod
    def list_files(folder: str, pattern: str = "*", recursive: bool = True):

        root = Path(folder).expanduser().resolve()
        if not root.exists():
            return []

        if recursive:
            paths = root.rglob(pattern)
        else:
            paths = root.glob(pattern)

        return sorted([str(p.resolve()) for p in paths if p.is_file()])
    
    @staticmethod
    def get_spark(app_name="ETL", master="local[*]"):
        return SparkSession.builder.appName(app_name).master(master).getOrCreate()
    

    @staticmethod
    def cfg_get(cfg: dict, path: str, default=None):
        cur = cfg
        for p in path.split("."):
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    @staticmethod
    def leer_csv_flexible(ruta_csv: str, sep: str = ";"):

        try:
            return pd.read_csv(ruta_csv, sep=sep, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            try:
                return pd.read_csv(ruta_csv, sep=sep, encoding="latin-1", low_memory=False)
            except Exception as e:
                print(f"No se pudo leer el CSV {ruta_csv}: {e}")
                return None
        except Exception as e:
            print(f"No se pudo leer el CSV {ruta_csv}: {e}")
            return None

    @staticmethod
    def _build_reemplazo_regex(reemplazos: dict):
        import re
        return re.compile("|".join(map(re.escape, reemplazos.keys()))) if reemplazos else None

    @staticmethod
    def reparar_texto_pandas(s, reemplazos: dict, patron):
        if pd.isna(s) or patron is None:
            return s
        if not isinstance(s, str):
            s = str(s)
        return patron.sub(lambda m: reemplazos[m.group(0)], s)

    @staticmethod
    def reparar_dataframe_pandas(df, cfg: dict, reemplazos_path: str = "quality.reemplazos_comunes"):
        reemplazos = Utils.cfg_get(cfg, reemplazos_path, default={})
        patron = Utils._build_reemplazo_regex(reemplazos)

        out = df.copy()
        cols_texto = out.select_dtypes(include=["object", "string"]).columns
        for c in cols_texto:
            out[c] = out[c].apply(lambda x: Utils.reparar_texto_pandas(x, reemplazos, patron))
        return out

    @staticmethod
    def unificar_columnas_duplicadas(df, cfg: dict, dup_path: str = "quality.duplicate_groups"):
        
        duplicate_groups = Utils.cfg_get(cfg, dup_path, default={})
        out = df.copy()
        out = out.replace(r"^\s*$", pd.NA, regex=True)

        for canon, variantes in duplicate_groups.items():
            presentes = [c for c in variantes if c in out.columns]
            if not presentes:
                continue

            if canon not in out.columns:
                out[canon] = out[presentes[0]]

            for other in presentes:
                if other == canon:
                    continue
                mask = out[canon].isna() & out[other].notna()
                if mask.any():
                    out.loc[mask, canon] = out.loc[mask, other]

            drop_cols = [c for c in presentes if c != canon]
            if drop_cols:
                out = out.drop(columns=drop_cols)

        return out

    @staticmethod
    def validar_csv_llamadas(
        ruta_csv: str,
        cfg: dict,
        sep: str = ";",
        min_fecha: str = "2010-01-01",
        max_fecha=None,
        columnas_path: str = "quality.columnas_esperadas",
        catalogo_path: str = "quality.catalogo_localidades",
    ):

        print(f"[VALIDACIÓN] Archivo: {ruta_csv}")

        if max_fecha is None:
            max_fecha = pd.Timestamp.today().normalize()

        df = Utils.leer_csv_flexible(ruta_csv, sep=sep)
        if df is None:
            issues = [{
                "archivo": ruta_csv,
                "check": "lectura_csv",
                "tipo": "ERROR",
                "detalle": "No se pudo leer el archivo",
                "filas_afectadas": None,
            }]
            return [], issues

        metrics = []
        issues = []

        n_filas, n_columnas = df.shape
        columnas_reales = list(df.columns)
        print(f"  Filas: {n_filas:,} | Columnas: {n_columnas}")

        cols_esp = Utils.cfg_get(cfg, columnas_path, default=[])
        cols_faltantes = [c for c in cols_esp if c not in columnas_reales]
        cols_extra = [c for c in columnas_reales if c not in cols_esp] if cols_esp else []

        print("  Columnas faltantes:", cols_faltantes if cols_faltantes else "NINGUNA")
        print("  Columnas extra:", cols_extra if cols_extra else "NINGUNA")

        metrics.append({
            "archivo": ruta_csv,
            "check": "schema",
            "filas": n_filas,
            "columnas": n_columnas,
            "cols_faltantes": ",".join(cols_faltantes) if cols_faltantes else "",
            "cols_extra": ",".join(cols_extra) if cols_extra else "",
        })

        if cols_faltantes:
            issues.append({
                "archivo": ruta_csv,
                "check": "schema_cols_faltantes",
                "tipo": "WARNING",
                "detalle": f"Faltan columnas: {cols_faltantes}",
                "filas_afectadas": n_filas,
            })

        col_fecha = "FECHA_INICIO_DESPLAZAMIENTO_MOVIL"
        if col_fecha in df.columns:
            fechas = pd.to_datetime(df[col_fecha], errors="coerce")
            fuera_rango_mask = (fechas < pd.to_datetime(min_fecha)) | (fechas > pd.to_datetime(max_fecha))
            fuera_rango = int(fuera_rango_mask.sum())

            print(f"  Rango fechas {col_fecha}: {fechas.min()} -> {fechas.max()} | fuera_rango={fuera_rango}")

            metrics.append({
                "archivo": ruta_csv,
                "check": "rango_fechas",
                "filas": n_filas,
                "fuera_rango": fuera_rango,
                "min_fecha": str(fechas.min()),
                "max_fecha": str(fechas.max()),
            })

            if fuera_rango > 0:
                issues.append({
                    "archivo": ruta_csv,
                    "check": "rango_fechas",
                    "tipo": "WARNING",
                    "detalle": f"Fechas fuera de rango [{min_fecha}, {max_fecha}]",
                    "filas_afectadas": fuera_rango,
                })

        catalogo_localidades = Utils.cfg_get(cfg, catalogo_path, default=None)
        if catalogo_localidades is not None and "CODIGO_LOCALIDAD" in df.columns:
            codigos_validos = set(catalogo_localidades.values()) | set(map(str, catalogo_localidades.values()))
            codigos = df["CODIGO_LOCALIDAD"].astype(str)
            mask_invalidos = ~codigos.isin(codigos_validos)
            n_invalidos = int(mask_invalidos.sum())

            print(f"  Códigos de localidad inválidos: {n_invalidos}")

            metrics.append({
                "archivo": ruta_csv,
                "check": "codigos_localidad",
                "filas": n_filas,
                "codigos_invalidos": n_invalidos,
            })

            if n_invalidos > 0:
                sample = df.loc[mask_invalidos, ["NUMERO_INCIDENTE", "CODIGO_LOCALIDAD"]].head(20)
                for _, row in sample.iterrows():
                    issues.append({
                        "archivo": ruta_csv,
                        "check": "codigo_localidad_invalido",
                        "tipo": "WARNING",
                        "detalle": f"Codigo localidad inválido: {row['CODIGO_LOCALIDAD']}",
                        "filas_afectadas": 1,
                    })

        print(f"[VALIDACIÓN] Finalizada: {ruta_csv}\n")
        
        return metrics, issues