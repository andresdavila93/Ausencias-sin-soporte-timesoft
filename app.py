import streamlit as st
import pandas as pd
import numpy as np
import re
import unicodedata
from datetime import timedelta
from io import BytesIO

# =========================
# Config
# =========================
st.set_page_config(page_title="Ausencias sin soporte", layout="wide")

# =========================
# Session State
# =========================
def init_state():
    defaults = {
        "ready": False,
        "excel_bytes": None,
        "file_name": None,
        "aus_sin_out": None,
        "summary": None,
        "params": None,
        "logs": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

def log(msg: str):
    st.session_state.logs.append(msg)

init_state()

# =========================
# Utils
# =========================
def normalize_text(s: str) -> str:
    """
    Normaliza textos para comparar nombres de columnas:
    - lowercase
    - sin tildes
    - deja solo alfanumérico (elimina ° . / etc.)
    """
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm_map = {normalize_text(c): c for c in df.columns}
    for cand in candidates:
        key = normalize_text(cand)
        if key in norm_map:
            return norm_map[key]
    return None

def clean_id(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, np.integer)):
        return str(int(x))
    if isinstance(x, float):
        return str(int(x)) if x.is_integer() else str(x).strip()
    s = str(x).strip().replace(" ", "")
    s = re.sub(r"\.0$", "", s)
    return s if s else None

def first_nonnull(series):
    for v in series:
        if pd.notna(v) and str(v).strip() != "":
            return v
    return np.nan

def effective_date_from_list(lst, end_date):
    cand = [d for d in (lst or []) if d <= end_date]
    return max(cand) if cand else None

def expand_ranges(df, p_start, p_end, id_col="id", ini_col="ini", fin_col="fin"):
    """
    Convierte rangos (ini-fin) a (id,fecha) diario recortado al periodo.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "fecha"])
    dfp = df[df[id_col].notna() & df[ini_col].notna() & df[fin_col].notna()].copy()
    dfp = dfp[(dfp[fin_col] >= p_start) & (dfp[ini_col] <= p_end)]
    out = []
    for _, r in dfp.iterrows():
        ini = max(r[ini_col], p_start)
        fin = min(r[fin_col], p_end)
        d = ini
        while d <= fin:
            out.append((r[id_col], d))
            d += timedelta(days=1)
    return pd.DataFrame(out, columns=["id", "fecha"]).drop_duplicates() if out else pd.DataFrame(columns=["id", "fecha"])

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def safe_select(df, cols):
    df = ensure_cols(df, cols)
    return df[cols]

# =========================
# SAP Parser robusto
# =========================
def _parse_sap_from_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    date_re = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
    num_re = re.compile(r"^\d{6,15}$")

    def parse_row(row):
        s = "\t".join([str(v) for v in row if pd.notna(v)])
        parts = [p.strip() for p in re.split(r"\t+", s) if p.strip() != ""]

        dates = [p for p in parts if date_re.match(p)]
        if len(dates) < 2:
            return None

        nums = [p for p in parts if num_re.match(p)]
        if len(nums) < 2:
            return None

        pernr = nums[0]
        cand = [n for n in nums[1:] if n != pernr]
        if not cand:
            return None
        cedula = max(cand, key=len)

        ini = pd.to_datetime(dates[0], format="%d.%m.%Y", errors="coerce")
        fin = pd.to_datetime(dates[1], format="%d.%m.%Y", errors="coerce")
        if pd.isna(ini) or pd.isna(fin):
            return None

        return {"id": clean_id(cedula), "ini": ini.date(), "fin": fin.date(), "pernr": pernr}

    rows = []
    for i in range(len(raw)):
        pr = parse_row(raw.iloc[i].tolist())
        if pr:
            rows.append(pr)

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["id", "ini", "fin", "pernr"])

def _parse_sap_from_text_lines(lines) -> pd.DataFrame:
    date_re = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
    num_re = re.compile(r"\b\d{6,15}\b")

    out = []
    for line in lines:
        dates = date_re.findall(line)
        if len(dates) < 2:
            continue

        nums = num_re.findall(line)
        if len(nums) < 2:
            continue

        pernr = nums[0]
        cand = [n for n in nums[1:] if n != pernr]
        if not cand:
            continue

        cedula = max(cand, key=len)

        ini = pd.to_datetime(dates[0], format="%d.%m.%Y", errors="coerce")
        fin = pd.to_datetime(dates[1], format="%d.%m.%Y", errors="coerce")
        if pd.isna(ini) or pd.isna(fin):
            continue

        out.append({"id": clean_id(cedula), "ini": ini.date(), "fin": fin.date(), "pernr": pernr})

    return pd.DataFrame(out) if out else pd.DataFrame(columns=["id", "ini", "fin", "pernr"])

def parse_sap_report(file_bytes: bytes, filename: str) -> pd.DataFrame:
    import io

    # 1) Excel por extensión
    try:
        if filename.endswith(".xls"):
            raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0, header=None, engine="xlrd")
        else:
            raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0, header=None, engine="openpyxl")
        return _parse_sap_from_dataframe(raw)
    except Exception:
        pass

    # 2) HTML / texto
    try:
        txt = file_bytes.decode("utf-8", errors="ignore")
    except Exception:
        txt = file_bytes.decode("latin-1", errors="ignore")

    if "<table" in txt.lower():
        try:
            tables = pd.read_html(txt)
            if tables:
                raw = tables[0].astype(str).reset_index(drop=True)
                return _parse_sap_from_dataframe(raw)
        except Exception:
            pass

    return _parse_sap_from_text_lines(txt.splitlines())

# =========================
# Excel Export
# =========================
def build_output_excel(dfs: dict) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sh, df in dfs.items():
            df.to_excel(writer, sheet_name=sh[:31], index=False)
    buffer.seek(0)
    return buffer.read()

# =========================
# UI
# =========================
st.title("📌 Ausencias sin soporte (TS + Ausentismos + SAP + Retiros + MasterData)")

with st.sidebar:
    st.header("⚙️ Controles")
    show_debug = st.checkbox("Mostrar diagnóstico (logs)", value=False)

    if st.button("🧹 Limpiar resultados"):
        st.session_state.ready = False
        st.session_state.excel_bytes = None
        st.session_state.file_name = None
        st.session_state.aus_sin_out = None
        st.session_state.summary = None
        st.session_state.params = None
        st.session_state.logs = []
        st.rerun()

with st.expander("📘 Instructivo", expanded=True):
    st.markdown(
        """
1) Carga los 6 archivos.  
2) Selecciona el periodo (inicio y fin).  
3) Clic en **Generar**.  
4) Descarga el Excel consolidado (no se pierde al descargar).  

**Reglas:**
- Retiro = `Desde - 1 día` (Retiros)
- Ingreso = MasterData donde `Clase de fecha` contiene "alta"
- Activos: solo IDs con `Función` autorizada en `funciones_marcación`
- MasterData ID: **N° pers. / Nº pers.**
"""
    )

with st.form("main_form", clear_on_submit=False):
    c1, c2 = st.columns(2)

    with c1:
        f_horas = st.file_uploader("📄 Rep_Horas_laboradas.xlsx", type=["xlsx"])
        f_ausrep = st.file_uploader("📄 Rep_aususentismos.xlsx", type=["xlsx"])
        f_retiros = st.file_uploader("📄 Retiros.xlsx", type=["xlsx"])

    with c2:
        f_md = st.file_uploader("📄 Md_activos.xlsx", type=["xlsx"])
        f_func = st.file_uploader("📄 funciones_marcación.xlsx", type=["xlsx"])
        f_aussap = st.file_uploader("📄 Ausentismos_SAP (XLS / XLSX)", type=["xls", "xlsx"])

    d1, d2 = st.columns(2)
    with d1:
        fecha_inicio = st.date_input("Fecha inicio del periodo")
    with d2:
        fecha_fin = st.date_input("Fecha fin del periodo")

    run = st.form_submit_button("🚀 Generar consolidado")

# =========================
# Procesamiento
# =========================
if run:
    st.session_state.logs = []

    if not all([f_horas, f_ausrep, f_retiros, f_md, f_func, f_aussap]):
        st.error("Debes cargar los 6 archivos.")
        st.stop()

    if fecha_fin < fecha_inicio:
        st.error("La fecha fin no puede ser menor que la fecha inicio.")
        st.stop()

    period_start = fecha_inicio
    period_end = fecha_fin

    with st.spinner("Procesando..."):
        # Leer
        horas = normalize_cols(pd.read_excel(BytesIO(f_horas.read()), sheet_name=0, engine="openpyxl"))
        ausrep = normalize_cols(pd.read_excel(BytesIO(f_ausrep.read()), sheet_name=0, engine="openpyxl"))
        retiros = normalize_cols(pd.read_excel(BytesIO(f_retiros.read()), sheet_name=0, engine="openpyxl"))
        md = normalize_cols(pd.read_excel(BytesIO(f_md.read()), sheet_name=0, engine="openpyxl"))
        func = normalize_cols(pd.read_excel(BytesIO(f_func.read()), sheet_name=0, engine="openpyxl"))

        sap_bytes = f_aussap.read()
        sap_name = (f_aussap.name or "").lower()
        aussap2 = parse_sap_report(sap_bytes, sap_name)

        # Column mapping
        col_h_id = find_col(horas, ["IdentificacionEmpleado", "IdentificaciónEmpleado"])
        col_h_fecha = find_col(horas, ["FechaEntrada", "Fecha Entrada"])

        col_ar_id = find_col(ausrep, ["Identificacion", "Identificación"])
        col_ar_ini = find_col(ausrep, ["Fecha_Inicio", "Fecha Inicio"])
        col_ar_fin = find_col(ausrep, ["Fecha_Final", "Fecha Final"])

        col_r_id = find_col(retiros, ["Número ID", "Numero ID", "Nº ID", "No ID"])
        col_r_desde = find_col(retiros, ["Desde"])

        # ✅ MasterData: primero N° pers.
        col_md_id = find_col(md, [
            "N° pers.", "Nº pers.", "N°pers.", "Nºpers.", "No pers.", "Nro pers.",
            "Numero pers.", "Número pers.", "Numero de personal", "Numero personal",
            "Número ID", "Numero ID"
        ])
        col_md_func = find_col(md, ["Función", "Funcion"])
        col_md_clase = find_col(md, ["Clase de fecha", "Clase Fecha"])
        col_md_fecha = find_col(md, ["Fecha"])

        col_f_func = find_col(func, ["Función", "Funcion"])

        log(f"[TS] ID={col_h_id} | Fecha={col_h_fecha}")
        log(f"[Aus Rep] ID={col_ar_id} | Ini={col_ar_ini} | Fin={col_ar_fin}")
        log(f"[Retiros] ID={col_r_id} | Desde={col_r_desde}")
        log(f"[MD] ID={col_md_id} | Func={col_md_func} | Clase={col_md_clase} | Fecha={col_md_fecha}")
        log(f"[Funcs] Func={col_f_func}")
        log(f"[SAP] Registros parseados={len(aussap2)}")

        missing = []
        if not col_h_id or not col_h_fecha:
            missing.append("Rep_Horas_laboradas: IdentificacionEmpleado / FechaEntrada")
        if not col_ar_id or not col_ar_ini or not col_ar_fin:
            missing.append("Rep_aususentismos: Identificacion / Fecha_Inicio / Fecha_Final")
        if not col_r_id or not col_r_desde:
            missing.append("Retiros: Número ID / Desde")
        if not col_md_id or not col_md_func or not col_md_clase or not col_md_fecha:
            missing.append("Md_activos: N° pers. / Función / Clase de fecha / Fecha")
        if not col_f_func:
            missing.append("funciones_marcación: Función")

        if missing:
            st.error("Faltan columnas críticas:\n- " + "\n- ".join(missing))
            if show_debug:
                st.info("\n".join(st.session_state.logs))
            st.stop()

        # TS
        horas2 = horas.copy()
        horas2["id"] = horas2[col_h_id].apply(clean_id)
        horas2["fecha"] = pd.to_datetime(horas2[col_h_fecha], errors="coerce").dt.date
        marc = horas2[horas2["id"].notna() & horas2["fecha"].notna()][["id", "fecha"]].drop_duplicates()

        # Ausentismos reporte
        ausrep2 = ausrep.copy()
        ausrep2["id"] = ausrep2[col_ar_id].apply(clean_id)
        ausrep2["ini"] = pd.to_datetime(ausrep2[col_ar_ini], errors="coerce").dt.date
        ausrep2["fin"] = pd.to_datetime(ausrep2[col_ar_fin], errors="coerce").dt.date
        ausrep_days = expand_ranges(ausrep2, period_start, period_end)

        # Retiros
        retiros2 = retiros.copy()
        retiros2["id"] = retiros2[col_r_id].apply(clean_id)
        retiros2["Desde_dt"] = pd.to_datetime(retiros2[col_r_desde], errors="coerce").dt.date
        retiros2["FechaRetiro"] = retiros2["Desde_dt"].apply(lambda d: d - timedelta(days=1) if pd.notna(d) else None)

        ret_list = (
            retiros2.groupby("id")["FechaRetiro"]
            .apply(lambda s: sorted(set([d for d in s.dropna()])))
            .reset_index()
        )
        ret_list["RetiroEfectivo"] = ret_list["FechaRetiro"].apply(lambda lst: effective_date_from_list(lst, period_end))
        ret_list["ListaRetiros"] = ret_list["FechaRetiro"].apply(
            lambda lst: ", ".join([d.isoformat() for d in lst]) if isinstance(lst, list) else ""
        )

        # MasterData (ID = N° pers.)
        md2 = md.copy()
        md2["id"] = md2[col_md_id].apply(clean_id)
        md2["funcion"] = md2[col_md_func].astype(str).str.strip()
        md2["clase_fecha"] = md2[col_md_clase].astype(str).str.strip()
        md2["fecha_clase"] = pd.to_datetime(md2[col_md_fecha], errors="coerce").dt.date

        md2["ingreso"] = np.where(md2["clase_fecha"].str.lower().str.contains("alta"), md2["fecha_clase"], pd.NaT)
        md2["ingreso"] = pd.to_datetime(md2["ingreso"], errors="coerce").dt.date

        auth_funcs = set(func[col_f_func].dropna().astype(str).str.strip().unique())
        md2["autorizado_TS"] = md2["funcion"].isin(auth_funcs)

        ing_list = (
            md2.groupby("id")["ingreso"]
            .apply(lambda s: sorted(set([d for d in s.dropna()])))
            .reset_index()
        )
        ing_list["IngresoEfectivo"] = ing_list["ingreso"].apply(lambda lst: effective_date_from_list(lst, period_end))
        ing_list["ListaIngresos"] = ing_list["ingreso"].apply(
            lambda lst: ", ".join([d.isoformat() for d in lst]) if isinstance(lst, list) else ""
        )

        authorized_ids = set(md2.loc[md2["autorizado_TS"] & md2["id"].notna(), "id"].unique())

        # SAP days
        aussap_days = expand_ranges(aussap2, period_start, period_end)

        # Universo + grid
        ids_union = pd.Index(pd.concat([
            pd.Series(list(authorized_ids)),
            horas2["id"], ausrep2["id"], aussap2["id"], retiros2["id"]
        ]).dropna().unique())

        all_dates = pd.date_range(period_start, period_end, freq="D").date
        grid = pd.MultiIndex.from_product([ids_union, all_dates], names=["id", "fecha"]).to_frame(index=False)

        # flags
        grid = grid.merge(marc.assign(tiene_marcacion=True), on=["id", "fecha"], how="left")
        grid["tiene_marcacion"] = grid["tiene_marcacion"].fillna(False)

        grid = grid.merge(ausrep_days.assign(tiene_aus_rep=True), on=["id", "fecha"], how="left")
        grid["tiene_aus_rep"] = grid["tiene_aus_rep"].fillna(False)

        grid = grid.merge(aussap_days.assign(tiene_aus_sap=True), on=["id", "fecha"], how="left")
        grid["tiene_aus_sap"] = grid["tiene_aus_sap"].fillna(False)

        grid = grid.merge(ret_list[["id", "RetiroEfectivo"]], on="id", how="left")
        grid = grid.merge(ing_list[["id", "IngresoEfectivo"]], on="id", how="left")
        grid = grid.merge(md2[["id", "autorizado_TS", "funcion"]].drop_duplicates("id"), on="id", how="left")
        grid["autorizado_TS"] = grid["autorizado_TS"].fillna(False)

        def estado_periodo(ret, ing):
            if pd.isna(ret):
                if pd.isna(ing):
                    return "Sin masterdata (posible retirado)"
                if ing > period_end:
                    return "Ingreso posterior al periodo"
                return "Activo (MD)"
            if ret < period_start:
                return "Retirado antes del periodo"
            if ret <= period_end:
                return "Retirado en el periodo"
            return "Retiro despues del periodo"

        grid["estado_periodo"] = [estado_periodo(r, i) for r, i in zip(grid["RetiroEfectivo"], grid["IngresoEfectivo"])]

        def vigente(d, ing, ret):
            if pd.notna(ing) and d < ing:
                return False
            if pd.notna(ret) and d > ret:
                return False
            return True

        grid["vigente_dia"] = [vigente(d, i, r) for d, i, r in zip(grid["fecha"], grid["IngresoEfectivo"], grid["RetiroEfectivo"])]

        grid["sin_soporte"] = (
            grid["vigente_dia"]
            & (~grid["tiene_marcacion"])
            & (~grid["tiene_aus_rep"])
            & (~grid["tiene_aus_sap"])
        )

        # considerar
        grid["considerar_activo_TS"] = (grid["estado_periodo"] == "Activo (MD)") & (grid["autorizado_TS"])
        grid["considerar"] = grid["considerar_activo_TS"] | grid["estado_periodo"].isin([
            "Retirado en el periodo", "Retirado antes del periodo", "Retiro despues del periodo", "Sin masterdata (posible retirado)"
        ])

        # Info master mínima (para no romper)
        info_master = pd.DataFrame({"id": ids_union})
        info_master = info_master.merge(md2[["id", "funcion"]].drop_duplicates("id"), on="id", how="left")
        info_master = info_master.merge(ret_list[["id", "ListaRetiros"]], on="id", how="left")
        info_master = info_master.merge(ing_list[["id", "ListaIngresos"]], on="id", how="left")

        def obs(stt):
            return {
                "Activo (MD)": "Activo autorizado TS: sin marcación y sin ausentismo (Reporte + SAP)",
                "Retirado en el periodo": "Retirado: sin marcación y sin ausentismo (Reporte + SAP) hasta fecha retiro",
                "Retiro despues del periodo": "Retiro posterior: sin marcación y sin ausentismo (Reporte + SAP) en el periodo",
                "Sin masterdata (posible retirado)": "Sin masterdata: sin marcación y sin ausentismo (Reporte + SAP) en el periodo"
            }.get(stt, "Sin marcación y sin ausentismo (Reporte + SAP)")

        aus_sin = grid[grid["considerar"] & grid["sin_soporte"]].merge(info_master, on="id", how="left")
        aus_sin["Observacion"] = aus_sin["estado_periodo"].map(obs)

        detail_cols = [
            "id", "funcion", "autorizado_TS", "fecha", "estado_periodo",
            "IngresoEfectivo", "RetiroEfectivo",
            "tiene_marcacion", "tiene_aus_rep", "tiene_aus_sap",
            "sin_soporte", "Observacion", "ListaIngresos", "ListaRetiros"
        ]
        aus_sin_out = safe_select(aus_sin, detail_cols).sort_values(["estado_periodo", "id", "fecha"])

        # =========================
        # RESUMEN ROBUSTO (NO KEYERROR)
        # =========================
        g = grid[grid["considerar"]].merge(info_master, on="id", how="left")

        need_cols = [
            "funcion", "autorizado_TS", "estado_periodo",
            "IngresoEfectivo", "RetiroEfectivo",
            "ListaIngresos", "ListaRetiros",
            "fecha", "vigente_dia",
            "tiene_marcacion", "tiene_aus_rep", "tiene_aus_sap",
            "sin_soporte"
        ]
        missing_cols = [c for c in need_cols if c not in g.columns]
        if missing_cols:
            log(f"[RESUMEN] Columnas faltantes en g (se crean vacías): {missing_cols}")
            for c in missing_cols:
                if c in ["vigente_dia", "tiene_marcacion", "tiene_aus_rep", "tiene_aus_sap", "sin_soporte", "autorizado_TS"]:
                    g[c] = False
                else:
                    g[c] = np.nan

        for c in ["vigente_dia", "tiene_marcacion", "tiene_aus_rep", "tiene_aus_sap", "sin_soporte", "autorizado_TS"]:
            g[c] = g[c].fillna(False)

        summary = g.groupby("id").agg(
            funcion=("funcion", "first"),
            autorizado_TS=("autorizado_TS", "first"),
            estado_periodo=("estado_periodo", "first"),
            Ingreso=("IngresoEfectivo", "first"),
            Retiro=("RetiroEfectivo", "first"),
            ListaIngresos=("ListaIngresos", "first"),
            ListaRetiros=("ListaRetiros", "first"),
            DiasPeriodo=("fecha", "nunique"),
            DiasVigente=("vigente_dia", "sum"),
            DiasConMarcacion=("tiene_marcacion", "sum"),
            DiasAusReporte=("tiene_aus_rep", "sum"),
            DiasAusSAP=("tiene_aus_sap", "sum"),
            DiasSinSoporte=("sin_soporte", "sum"),
        ).reset_index()

        ultima_marc = g[g["tiene_marcacion"]].groupby("id")["fecha"].max().rename("UltimaMarcacion")
        summary = summary.merge(ultima_marc, on="id", how="left").sort_values(["estado_periodo", "DiasSinSoporte"], ascending=[True, False])

        # Hojas adicionales
        retiros_fuera = summary[summary["estado_periodo"] == "Retirado antes del periodo"].copy()
        retiros_fuera["TieneMovEnPeriodo"] = np.where(
            (retiros_fuera["DiasConMarcacion"] > 0) | (retiros_fuera["DiasAusReporte"] > 0) | (retiros_fuera["DiasAusSAP"] > 0),
            "SI", "NO"
        )

        ingresos_post = summary[summary["estado_periodo"] == "Ingreso posterior al periodo"].copy()

        inconsistencias = summary[
            ((summary["estado_periodo"] == "Ingreso posterior al periodo") & (summary["DiasConMarcacion"] > 0)) |
            ((summary["Ingreso"].notna()) & (summary["Retiro"].notna()) & (summary["Retiro"] < summary["Ingreso"]) & (summary["DiasConMarcacion"] > 0))
        ].copy()

        params = pd.DataFrame({
            "Parametro": [
                "Periodo_inicio", "Periodo_fin",
                "MD_id_col_usada",
                "Regla_retiro", "Regla_ingreso", "Regla_activos_TS",
                "Cantidad_funciones_autorizadas", "Ausentismos_SAP_parseados"
            ],
            "Valor": [
                str(period_start), str(period_end),
                str(col_md_id),
                "Fecha retiro = Desde - 1 día",
                "Ingreso = Fecha (Clase de fecha contiene 'alta')",
                "Activos: SOLO IDs en MasterData con función autorizada (TS)",
                str(len(auth_funcs)),
                str(len(aussap2))
            ]
        })

        dfs = {
            "Parametros": params,
            "Ausencias_sin_soporte": aus_sin_out,
            "Resumen_periodo": summary,
            "Retiros_fuera_rango": retiros_fuera,
            "Ingresos_posteriores": ingresos_post,
            "Inconsistencias": inconsistencias,
        }

        excel_bytes = build_output_excel(dfs)
        file_name = f"Ausencias_sin_soporte_{period_start}_{period_end}.xlsx"

        st.session_state.excel_bytes = excel_bytes
        st.session_state.file_name = file_name
        st.session_state.aus_sin_out = aus_sin_out
        st.session_state.summary = summary
        st.session_state.params = params
        st.session_state.ready = True

# =========================
# Resultados (persistentes)
# =========================
if st.session_state.ready:
    st.success("Listo ✅. Ya puedes revisar y descargar (no se pierde al descargar).")

    tabs = st.tabs(["📄 Detalle", "📊 Resumen", "⚙️ Parámetros", "🧾 Diagnóstico"])
    with tabs[0]:
        st.dataframe(st.session_state.aus_sin_out, use_container_width=True, height=520)
    with tabs[1]:
        st.dataframe(st.session_state.summary, use_container_width=True, height=520)
    with tabs[2]:
        st.dataframe(st.session_state.params, use_container_width=True, height=240)
    with tabs[3]:
        st.write("\n".join(st.session_state.logs) if st.session_state.logs else "Sin logs.")
        st.caption("En Parámetros, 'MD_id_col_usada' debe quedar como N° pers. / Nº pers.")
        if show_debug:
            st.info("\n".join(st.session_state.logs))

    st.download_button(
        label="⬇️ Descargar Excel consolidado",
        data=st.session_state.excel_bytes,
        file_name=st.session_state.file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_excel_fixed",
    )
else:
    st.info("Carga archivos, selecciona el periodo y presiona **Generar consolidado**.")
