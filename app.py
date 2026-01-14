import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import timedelta
from io import BytesIO

st.set_page_config(page_title="Ausencias sin soporte", layout="wide")

# ----------------------------
# Helpers
# ----------------------------
def clean_id(x):
    if pd.isna(x): return None
    if isinstance(x, (int, np.integer)): return str(int(x))
    if isinstance(x, float): return str(int(x)) if x.is_integer() else str(x).strip()
    s = str(x).strip().replace(" ", "")
    s = re.sub(r"\.0$", "", s)
    return s if s else None

def expand_ranges(df, p_start, p_end, id_col="id", ini_col="ini", fin_col="fin"):
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

def effective_date_from_list(lst, end_date):
    cand = [d for d in (lst or []) if d <= end_date]
    return max(cand) if cand else None

def first_nonnull(series):
    for v in series:
        if pd.notna(v) and str(v).strip() != "":
            return v
    return np.nan

def parse_sap_report(file_bytes):
    """
    Parser para Ausentismos SAP cuando viene como reporte tabulado con \t.
    Intenta leerlo como Excel, toma filas como texto y extrae:
    - cedula (id)
    - fecha inicio / fin (dd.mm.yyyy)
    """
    raw = pd.read_excel(BytesIO(file_bytes), sheet_name=0, header=None)

    date_re = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
    num_re  = re.compile(r"^\d{6,15}$")

    def parse_row(row):
        s = "".join([str(v) for v in row if not pd.isna(v)])
        if "\t" not in s:
            return None
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

def build_output_excel(dfs: dict) -> bytes:
    """
    dfs: dict(sheet_name -> dataframe)
    retorna bytes del excel
    """
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sh, df in dfs.items():
            df.to_excel(writer, sheet_name=sh[:31], index=False)
    buffer.seek(0)
    return buffer.read()

# ----------------------------
# UI
# ----------------------------
st.title("📌 Consolidado: Ausencias sin soporte")

st.markdown(
    """
Sube los archivos del periodo y el sistema generará un Excel con:
- Ausencias sin soporte (detalle por día)
- Resumen por persona
- Retiros fuera de rango
- Ingresos posteriores
- Inconsistencias
"""
)

col1, col2 = st.columns(2)

with col1:
    f_horas   = st.file_uploader("Rep_Horas_laboradas.xlsx", type=["xlsx"])
    f_ausrep  = st.file_uploader("Rep_aususentismos.xlsx", type=["xlsx"])
    f_retiros = st.file_uploader("Retiros.xlsx", type=["xlsx"])
with col2:
    f_md      = st.file_uploader("Md_activos.xlsx", type=["xlsx"])
    f_func    = st.file_uploader("funciones_marcación.xlsx", type=["xlsx"])
    f_aussap  = st.file_uploader("Ausentismos_SAP (XLS o XLSX)", type=["xls", "xlsx"])

st.divider()

fecha_inicio = st.date_input("Fecha inicio del periodo")
fecha_fin    = st.date_input("Fecha fin del periodo")

run = st.button("🚀 Generar consolidado", type="primary")

if run:
    if not all([f_horas, f_ausrep, f_retiros, f_md, f_func, f_aussap]):
        st.error("Faltan archivos por cargar. Debes cargar los 6.")
        st.stop()

    if fecha_fin < fecha_inicio:
        st.error("La fecha fin no puede ser menor que la fecha inicio.")
        st.stop()

    period_start = fecha_inicio
    period_end = fecha_fin

    with st.spinner("Procesando..."):
        # Read inputs
        horas   = pd.read_excel(BytesIO(f_horas.read()), sheet_name=0)
        ausrep  = pd.read_excel(BytesIO(f_ausrep.read()), sheet_name=0)
        retiros = pd.read_excel(BytesIO(f_retiros.read()), sheet_name=0)
        md      = pd.read_excel(BytesIO(f_md.read()), sheet_name=0)
        func    = pd.read_excel(BytesIO(f_func.read()), sheet_name=0)

        # SAP report parse
        sap_bytes = f_aussap.read()
        aussap2 = parse_sap_report(sap_bytes)

        # Normalize Horas
        horas2 = horas.copy()
        horas2["id"] = horas2["IdentificacionEmpleado"].apply(clean_id)
        horas2["fecha"] = pd.to_datetime(horas2["FechaEntrada"], errors="coerce").dt.date
        marc = horas2[horas2["id"].notna() & horas2["fecha"].notna()][["id", "fecha"]].drop_duplicates()

        # Normalize Ausentismos Reporte
        ausrep2 = ausrep.copy()
        ausrep2["id"]  = ausrep2["Identificacion"].apply(clean_id)
        ausrep2["ini"] = pd.to_datetime(ausrep2["Fecha_Inicio"], errors="coerce").dt.date
        ausrep2["fin"] = pd.to_datetime(ausrep2["Fecha_Final"], errors="coerce").dt.date
        ausrep_days = expand_ranges(ausrep2, period_start, period_end)

        # Normalize Retiros
        retiros2 = retiros.copy()
        retiros2["id"] = retiros2["Número ID"].apply(clean_id)
        retiros2["Desde_dt"] = pd.to_datetime(retiros2["Desde"], errors="coerce").dt.date
        retiros2["FechaRetiro"] = retiros2["Desde_dt"].apply(lambda d: d - timedelta(days=1) if pd.notna(d) else None)

        ret_list = retiros2.groupby("id")["FechaRetiro"].apply(lambda s: sorted(set([d for d in s.dropna()]))).reset_index()
        ret_list["RetiroEfectivo"] = ret_list["FechaRetiro"].apply(lambda lst: effective_date_from_list(lst, period_end))
        ret_list["ListaRetiros"] = ret_list["FechaRetiro"].apply(lambda lst: ", ".join([d.isoformat() for d in lst]) if isinstance(lst, list) else "")

        # Normalize Masterdata
        md2 = md.copy()
        md2["id"] = md2["Número ID"].apply(clean_id)
        md2["funcion"] = md2["Función"].astype(str).str.strip()
        md2["clase_fecha"] = md2["Clase de fecha"].astype(str).str.strip()
        md2["fecha_clase"] = pd.to_datetime(md2["Fecha"], errors="coerce").dt.date
        md2["ingreso"] = np.where(md2["clase_fecha"].str.lower().str.contains("alta"), md2["fecha_clase"], pd.NaT)
        md2["ingreso"] = pd.to_datetime(md2["ingreso"], errors="coerce").dt.date

        # Authorized functions
        auth_funcs = set(func["Función"].dropna().astype(str).str.strip().unique())
        md2["autorizado_TS"] = md2["funcion"].isin(auth_funcs)

        ing_list = md2.groupby("id")["ingreso"].apply(lambda s: sorted(set([d for d in s.dropna()]))).reset_index()
        ing_list["IngresoEfectivo"] = ing_list["ingreso"].apply(lambda lst: effective_date_from_list(lst, period_end))
        ing_list["ListaIngresos"] = ing_list["ingreso"].apply(lambda lst: ", ".join([d.isoformat() for d in lst]) if isinstance(lst, list) else "")

        authorized_ids = set(md2.loc[md2["autorizado_TS"] & md2["id"].notna(), "id"].unique())

        # Expand SAP absences
        aussap_days = expand_ranges(aussap2, period_start, period_end)

        # Universe
        ids_union = pd.Index(pd.concat([
            pd.Series(list(authorized_ids)),
            horas2["id"], ausrep2["id"], aussap2["id"], retiros2["id"]
        ]).dropna().unique())

        dates = pd.date_range(period_start, period_end, freq="D").date
        grid = pd.MultiIndex.from_product([ids_union, dates], names=["id", "fecha"]).to_frame(index=False)

        # Flags
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

        # Sin soporte
        grid["sin_soporte"] = grid["vigente_dia"] & (~grid["tiene_marcacion"]) & (~grid["tiene_aus_rep"]) & (~grid["tiene_aus_sap"])

        # Consideración según regla TS
        grid["considerar_activo_TS"] = (grid["estado_periodo"] == "Activo (MD)") & (grid["autorizado_TS"])
        grid["considerar"] = grid["considerar_activo_TS"] | grid["estado_periodo"].isin([
            "Retirado en el periodo", "Retirado antes del periodo", "Retiro despues del periodo", "Sin masterdata (posible retirado)"
        ])

        # Info master
        info = []

        cols_h = ["id","Codigo_Empleado","Nombres","Apellidos","Empresa","Sucursal","Dependencia","Municipio","Centro_Costos_Marcacion"]
        cols_h = [c for c in cols_h if c in horas2.columns]
        info_h = horas2[cols_h].drop_duplicates("id").copy()
        info_h = info_h.rename(columns={"Codigo_Empleado":"CodigoEmpleado","Nombres":"Nombre","Apellidos":"Apellido"})
        info.append(info_h)

        cols_ar = ["id","Codigo_Empleado","Nombre_Empleado","Apellido_Empleado","Dependencia","Centro_De_Costo","Cargo"]
        cols_ar = [c for c in cols_ar if c in ausrep2.columns]
        info_ar = ausrep2[cols_ar].drop_duplicates("id").copy()
        info_ar = info_ar.rename(columns={"Codigo_Empleado":"CodigoEmpleado","Nombre_Empleado":"Nombre","Apellido_Empleado":"Apellido","Centro_De_Costo":"CentroCosto"})
        info.append(info_ar)

        info_md = md2[["id", "funcion"]].drop_duplicates("id").copy()
        info.append(info_md)

        info_master = pd.concat(info, ignore_index=True, sort=False)
        info_master = info_master.groupby("id").agg(first_nonnull).reset_index()
        info_master = info_master.merge(ret_list[["id", "ListaRetiros"]], on="id", how="left")
        info_master = info_master.merge(ing_list[["id", "ListaIngresos"]], on="id", how="left")

        # Outputs
        def obs(stt):
            return {
                "Activo (MD)":"Activo autorizado TS: sin marcación y sin ausentismo (Reporte + SAP)",
                "Retirado en el periodo":"Retirado: sin marcación y sin ausentismo (Reporte + SAP) hasta fecha retiro",
                "Retiro despues del periodo":"Retiro posterior: sin marcación y sin ausentismo (Reporte + SAP) en el periodo",
                "Sin masterdata (posible retirado)":"Sin masterdata: sin marcación y sin ausentismo (Reporte + SAP) en el periodo"
            }.get(stt, "Sin marcación y sin ausentismo (Reporte + SAP)")

        aus_sin = grid[grid["considerar"] & grid["sin_soporte"]].merge(info_master, on="id", how="left")
        aus_sin["Observacion"] = aus_sin["estado_periodo"].map(obs)

        aus_sin_out = aus_sin[[
            "id","CodigoEmpleado","Nombre","Apellido","Empresa","Sucursal","Dependencia","Centro_Costos_Marcacion",
            "funcion","autorizado_TS","fecha","estado_periodo","IngresoEfectivo","RetiroEfectivo",
            "tiene_marcacion","tiene_aus_rep","tiene_aus_sap","Observacion","ListaIngresos","ListaRetiros"
        ]].sort_values(["estado_periodo","id","fecha"])

        g = grid[grid["considerar"]].merge(info_master, on="id", how="left")
        summary = g.groupby("id").agg(
            CodigoEmpleado=("CodigoEmpleado","first"),
            Nombre=("Nombre","first"),
            Apellido=("Apellido","first"),
            Empresa=("Empresa","first"),
            Dependencia=("Dependencia","first"),
            funcion=("funcion","first"),
            autorizado_TS=("autorizado_TS","first"),
            estado_periodo=("estado_periodo","first"),
            Ingreso=("IngresoEfectivo","first"),
            Retiro=("RetiroEfectivo","first"),
            ListaIngresos=("ListaIngresos","first"),
            ListaRetiros=("ListaRetiros","first"),
            DiasPeriodo=("fecha","nunique"),
            DiasVigente=("vigente_dia","sum"),
            DiasConMarcacion=("tiene_marcacion","sum"),
            DiasAusReporte=("tiene_aus_rep","sum"),
            DiasAusSAP=("tiene_aus_sap","sum"),
            DiasSinSoporte=("sin_soporte","sum"),
        ).reset_index()

        ultima_marc = g[g["tiene_marcacion"]].groupby("id")["fecha"].max().rename("UltimaMarcacion")
        summary = summary.merge(ultima_marc, on="id", how="left").sort_values(["estado_periodo","DiasSinSoporte"], ascending=[True, False])

        retiros_fuera = summary[summary["estado_periodo"]=="Retirado antes del periodo"].copy()
        retiros_fuera["TieneMovEnPeriodo"] = np.where(
            (retiros_fuera["DiasConMarcacion"]>0) | (retiros_fuera["DiasAusReporte"]>0) | (retiros_fuera["DiasAusSAP"]>0),
            "SI","NO"
        )

        ing_post = summary[summary["estado_periodo"]=="Ingreso posterior al periodo"].copy()

        incons = summary[
            ((summary["estado_periodo"]=="Ingreso posterior al periodo") & (summary["DiasConMarcacion"]>0)) |
            ((summary["Ingreso"].notna()) & (summary["Retiro"].notna()) & (summary["Retiro"] < summary["Ingreso"]) & (summary["DiasConMarcacion"]>0))
        ].copy()

        params = pd.DataFrame({
            "Parametro":[
                "Periodo_inicio",
                "Periodo_fin",
                "Regla_retiro",
                "Regla_ingreso",
                "Regla_activos_TS",
                "Funciones_autorizadas",
                "Cantidad_funciones_autorizadas",
                "Ausentismos_SAP_parseados"
            ],
            "Valor":[
                str(period_start),
                str(period_end),
                "Fecha retiro = Desde - 1 día",
                "Ingreso = Fecha (Clase de fecha contiene 'alta')",
                "Activos: SOLO IDs en MasterData con función autorizada (TS)",
                "funciones_marcación.xlsx (col: Función)",
                str(len(auth_funcs)),
                str(len(aussap2))
            ]
        })

        # Build excel
        dfs = {
            "Parametros": params,
            "Ausencias_sin_soporte": aus_sin_out,
            "Resumen_periodo": summary,
            "Retiros_fuera_rango": retiros_fuera,
            "Ingresos_posteriores": ing_post,
            "Inconsistencias": incons,
        }

        excel_bytes = build_output_excel(dfs)

    # ----------------------------
    # Output UI
    # ----------------------------
    st.success("Listo ✅. Abajo tienes el preview y el botón de descarga.")

    t1, t2 = st.tabs(["📄 Ausencias sin soporte (detalle)", "📊 Resumen por persona"])

    with t1:
        st.dataframe(aus_sin_out, use_container_width=True, height=520)

    with t2:
        st.dataframe(summary, use_container_width=True, height=520)

    st.download_button(
        label="⬇️ Descargar Excel consolidado",
        data=excel_bytes,
        file_name=f"Ausencias_sin_soporte_{period_start}_{period_end}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
