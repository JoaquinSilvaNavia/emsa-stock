"""
emsa_sync_stock.py
EMSA — Sync Stock de Inventario → Google Sheets
Vista SQL: INFORMAT_Vista_Stock_Actual_por_Bodegas_EMSA

Lógica:
  stock_actual:    snapshot más reciente (sobreescribe)
  stock_historico: acumulación diaria (agrega sin borrar)
  meta:            timestamp

USO:
  py emsa_sync_stock.py --once
  py emsa_sync_stock.py --loop  (cada 24 horas)

DEPENDENCIAS:
  py -m pip install pyodbc gspread google-auth pandas
"""

import sys, time, pyodbc, gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime

SQL_SERVER   = "10.182.120.5"
SQL_DATABASE = "EMB_METROP"
SQL_USER     = "informat"
SQL_PASSWORD = "centauro"

SHEET_ID  = "1EmlOHAqIQzbcvDsqLcsCaFGYyJ0Pifmsjmv9j13z8Qo"
CRED_FILE = "credenciales_google.json"

QUERY = """
SELECT
    [Cod.Bodega],[Nombre Bodega],
    [Cod.Producto],[Nombre Producto],[Uni.Med.],
    [Stock Actual],[Valorizado_FI],
    [Nivel 1],[Nivel 2],[Nivel 3],[Nivel 4],[Nivel 5]
FROM [INFORMAT_Vista_Stock_Actual_por_Bodegas_EMSA]
ORDER BY [Nivel 1],[Nivel 2],[Nombre Bodega],[Nombre Producto]
"""

def get_sql_conn():
    return pyodbc.connect(
        "DRIVER={SQL Server Native Client 10.0};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};APP=EMSA_Stock_Sync;"
    )

def get_sheet(tab):
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    try:    return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
            return sh.add_worksheet(title=tab, rows=5000, cols=15)

def clasifica(n1):
    n = str(n1).strip().upper()
    if 'TERMINADO' in n or 'P.TERM' in n: return 'P.Terminados'
    if 'INSUMO' in n:                      return 'Insumos'
    if 'MATERIA' in n or 'M.PRIMA' in n:  return 'Materias Primas'
    if 'ENVASE' in n:                      return 'Envases'
    if 'REPUESTO' in n:                    return 'Repuestos'
    return 'Otros'

def sync():
    fecha = datetime.now().strftime("%Y-%m-%d")
    print(f"[{datetime.now():%H:%M:%S}] Iniciando sync stock — {fecha}...")

    try:
        conn = get_sql_conn()
        df   = pd.read_sql(QUERY, conn)
        conn.close()
        print(f"  SQL OK — {len(df)} filas")
    except Exception as e:
        print(f"  ERROR SQL: {e}"); return False

    # Limpiar
    df.columns = [c.strip() for c in df.columns]
    df = df[[c for c in df.columns if not c.startswith(('&','!','?'))]].copy()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip()

    df['CLASIFICACION']  = df['Nivel 1'].apply(clasifica)
    df['FECHA']          = fecha
    df['Stock Actual']   = pd.to_numeric(df['Stock Actual'],   errors='coerce').fillna(0)
    df['Valorizado_FI']  = pd.to_numeric(df['Valorizado_FI'],  errors='coerce').fillna(0)

    print(f"  Clasificaciones: {df['CLASIFICACION'].value_counts().to_dict()}")
    print(f"  Valor total: ${df['Valorizado_FI'].sum():,.0f}")

    cols = ['FECHA','CLASIFICACION','Nivel 1','Nivel 2','Nivel 3',
            'Cod.Bodega','Nombre Bodega','Cod.Producto','Nombre Producto',
            'Uni.Med.','Stock Actual','Valorizado_FI']
    df_out = df[cols].fillna('').astype(str)

    # stock_actual — sobreescribe
    try:
        ws = get_sheet("stock_actual")
        ws.clear()
        ws.update([df_out.columns.tolist()] + df_out.values.tolist())
        print(f"  Sheet 'stock_actual' OK — {len(df_out)} filas")
    except Exception as e:
        print(f"  ERROR stock_actual: {e}"); return False

    # stock_historico — acumula (solo 1 registro por día)
    try:
        ws_h = get_sheet("stock_historico")
        existing = ws_h.get_all_values()
        fechas   = [r[0] for r in existing[1:]] if len(existing) > 1 else []
        if fecha in fechas:
            print(f"  stock_historico — ya existe {fecha}, omitiendo")
        elif not existing:
            ws_h.update([df_out.columns.tolist()] + df_out.values.tolist())
            print(f"  stock_historico OK — primera carga")
        else:
            ws_h.append_rows(df_out.values.tolist())
            print(f"  stock_historico OK — {len(df_out)} filas agregadas")
    except Exception as e:
        print(f"  WARNING stock_historico: {e}")

    # meta
    try:
        ws_m = get_sheet("meta")
        ws_m.update(values=[[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]], range_name="A1")
        print(f"  Meta OK")
    except Exception as e:
        print(f"  WARNING meta: {e}")

    print(f"  Sync completado correctamente")
    return True

if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "--once"
    if   modo == "--once": sync()
    elif modo == "--loop":
        print("Loop cada 24h. Ctrl+C para detener.")
        while True:
            sync()
            time.sleep(86400)
    else:
        print("Uso: py emsa_sync_stock.py --once | --loop")
