import pandas as pd
from sqlalchemy import create_engine
import urllib
from rich.console import Console
import time

# Inicia contador de tiempo
start_time = time.time()
console = Console()

# 🔐 Parámetros de conexión
server = 'KOFMXAZMND01'
database = 'DATAKOF_MX_USERS'

# 🧩 Cadena de conexión para autenticación por Windows
params = urllib.parse.quote_plus(
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

# 🎯 Crear conexión con SQLAlchemy
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 📥 Consulta SQL
query = """
SELECT DISTINCT
        c.[Num_Cliente] AS ID_SAP,
        c.[NombreCliente] AS NOMBRE,
        c.[Cadena] AS CADENA,
        c.[Coord_X] AS LONGITUD,
        c.[DescGEC] AS GEC_SAP,
        c.[Coord_Y] AS LATITUD,
        c.[ModalidadVentas] AS MODALIDAD,
        c.[Descripción Estado] AS ZONA,
        c.[Zona Ventas   U O ] AS UO,
        c2.[_NIVEL_ESTRATEGICO_1] AS CANAL,
        c2.[_NIVEL_ESTRATEGICO_2] AS GRUPO_RM1,
        c2.[_NIVEL_ESTRATEGICO_3] AS NIVEL_ESTRATEGICO,
        c2.[CANAL_KOF] AS CANAL_KOF_KEY,
        c2.[CANAL_KOF_DESC] AS CANAL_KOF_DESC,
        c2.[GRUPO_CANAL_DESC] AS GRUPO_CANAL_SAP,
        ZPV.[MétodoVenta] AS MTDO_VTA_ZPV,
        ZPV.[Ruta] AS RUTA_ZPV,
        ZPV.[Frecuencia de Visita] AS FV_ZPV,
        ZPV.[Ritmo] AS RITMO_ZPV,
        ZTK.[MétodoVenta] AS MTDO_VTA_ZTK,
        ZTK.[Ruta] AS RUTA_ZTK,
        ZTK.[Frecuencia de Visita] AS FV_ZTK,
        ZTK.[Ritmo] AS RITMO_ZTK,
        ZJV.[MétodoVenta] AS MTDO_VTA_ZJV,
        ZJV.[Ruta] AS RUTA_ZJV,
        ZJV.[Frecuencia de Visita] AS FV_ZJV,
        ZJV.[Ritmo] AS RITMO_ZJV,
        c.[Centro Suministro] AS CENTRO_SUMINISTRO,
        c.[ZonaTransporte] AS RUTA_REPARTO
FROM 
    [DATAKOF_MX_USERS].[kof].[MEX_KOF_SAP_CLIENTE_FULL] c
    LEFT JOIN [DATAKOF_MX_USERS].[kof].[MEX_KOF_SAP_CLIENTE_FULL] ZPV
        ON c.[Num_Cliente] = ZPV.[Num_Cliente]
        AND ZPV.[Tipo Plan de Visita] = 'ZPV'
        AND ZPV.[Estatus del Cliente] NOT IN ('B','L')
    LEFT JOIN [DATAKOF_MX_USERS].[kof].[MEX_KOF_SAP_CLIENTE_FULL] ZJV
        ON c.[Num_Cliente] = ZJV.[Num_Cliente]
        AND ZJV.[Tipo Plan de Visita] = 'ZJV'
        AND ZJV.[Estatus del Cliente] NOT IN ('B','L')
    LEFT JOIN [DATAKOF_MX_USERS].[kof].[MEX_KOF_SAP_CLIENTE_FULL] ZTK
        ON c.[Num_Cliente] = ZTK.[Num_Cliente]
        AND ZTK.[Tipo Plan de Visita] = 'ZTK'
        AND ZTK.[Estatus del Cliente] NOT IN ('B','L')
    LEFT JOIN [kof].[MEX_KOF_SAP_DIM_CLIENTE] c2
        ON c.[Num_Cliente] = c2.[CLIENTE]
WHERE 
    c.[Tipo Plan de Visita] IN ('ZPV', 'ZJV', 'ZTK')
    AND c.[Estatus del Cliente] NOT IN ('B')
ORDER BY c.[Num_Cliente]
"""

# 🧪 Ejecutar la consulta
print("⏳ Ejecutando consulta...")
df_clientes = pd.read_sql(query, engine)

# Línea decorativa de cierre
console.rule("[bold magenta]✅ Transformación guardada como 'base_clientes_final.csv'")

# Calcular tiempo de ejecución
elapsed_time = time.time() - start_time
formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

# Mostrar resumen
console.rule("[bold blue]📊 Resumen de la ejecución")
console.print(f"🕒 Tiempo de ejecución total: [bold green]{formatted_time}[/bold green]")
console.print(f"📈 Filas totales: [bold cyan]{len(df_clientes):,}[/bold cyan]")

# Mostrar suma de columnas numéricas
console.print("[bold yellow]🔢 Suma total por medida:")
totales = df_clientes.sum(numeric_only=True)

# Imprimir cada columna numérica con su total formateado
for col, total in totales.items():
    console.print(f"[bold white]{col}[/bold white]: [bold green]{total:,.2f}[/bold green]")
