import clr
import os
import sys
import pandas as pd
import time
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn, TextColumn

# Inicializa consola
console = Console()

# Cargar DLL
dll_path = r"C:\Users\MX05081363\OneDrive - Coca-Cola FEMSA\KOF\DLLs\Microsoft.AnalysisServices.AdomdClient.dll"
console.rule("[bold yellow]📦 Cargando ADOMD Client DLL...")

if os.path.exists(dll_path):
    clr.AddReference(dll_path)
    console.print(f"[green]✅ DLL cargada: {dll_path}")
else:
    console.print(f"[red]❌ DLL no encontrada:\n{dll_path}")
    sys.exit(1)

# Importar Pyadomd después de cargar la DLL
from pyadomd import Pyadomd

# Conexión
conn_str = "Provider=MSOLAP;Data Source=KOFMXAZJVTA01;Catalog=SSASModeloVentas;Integrated Security=SSPI;"

# Lista de combinaciones Año-Mes
periodos = [(2025,6)]
#zonas = ['BAJIO','CENTRO - PACIFICO','GOLFO','MONARCA','SURESTE','VALLE DE MÉXICO']
zonas = ['VALLE DE MÉXICO']

# Función para ejecutar la consulta
def ejecutar_consulta(anio, mes, zona):
    console.rule(f"[bold green]🔄 Procesando: Zona {zona} - Año {anio} - Mes {mes:02}")

    mdx = f"""
    SELECT 
        NON EMPTY 
        {{
            [Measures].[Ingreso Neto sin IEPS], 
            [Measures].[Venta CFC],
            [Measures].[Venta CF],
            [Measures].[Venta CU]
        }} ON COLUMNS,

        NON EMPTY 
            (
                [Clientes].[Cve Cliente].[Cve Cliente].ALLMEMBERS *
                [Tiempo].[Cve Mes].[Cve Mes].ALLMEMBERS *
                [Año].[Año Acomodo].[Año Acomodo].ALLMEMBERS *
                [Tipo TPV].[Tipo PPV].[Tipo PPV].ALLMEMBERS *
                [Clientes].[Cve Centro].[Cve Centro].ALLMEMBERS
            )
        DIMENSION PROPERTIES 
            MEMBER_CAPTION, 
            MEMBER_UNIQUE_NAME 
        ON ROWS

    FROM 
    (
        SELECT 
        (
            [Tiempo].[Año natural].&[{anio}],
            [Año].[Año Acomodo].&[{anio}],
            [Tiempo].[Cve Mes].&[{mes}],
            [Clientes].[Territorio].&[{zona}],
            {{
                [Clientes].[Grupo RM1].&[HM Tradicional],
                [Clientes].[Grupo RM1].&[OP Tradicional],
                [Clientes].[Grupo RM1].&[HM Moderno],
                [Clientes].[Grupo RM1].&[OP Moderno],
                [Clientes].[Grupo RM1].&[Mayoristas]
            }},
            {{
                [Tipo TPV].[Tipo PPV].&[ZPV],
                [Tipo TPV].[Tipo PPV].&[ZTK],
                [Tipo TPV].[Tipo PPV].&[ZJV]
            }}
        ) ON COLUMNS

        FROM [MODELO VENTAS]
    )

    CELL PROPERTIES 
        VALUE, 
        FORMATTED_VALUE
    """

    start_time = time.time()

    with Pyadomd(conn_str) as conn:
        with conn.cursor().execute(mdx) as cur:
            data = cur.fetchall()
            columns = [col[0] for col in cur.description]

            rows = []
            with Progress(
                SpinnerColumn(),
                "[progress.description]{task.description}",
                BarColumn(),
                TimeElapsedColumn(),
                TextColumn("{task.completed} filas"),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task("[cyan]Cargando resultados...", total=len(data))
                for row in data:
                    rows.append(row)
                    progress.advance(task)

            df = pd.DataFrame(rows, columns=columns)

            df.drop(columns=[col for col in df.columns if 'MEMBER_UNIQUE_NAME' in col], inplace=True)
            df.rename(columns={
                                '[Clientes].[Cve Cliente].[Cve Cliente].[MEMBER_CAPTION]': 'Cliente',
                                '[Tiempo].[Cve Mes].[Cve Mes].[MEMBER_CAPTION]': 'Mes',
                                '[Año].[Año Acomodo].[Año Acomodo].[MEMBER_CAPTION]': 'Año',
                                '[Tipo TPV].[Tipo PPV].[Tipo PPV].[MEMBER_CAPTION]': 'TPV',
                                '[Clientes].[Cve Centro].[Cve Centro].[MEMBER_CAPTION]': 'Zona Ventas',
                                '[Measures].[Ingreso Neto sin IEPS]': 'Ingreso Neto',
                                '[Measures].[Venta CFC]': 'CFC',
                                '[Measures].[Venta CF]': 'CF',
                                '[Measures].[Venta CU]': 'CU'
                                }, inplace=True)

            # Guardar archivo
            file_name = f"volumen_{zona}_{anio}_{mes:02}.parquet"
            df.to_parquet(f"parquets/{file_name}", index=False)

            elapsed_time = time.time() - start_time
            formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

            console.print(f"[green]✅ Datos guardados como [bold]{file_name}[/bold]")
            console.print(f"🕒 Tiempo de ejecución: [bold]{formatted_time}[/bold]")
            console.print(f"📊 Total de filas: [bold cyan]{len(df):,}[/bold cyan]")

            console.print("[bold yellow]🔢 Suma total por medida:")
            # Calcular totales numéricos y formatearlos
            totales = df.sum(numeric_only=True)

            # Mostrar totales con formato legible
            for col, total in totales.items():
                console.print(f"[bold white]{col}[/bold white]: [bold green]{total:,.2f}[/bold green]")

            print(df.head())     



# Ejecutar para cada periodo
for zona in zonas:
    for anio, mes in periodos:
        try:
            ejecutar_consulta(anio, mes, zona)
        except Exception as e:
            console.print(f"[red]❌ Error en {zona}-{anio}-{mes:02}: {e}")
