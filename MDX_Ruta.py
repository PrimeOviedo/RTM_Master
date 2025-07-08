import clr
import os
import sys
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn, TextColumn
import pandas as pd

# Inicializa consola
console = Console()

# Cargar la DLL del cliente ADOMD
dll_path = r"C:\Users\MX05081363\OneDrive - Coca-Cola FEMSA\KOF\DLLs\Microsoft.AnalysisServices.AdomdClient.dll"
console.rule("[bold yellow]📦 Verificando y cargando ADOMD Client DLL...")

if os.path.exists(dll_path):
    clr.AddReference(dll_path)
    console.print(f"[green]✅ DLL cargada correctamente: {dll_path}")
else:
    console.print(f"[red]❌ No se encontró la DLL en la ruta:\n{dll_path}")
    sys.exit(1)

# Importar Pyadomd después de cargar la DLL
from pyadomd import Pyadomd

# Cadena de conexión
conn_str = "Provider=MSOLAP;Data Source=KOFMXAZJVTA01;Catalog=SSASModeloVentas;Integrated Security=SSPI;"

# Consulta MDX
mdx = """
SELECT 
    NON EMPTY 
    {
        [Measures].[Ingreso Neto sin IEPS], 
        [Measures].[Venta CFC],
        [Measures].[Venta CF],
        [Measures].[Venta CU]
    } ON COLUMNS,

    NON EMPTY 
        (
            --[Tipo TPV].[Ruta Preventa].[Ruta Preventa].ALLMEMBERS *
            [Estructura Comercial].[Ruta ZPV].[Ruta ZPV].ALLMEMBERS *
            [Tiempo].[Mes].[Mes].ALLMEMBERS *
            [Tiempo].[Cve Mes].[Cve Mes].ALLMEMBERS *
            [Año].[Año Acomodo].[Año Acomodo].ALLMEMBERS *
            [Tipo TPV].[Tipo PPV].[Tipo PPV].ALLMEMBERS
        )
    DIMENSION PROPERTIES 
        MEMBER_CAPTION, 
        MEMBER_UNIQUE_NAME 
    ON ROWS

FROM 
(
    SELECT 
    (
        [Tiempo].[Año natural].&[2024],
        [Tiempo].[Cve Mes].&[11],
        {[Tipo TPV].[Tipo PPV].&[ZPV], [Tipo TPV].[Tipo PPV].&[ZTK], [Tipo TPV].[Tipo PPV].&[ZJV]},
        [Año].[Año Acomodo].&[2024]
    ) ON COLUMNS

    FROM [MODELO VENTAS]
)

CELL PROPERTIES 
    VALUE, 
    FORMATTED_VALUE
"""

# Ejecutar consulta y exportar
console.rule("[bold green]🚀 Conectando a Cubo OLAP...")

with Pyadomd(conn_str) as conn:
    with conn.cursor().execute(mdx) as cur:
        data = cur.fetchall()
        columns = [col[0] for col in cur.description]

        console.rule("[bold cyan]📥 Descargando filas...")

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
            task = progress.add_task("[green]Procesando resultados...", total=len(data))
            for row in data:
                rows.append(row)
                progress.advance(task)

        df = pd.DataFrame(rows, columns=columns)
        df.to_csv("consulta_mdx.csv", index=False)

        console.rule("[bold magenta]✅ Consulta guardada como 'consulta_mdx.csv'")

        # Imprimir línea por línea cada fila
        console.rule("[bold cyan]📄 Imprimiendo resultados línea por línea...")
        for i, row in enumerate(rows, 1):
            console.print(f"[{i}] {dict(zip(columns, row))}")
