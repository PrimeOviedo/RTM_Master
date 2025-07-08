import clr
import os
import sys
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn, TextColumn
import pandas as pd
import time

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
            [Clientes].[Cve Cliente].[Cve Cliente].ALLMEMBERS *
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
        [Tiempo].[Año natural].&[2025],
        [Año].[Año Acomodo].&[2025],
        [Tiempo].[Cve Mes].&[5],
		{[Clientes].[Grupo RM1].&[HM Tradicional],[Clientes].[Grupo RM1].&[OP Tradicional],[Clientes].[Grupo RM1].&[HM Moderno],[Clientes].[Grupo RM1].&[OP Moderno],[Clientes].[Grupo RM1].&[Mayoristas]},
        {[Tipo TPV].[Tipo PPV].&[ZPV],[Tipo TPV].[Tipo PPV].&[ZTK],[Tipo TPV].[Tipo PPV].&[ZJV]}
    ) ON COLUMNS

    FROM [MODELO VENTAS]
)

CELL PROPERTIES 
    VALUE, 
    FORMATTED_VALUE
"""

# Ejecutar consulta y exportar
console.rule("[bold green]🚀 Conectando a Cubo OLAP...")

start_time = time.time()


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

        df_vol = pd.DataFrame(rows, columns=columns)
        
        #df_vol.to_csv("consulta_cliente_mdx.csv", index=False)
        df_vol.to_parquet("consulta_cliente_mdx.parquet", index=False)

        console.rule("[bold magenta]✅ Consulta guardada como 'consulta_cliente_mdx.csv'")

        # Calcula tiempo de ejecución
        elapsed_time = time.time() - start_time
        formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

        # Mostrar resumen
        console.rule("[bold blue]📊 Resumen de la ejecución")

        console.print(f"🕒 Tiempo de ejecución total: [bold green]{formatted_time}[/bold green]")
        console.print(f"📈 Filas totales: [bold cyan]{len(df_vol)}[/bold cyan]")

        # Mostrar suma de medidas numéricas
        console.print("[bold yellow]🔢 Suma total por medida:")
        # Calcular totales numéricos y formatearlos
        totales = df_vol.sum(numeric_only=True)

        # Mostrar totales con formato legible
        for col, total in totales.items():
            console.print(f"[bold white]{col}[/bold white]: [bold green]{total:,.2f}[/bold green]")


        # Imprimir línea por línea cada fila
        #console.rule("[bold cyan]📄 Imprimiendo resultados línea por línea...")
        #for i, row in enumerate(rows, 1):
        #    console.print(f"[{i}] {dict(zip(columns, row))}")
