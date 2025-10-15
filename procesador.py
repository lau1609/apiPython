import pandas as pd
import numpy as np

MESES_MAPEO = {
    'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 
    'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8, 
    'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
}
def procesar_excel_final(mapeo_paths):
    """
    Procesar el archivo BD-SICHITUR.xlsx, reemplazando IDs, creando la columna Fecha,
    y preservando los guiones.
    
    Args:
        mapeo_paths (dict): Diccionario con las rutas de los tres archivos de mapeo.
    """
    excel_principal_path = 'BD-SICHITUR.xlsx'
    NA_VALUES = ['#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '#DIV/0!', '#NULL!', 'N/A', 'NA', 'NULL', 'nan']
    try:
        print(f"Cargando archivo principal: {excel_principal_path}")
        df_principal = pd.read_excel(excel_principal_path, na_values=NA_VALUES)
        print("Cargando archivos de mapeo...")
        df_regiones = pd.read_excel(mapeo_paths['region'])
        df_municipios = pd.read_excel(mapeo_paths['municipio'])
        df_localidades = pd.read_excel(mapeo_paths['localidad'])
        df_principal = pd.merge(df_principal, df_regiones, on='Región', how='left')
        df_principal = pd.merge(df_principal, df_municipios, on='Municipio', how='left')
        df_principal = pd.merge(df_principal, df_localidades, on='Localidad', how='left')
        df_principal = df_principal.drop(['Región', 'Municipio', 'Localidad'], axis=1)
        print("Creando columna de fecha (dd/mm/aaaa)...")
        df_principal['Mes_Num'] = df_principal['Mes'].astype(str).str.strip().replace(MESES_MAPEO)
        df_principal['Año'] = pd.to_numeric(df_principal['Año'], errors='coerce', downcast='integer')
        fecha_str = df_principal['Año'].astype(str) + '-' + df_principal['Mes_Num'].astype(str) + '-01'
        df_principal['Fecha'] = pd.to_datetime(fecha_str, format='%Y-%m-%d', errors='coerce').dt.strftime('%d/%m/%Y')
        df_principal = df_principal.drop(['Año', 'Mes', 'Mes_Num'], axis=1)
        print("Eliminando columnas GPD...")
        df_principal = df_principal.drop(['GPD-12', 'GPD-345'], axis=1)
        columnas_ordenadas_ids = ['Region_ID', 'Municipio_ID', 'Localidad_ID', 'Fecha']
        columnas_remanentes = [col for col in df_principal.columns if col not in columnas_ordenadas_ids]
        df_principal = df_principal[columnas_ordenadas_ids + columnas_remanentes]
        print("Archivo procesado con éxito.")
        return df_principal

    except FileNotFoundError:
        print(f"ERROR: El archivo '{excel_principal_path}' o alguno de los mapeos no fue encontrado.")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return None

if __name__ == "__main__":
    
    mapeo_paths = {
        'region': 'mapeo_regiones.xlsx',
        'municipio': 'mapeo_municipios.xlsx',
        'localidad': 'mapeo_localidades.xlsx'
    }
    output_csv_path = 'datos_procesados_final.csv'
    df_procesado = procesar_excel_final(mapeo_paths)
    if df_procesado is not None:
        df_procesado.to_csv(output_csv_path, index=False)
        print(f"\nEl archivo CSV final se guardó en: {output_csv_path}")
        print("Proceso completado")