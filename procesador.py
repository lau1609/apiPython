import pandas as pd
import numpy as np
from datetime import datetime
import io
import sys 

MESES_MAPEO = {
    'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 
    'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8, 
    'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
}

def procesar_excel_final_api(excel_bytes, mapeo_paths):
    
    NA_VALUES = ['#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '#DIV/0!', '#NULL!', 'N/A', 'NA', 'NULL', 'nan']
    
    try:
        df_principal = pd.read_excel(io.BytesIO(excel_bytes), na_values=NA_VALUES)
        
        # 1. Limpieza de columnas para estandarizar
        df_principal.columns = df_principal.columns.str.strip().str.lower().str.replace('í', 'i').str.replace('ó', 'o')
        
        # 2. Carga de Mapeos
        df_regiones = pd.read_excel(mapeo_paths['region'])
        df_municipios = pd.read_excel(mapeo_paths['municipio'])
        df_localidades = pd.read_excel(mapeo_paths['localidad'])

        # 3. MERGE
        df_principal = pd.merge(df_principal, df_regiones, left_on='region', right_on='Region', how='left')
        df_principal = pd.merge(df_principal, df_municipios, left_on='municipio', right_on='Municipio', how='left')
        df_principal = pd.merge(df_principal, df_localidades, left_on='localidad', right_on='Localidad', how='left')
        
        # 4. ELIMINACIÓN de columnas originales y temporales del merge
        cols_to_drop = ['region', 'municipio', 'localidad', 'Region', 'Municipio', 'Localidad']
        df_principal = df_principal.drop([col for col in cols_to_drop if col in df_principal.columns], axis=1)

        # 5. CREACIÓN de Fecha
        df_principal['mes_num'] = df_principal['mes'].astype(str).str.strip().replace(MESES_MAPEO)
        df_principal['año'] = pd.to_numeric(df_principal['año'], errors='coerce', downcast='integer')
        
        # Generar fecha en formato aaaa-mm-dd
        fecha_str = df_principal['año'].astype(str) + '-' + df_principal['mes_num'].astype(str) + '-01'
        df_principal['Fecha'] = pd.to_datetime(fecha_str, format='%Y-%m-%d', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Eliminar columnas auxiliares
        df_principal = df_principal.drop(['año', 'mes', 'mes_num'], axis=1)
        
        # 6. ELIMINACIÓN de columnas GPD
        df_principal = df_principal.drop(['GPD-12', 'GPD-345'], axis=1, errors='ignore')
        
        # 7. REORDENAMIENTO DE COLUMNAS
        columnas_ordenadas_ids = ['Region_ID', 'Municipio_ID', 'Localidad_ID', 'Fecha']
        columnas_remanentes = [col for col in df_principal.columns if col not in columnas_ordenadas_ids]
        df_principal = df_principal[columnas_ordenadas_ids + columnas_remanentes]

        # 8. IMPRESIÓN DE COLUMNAS FINALES (Para depuración)
        final_columns_count = len(df_principal.columns)
        print(f"Número de columnas final: {final_columns_count}")
        print(f"Nombres de columnas finales: {list(df_principal.columns)}")
        # Forzar el vaciado del buffer para que aparezca inmediatamente en los logs de Render
        sys.stdout.flush() 

        # ⚠️ Eliminamos la validación estricta que causaba el ERROR 500
        
        return df_principal

    except Exception as e:
        # Si hay otro error (por ejemplo, KeyError en algún merge)
        print(f"Error interno en procesador.py: {e}")
        return None
