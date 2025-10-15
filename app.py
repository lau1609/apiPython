from flask import Flask, request, jsonify
import pandas as pd
from procesador import procesar_excel_final_api 
import io

app = Flask(__name__)

MAPEO_PATHS = {
    'region': 'mapeo_regiones.xlsx',
    'municipio': 'mapeo_municipios.xlsx',
    'localidad': 'mapeo_localidades.xlsx'
}

@app.route('/process-excel', methods=['POST'])
def process_data():
    """
    Recibe el archivo Excel, lo procesa y devuelve el CSV como texto.
    """
    
    if 'excel_file' not in request.files:
        return jsonify({"success": False, "message": "Falta el archivo 'excel_file' en la solicitud."}), 400

    excel_file = request.files['excel_file']

    try:
        excel_bytes = excel_file.read()
        
        df_procesado = procesar_excel_final_api(excel_bytes, MAPEO_PATHS)

        if df_procesado is None:
             return jsonify({"success": False, "message": "El procesamiento interno de datos falló."}), 500

        #csv_output = df_procesado.to_csv(index=False, sep=',', encoding='utf-8')

        csv_output = df_procesado.to_csv(index=False, header=False, sep=',', encoding='utf-8')
        return jsonify({"success": True, "csv_data": csv_output, "message": "Procesamiento completado."}), 200

    except Exception as e:
        return jsonify({"success": False, "message": f"Error del servidor: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=os.environ.get("PORT", 5000))
