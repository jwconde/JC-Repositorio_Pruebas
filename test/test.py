## librerias a usar
from sct_tools import *
import json
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timezone

# formato para leer el archivo
csv_separator = ';'
file_encoding = 'latin'
path = 'C:/PROYECTO_SEAT_AWS/JIRA_SCT_48/main/properties/'
file_name = 'Necesidades-BESI-schema.json'

# archivo de salida
nombre_fichero = "ESTRATEGIA_ALMACEN.txt"

# vamos a leer el fichero
f = open(nombre_fichero, 'r')
file_content = StringIO(f.read())
f.close()

# recogemos la variable file_name
file = path + file_name

# cargamos el fichero json
f = open(file, 'rb')
schema = json.load(f)
f.close()

print(get_inputfile_header(schema))
print('_______')
print(get_inputfile_column_types(schema))
print('_______')
print(get_outputfile_header(schema))
print('_______')
print(get_outputfile_ordered_header(schema))

header_columns = get_inputfile_header(schema)
column_types = get_inputfile_column_types(schema)

df = pd.read_csv(file_content,
                 sep=csv_separator,
                 header=None,
                 names=header_columns,
                 dtype=column_types
                 )

df['ID'] = None
print(df.dtypes)

# Put Object in Cleansed File
# Get file generation datetime
json_generate_date = datetime.utcnow()
json_str_generate_date = json_generate_date.strftime(json_date_format)

# Initialize file structure
json_result = {
    'Fecha': json_str_generate_date,
    'estrategia': [],

}

# Get output columns names
new_names = get_outputfile_header(schema)
df.columns = new_names

# Reorder dataframe
ordered_output_header = get_outputfile_ordered_header(schema)
df = df.reindex(columns=ordered_output_header)

# Get dataframe data in json format
result = df.to_json(orient='index')
result = json.loads(result)

# Fill file STOCK-UBICACIONES
for i in result.items():
    json_result['estrategia'].append(i[1])

# Dumps the result in JSON format local
json_result = json.dumps(json_result)
f=open('json_test.json','w')
f.write(json_result)
f.close()