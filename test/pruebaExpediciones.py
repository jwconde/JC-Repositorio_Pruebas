from sct_tools import *
import json
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime,timezone

csv_separator = ';'
file_encoding = 'latin'
path ='C:/Users/dbustosm/everis/Control Tower - DEV - General/FILES_DEFINITION_SCHEMA/'
file_name = 'expediciones-schema.json'

nombre_fichero ='EXPEDICIONES.txt'

f = open(nombre_fichero,'r')
file_content = StringIO(f.read())
f.close()

file = path + file_name

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

string_to_dt_conv = lambda x: pd.to_datetime(x, utc=True) if x != '00000000' else pd.to_datetime('1990-01-01 00:00:00', format='%Y-%m-%d %H:%M:%S', utc=True)
float_to_int_conv = lambda x: int(x[0:x.find('.')]) if x else None
string_to_time_conv = lambda x: pd.to_datetime(x, format='%H%M%S', utc=True) if x != '000000' else pd.to_datetime('1990-01-01 00:00:00', utc = True)

df = pd.read_csv(file_content,
                 sep = csv_separator,
                 header = None,
                 names = header_columns,
                 dtype = column_types,
                 converters = {'Cantidad pedida por el cliente en UMV (acumulado)': float_to_int_conv,
                               'Cantidad confirmada': float_to_int_conv,
                               'Cantidad acumulada recibida del cliente': float_to_int_conv,
                               'Cantidad pendiente de servir (en entrega sin contabilizar)': float_to_int_conv,
                               'Cantidad pendiente de servir (en entrega)': float_to_int_conv,
                               'Fecha de reparto': string_to_dt_conv,
                               'Fecha de la orden de entrega': string_to_dt_conv
                               },
                 encoding = file_encoding
                 )
print(df.dtypes)

# Put Object in Cleansed File
# Get file generation datetime
json_generate_date = datetime.utcnow()
json_str_generate_date = json_generate_date.strftime(json_date_format)
# Initialize file structure
json_result = {
    'Fecha': json_str_generate_date,
    'EXPEDICIONES-SAP': [],
}
# Get output columns names
new_names = get_outputfile_header(schema)
df.columns = new_names

# Reorder dataframe
ordered_output_header = get_outputfile_ordered_header(schema)
df = df.reindex(columns=ordered_output_header)

def get_outputfile_types(file_schema:dict)->dict:
    types = {}
    for column in file_schema.items():
        key = column[1]['output_fieldname']
        types[key]={}
        types[key]['type']= column[1]['output_type']
    return types

#Transform null columns:
specific_types = get_outputfile_types(schema)
for column in specific_types.items():
    if column[1]['type'] == 'str' or column[1]['type'] == 'date':
        df.fillna('', inplace=True)

# Get dataframe data in json format
result = df.to_json(orient='index')
result = json.loads(result)
#print(result)

# Fill file expediciones
for i in result.items():
    json_result['EXPEDICIONES-SAP'].append(i[1])

# Dumps the result in JSON format
json_result = json.dumps(json_result)
data = BytesIO(json_result.encode('utf-8'))
f=open('json_test.json', 'w')
f.write(json_result)
f.close()