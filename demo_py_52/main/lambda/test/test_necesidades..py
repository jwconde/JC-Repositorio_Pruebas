## librerias aws
from sct_tools import *
import json
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timezone


# formato para leer el archivo
csv_separator = ';'
file_encoding = 'utf-8'
path = 'C:/PROYECTO_SEAT_AWS/JIRA_SCT_48/main/properties/'
file_name = 'necesidades-schema.json'

# archivo de salida
nombre_fichero = 'MATNECOB.txt'

# vamos a leer el fichero
f = open(nombre_fichero, 'r')
file_content = StringIO(f.read())
f.close()
#revisar
#revisar#revisar#revisar#revisar
#revisar
#revisar

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


# Create converters for correct column parsing
#float_to_int_conv = lambda x: int(x[0:x.find('.')]) if x else None

try:
    # Check csv separator
    df = pd.read_csv(file_content,
                     sep=csv_separator,
                     header=None,
                     nrows=1,
                     encoding=file_encoding)

    if df.shape[1] != len(header_columns):
        raise IOError('Incorrect csv separator')
    else:
        file_content.seek(0)

    # Create converters for correct column parsing
    #float_to_int_conv = lambda x: int(x[0:x.find('.')]) if x else None

    # Create converters for correct column parsing
    df = pd.read_csv(file_content,
                     sep=csv_separator,
                     header=None,
                     names=header_columns,
                     dtype=column_types,
                     encoding=file_encoding
    #                 converters={
    #                     'Cantidad Planificada': float_to_int_conv
    #                 }
                     )
except Exception as e:
    logger.error(e)
    raise e
print(df.dtypes)

# Put Object in Cleansed File
# Get file generation datetime
json_generate_date = datetime.utcnow()
json_str_generate_date = json_generate_date.strftime(json_date_format)

# Initialize file structure
json_result = {
    'Fecha': json_str_generate_date,
    'NECESIDADES': [],

}


# Get output columns names-----rev------ok
new_names = get_outputfile_header(schema)
df.columns = new_names

#new_names = get_outputfile_header(file_schema)
#df.columns = new_names

# Reorder dataframe---rev ----OK
ordered_output_header = get_outputfile_ordered_header(schema)
df = df.reindex(columns=ordered_output_header)

#ordered_output_header = get_outputfile_ordered_header(file_schema)
#df = df.reindex(columns=ordered_output_header)

#print (df)
# Convert cantidad a int
df = df.astype({'cantidad': int})

# Transform datetime columns into formatted strings with appropriate tz:
#specific_formats = get_outputfile_formats(file_schema)

# Transform null columns:
#def get_outputfile_types(file_schema:dict)->dict:
#    types = {}
#    for column in file_schema.items():
#        key = column[1]['output_fieldname']
#        types[key]={}
#        types[key]['type']= column[1]['output_type']
#    return types

#Transform null columns:
#specific_types = get_outputfile_types(schema)
#for column in specific_types.items():
#    if column[1]['type'] == 'str' or column[1]['type'] == 'date':
#        df.fillna('', inplace=True)


# Get dataframe data in json format
result = df.to_json(orient='index')
result = json.loads(result)

# Fill file STOCK-UBICACIONES
for i in result.items():
    json_result['NECESIDADES'].append(i[1])

# Dumps the result in JSON format local
json_result = json.dumps(json_result)
data = BytesIO(json_result.encode('utf-8'))

f=open('json_test.json','w')
f.write(json_result)
f.close()