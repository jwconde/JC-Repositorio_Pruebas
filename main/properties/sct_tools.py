import logging
import pandas as pd

date_format = '%Y%m%d'
datetime_format = '%Y-%m-%d %H:%M:%S.%f %Z'
utc_datetime_format = '%Y%m%dT%H%M%SZ'
json_date_format = '%Y-%m-%d %H:%M:%S'

parquet_compression = None


# Initiate logger
def initiate_logger(loggin_level):
    logger = logging.getLogger('')
    logger.setLevel(loggin_level)

    return logger


# Return a list with the header columns name
def get_inputfile_header(file_schema: dict) -> list:
    header = []
    for column in file_schema.items():
        if not ('in_origin' in column[1].keys() and column[1]['in_origin'] == 0):
            header.append((column[1]['order'], column[0]))
            header.sort(key = lambda x: x[0])
    return [column[1] for column in header]

 

def test_get_inputfile_header():
    test_dict = {
        'Columna 1': {'order': 3},
        'Columna 2': {'order': 1},
        'Columna 3': {'order': 2}
    }
    assert get_inputfile_header(test_dict) == ['Columna 2', 'Columna 3', 'Columna 1']

 


# Return a dict with the type of file columns
def get_inputfile_column_types (file_schema: dict) -> dict:
    types_dict = {}
    for column in file_schema.items():
        if not ('in_origin' in column[1].keys() and column[1]['in_origin'] == 0):
            types_dict[column[0]] = column[1]['type']

    return types_dict

 

def test_get_inputfile_column_types():
    test_dict = {
        'Columna 1': {'order': 1, 'type': str},
        'Columna 2': {'order': 2, 'type': int},
        'Columna 3': {'order': 3, 'type': complex}
    }
    assert get_inputfile_column_types(test_dict) == {'Columna 1': str, 'Columna 2': int, 'Columna 3': complex}

 


def get_inputfile_converters(file_schema: dict) -> dict:
    formats = {}
    for column in file_schema.items():
        if column[1]['output_type'] == 'date':
            if column[1]['input_datetime_zone'] == 'UTC':
                s = eval('lambda x: pd.to_datetime(x, format = column[1][\'input_format\'], utc = True)')
                formats[column[0]] = s
    return formats


# Return a list with the output file header columns names
def get_outputfile_header(file_schema: dict) -> list:
    output_header = []
    for column in file_schema.items():
        if not ('skip_output' in column[1].keys() and column[1]['skip_output'] == 1):
            output_header.append((column[1]['order'], column[1]['output_fieldname']))
            output_header.sort(key = lambda x: x[0])
    return [column[1] for column in output_header]

 

def test_get_outputfile_header():
    test_dict = {
        'Columna 1': {'order': 1, 'output_fieldname': 'c1'},
        'Columna 2': {'order': 2, 'output_fieldname': 'c2'},
        'Columna 3': {'order': 3, 'output_fieldname': 'c3'}
    }
    assert get_outputfile_header(test_dict) == ['c1', 'c2', 'c3']

 

# Return a list with the output file header columns names ordered
def get_outputfile_ordered_header(file_schema: dict) -> list:
    header = []
    for column in file_schema.items():
        if not ('skip_output' in column[1].keys() and column[1]['skip_output'] == 1):
            header.append((column[1]['output_order'], column[1]['output_fieldname']))
            header.sort(key = lambda x: x[0])
    return [column[1] for column in header]

 

def test_get_outputfile_ordered_header():
    test_dict = {
        'Columna 1': {'order': 1, 'output_fieldname': 'c1', 'output_order': 2},
        'Columna 2': {'order': 2, 'output_fieldname': 'c2', 'output_order': 3},
        'Columna 3': {'order': 3, 'output_fieldname': 'c3', 'output_order': 1}
    }
    assert get_outputfile_ordered_header(test_dict) == ['c3', 'c1', 'c2']

 


def get_outputfile_formats(file_schema: dict) -> dict:
    formats = {}
    for column in file_schema.items():
        if not ('skip_output' in column[1].keys() and column[1]['skip_output'] == 1):
            key = column[1]['output_fieldname']
            formats[key] = {}
            formats[key]['type'] = column[1]['output_type']
            if 'output_format' in column[1]:
                formats[key]['format'] = column[1]['output_format']
            if 'output_datetime_zone' in column[1]:
                formats[key]['timezone'] = column[1]['output_datetime_zone']

    return formats