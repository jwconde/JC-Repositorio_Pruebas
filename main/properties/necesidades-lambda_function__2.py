import logging
import os
from datetime import datetime, timezone
from io import StringIO, BytesIO
import json
import boto3
import botocore.exceptions
import pandas as pd
import numpy as np

from sct_tools import *

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')

manage_layer_parameter = '/sct/manage_layer_bucket_name'
consum_layer_parameter = '/sct/consum_layer_bucket_name'

supplier = 'SAP'
dest = 'ZEUS'
file = 'necesidades'
cleansed_filename = 'necesidades'

csv_separator = ';'
file_encoding = 'utf-8'


def lambda_handler(event, context):
    # Initialize logger
    logger = initiate_logger(logging.INFO)
    logger.debug('Starting lambda handler')

    # Intialize return variables
    return_code = 200
    file_parsed_list = []

    # Check if event has the spected type
    if not isinstance(event, dict):
        return_code = 400
        return_message = 'The event has unexpected type (' + str(type(event)) + ')'
        logger.error(return_message)
        raise Error(return_message)

    # Check if event has Records key
    elif 'Records' in event.keys():

        # Capa manager
        # Get manage bucket name
        response = ssm_client.get_parameter(Name=manage_layer_parameter)
        if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
            manage_bucket_name = response['Parameter']['Value']
            logger.debug('Manage Bucket: ' + manage_bucket_name)
        else:
            return_message = 'Not found parameter: ' + manage_layer_parameter
            logger.error(return_message)
            return_code = 500
            raise Error(return_message)

        # Capa consumo
        # Get consum bucket name
        response = ssm_client.get_parameter(Name=consum_layer_parameter)
        if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
            consum_bucket_name = response['Parameter']['Value']
            logger.debug('Consum Bucket: ' + consum_bucket_name)
        else:
            return_message = 'Not found parameter: ' + consum_layer_parameter
            logger.error(return_message)
            return_code = 500
            raise Error(return_message)

        logger.debug('The event has ' + str(len(event['Records'])) + ' records')

        # Parse each event record
        for record in event['Records']:

            bucket_name = record['s3']['bucket']['name']
            file_name = record['s3']['object']['key']
            logger.debug('Parsing ' + bucket_name + ':' + file_name)

            # Retrieve object metadata
            logger.debug('Retriving object metadata')
            response = s3_client.head_object(Bucket=bucket_name,
                                             Key=file_name)

            # Check retrive object metadata response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.debug('ResponseMetadata is OK')
                creation_date = response['LastModified']
                creation_date.replace(tzinfo=timezone.utc)
            else:
                creation_date = datetime.utcnow()
                logger.warning('ResponseMetadata is incorrect. Setting current utc time')

            str_creation_date = creation_date.strftime(date_format)
            str_creation_datetime = creation_date.strftime(datetime_format)
            str_creation_datetime_utc = creation_date.strftime(utc_datetime_format)
            logger.debug('LastModified: ' + str_creation_datetime)

            # Archive file in Raw Bucket
            new_raw_key = supplier + '/' + file + '/date=' + str_creation_date
            new_raw_key += '/' + str_creation_datetime_utc + '-' + file_name
            logger.debug('The object is relocated in ' + new_raw_key)

            response = s3_client.copy_object(Bucket=bucket_name,
                                             Key=new_raw_key,
                                             CopySource={
                                                 'Bucket': bucket_name,
                                                 'Key': file_name
                                             },
                                             Metadata={
                                                 'creation_date': str_creation_datetime,
                                                 'supplier': supplier
                                             })

            # Check copy object response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.info('The file ' + file_name + ' was moved to ' + new_raw_key)
                logger.debug('The ETag of new object is: ' + response['CopyObjectResult']['ETag'])
            else:
                return_code = 500
                return_message = 'The file has not been copied. The response of the copy action is not ok'
                logger.error(return_message)
                raise Error(return_Message)

            # Delete copied file
            response = s3_client.delete_object(Bucket=bucket_name,
                                               Key=file_name)

            # Check delete copied file response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.debug('The file ' + file_name + 'has been deleted')
            else:
                logger.warning('The file ' + file_name + ' could not be deleted')

            # Download file schema from bucket
            schema_filename = 'FILES_DEFINITION_SCHEMA/' + file + '-schema.json'
            response = s3_client.get_object(Bucket=bucket_name,
                                            Key=schema_filename)

            # Check get schema response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.debug('The file schema ' + schema_filename + ' obtained successfully')
            else:
                return_code = 500
                return_message = 'The file schema has not been obtained. Is not posible parse document'
                logger.error(return_message)
                raise Error(return_Message)

            file_schema = StringIO(response['Body'].read().decode('utf-8'))
            file_schema = json.loads(file_schema.read())

            # Download file from bucket
            response = s3_client.get_object(Bucket=bucket_name,
                                            Key=new_raw_key)

            # Check download file response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.debug('The object ' + new_raw_key + ' has been obtained succesfully')
            else:
                return_message = 'The object ' + new_raw_key + ' has not been obtained and it is imposible to parse'
                logger.error(return_message)
                return_code = 500
                raise Error(return_message)

            # Parse file
            file_content = StringIO(response['Body'].read().decode(file_encoding))

            header_columns = get_inputfile_header(file_schema)
            column_types = get_inputfile_column_types(file_schema)

            # Create converters for correct column parsing
            # float_to_int_conv = lambda x: int(x[0:x.find('.')]) if x else None

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

                df = pd.read_csv(file_content,
                                 sep=csv_separator,
                                 header=None,
                                 names=header_columns,
                                 dtype=column_types,
                                 encoding=file_encoding
                                 #                 converters={
                                 #                            'Cantidad Planificada': float_to_int_conv
                                 #                            }
                                 )
            except Exception as e:
                logger.error(e)
                raise e

            logger.info('Readed ' + str(df.shape[0]) + ' lines')

            # Put Object in Curated Bucket
            # Create parquet file in tmp dir
            parquet_filename = str_creation_datetime_utc + '-' + file + '.parquet'
            parquet_path = '/tmp/'
            parquet_file = parquet_path + parquet_filename
            df.to_parquet(parquet_file,
                          engine='fastparquet',
                          compression=parquet_compression)

            # Read tmp parquet file to buffer
            f = open(parquet_file, 'rb')
            parquet_data = BytesIO(f.read())

            # Put object to S3
            parquet_key = supplier + '/' + file + '/date=' + str_creation_date
            parquet_key += '/' + parquet_filename
            response = s3_client.put_object(Bucket=manage_bucket_name,
                                            Key=parquet_key,
                                            Body=parquet_data,
                                            Metadata={
                                                'creation_date': str_creation_datetime,
                                                'supplier': supplier
                                            })
            f.close()

            # Remove parquet file in /tmp dir
            os.remove(parquet_file)

            # Check put object to S3 response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.info('Object ' + parquet_key + ' store in manage bucket (' + manage_bucket_name + ')')
            else:
                return_message = 'Error putting object ' + + ' ito manage bucket (' + manage_bucket_name + ')'
                return_code = 500
                logger.error(return_message)
                raise Error(return_message)

            # Put Object in Cleansed File
            # Get file generation datetime
            json_generate_date = datetime.utcnow()
            json_str_generate_date = json_generate_date.strftime(json_date_format)

            # Initialize file structure
            json_result = {
                'Fecha': json_str_generate_date,
                'NECESIDADES': []
            }

            # Get output columns names
            new_names = get_outputfile_header(file_schema)
            df.columns = new_names

            # Reorder dataframe
            ordered_output_header = get_outputfile_ordered_header(file_schema)
            df = df.reindex(columns=ordered_output_header)

    #   rev     # Remove column from origin file that is not mapped into the output
    #        df.drop('hash', axis=1, inplace=True)

            # Remove column from origin file that is not mapped into the output
    #        df.drop(['idRegistro', 'entrega', 'hash'], axis=1, inplace=True)


    #  rev      # Transform datetime columns into formatted strings with appropriate tz:
            specific_formats = get_outputfile_formats(file_schema)

            # Convert float a int
            df = df.astype({'cantidad': int})

            # Get dataframe data in json format
            result = df.to_json(orient='index')
            result = json.loads(result)

            # Fill file NECESIDADES
            for i in result.items():
                json_result['NECESIDADES'].append(i[1])

            # Dumps the result in JSON format
            json_result = json.dumps(json_result)
            data = BytesIO(json_result.encode('utf-8'))

            # Put object in bucket
            json_name = str_creation_datetime_utc + '-' + cleansed_filename + '.json'
            json_key_name = dest + '/' + file + '/date=' + str_creation_date + '/' + json_name
            response = s3_client.put_object(Bucket=consum_bucket_name,
                                            Key=json_key_name,
                                            Body=data,
                                            Metadata={
                                                'creation_date': str_creation_datetime,
                                                'supplier': supplier
                                            })

            # Check put object to S3 response
            if response['ResponseMetadata']['HTTPStatusCode'] // 100 == 2:
                logger.info('Object ' + json_name + ' stored in consum bucket (' + consum_bucket_name + ')')
            else:
                return_code = 500
                return_message = 'Error putting obejct ' + json_name
                return_message += ' into consum bucket (' + consum_bucket_name + ')'
                logger.error(return_message)
                raise Error(return_message)

            file_parsed_list.append((file_name, new_raw_key))

        return_message = 'Parsed this files: ' + str(file_parsed_list)
        logger.debug(return_message)

        return {
            'code': return_code,
            'message': return_message
        }