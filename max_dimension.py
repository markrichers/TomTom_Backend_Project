# -*- coding: utf-8 -*-
"""
Created on Wed Sep  3:09 2023
@author: marknguyen
"""

'''
The quality check should check for the allowed values related to the following Maximum Dimensions based on YARD:
    Maximum Height: Standard Road Edge or Ferry Edge has Maximum Total Height Allowed
    Maximum Length: Standard Road Edge or Ferry Edge has Maximum Total Length Allowed 
    Maximum Total Weight Allowed: Standard Road Edge or Ferry Edge has Maximum Total Weight Allowed
    Maximum Weight Per Axle Allowed: Standard Road Edge or Ferry Edge has Maximum Total Weight per Axle Allowed  
    Maximum Width Allowed: Standard Road Edge or Ferry Edge has Maximum Total Width Allowed


+ Maximum Dimension attributes:

    Maximum Total Weight
    Maximum Weight per Axle
    Maximum Height
    Maximum Length
    Maximum Width
'''


import os
import sys
import os
sys.path.append(r'C:\Users\phatthie\OneDrive - TomTom\Desktop\Coding Project\sourcing-grip-development\sourcing-grip\source-code\api')
# sys.path.append(r'C:\Users\phatthie\OneDrive - TomTom\Desktop\github\sourcing-grip-development\sourcing-grip\source-code\api')

import re
from qualitychecks.database import yard_connection, get_solite_km, check_maximum_dimension, update_quality, call_solite_max_dimension, insert_meta
import psycopg2
from datetime import datetime
import pandas as pd
import colorama

yhost = os.getenv('Y_HOST')
ydb = os.getenv('Y_DB')
yuser = os.getenv('Y_USER')
ypassw = os.getenv('Y_PASSWORD')
schema1 = os.getenv('Y_SCHEMA')
yport = os.getenv('Y_PORT')

ghost = os.getenv('G_HOST')
gdb = os.getenv('G_DB')
guser = os.getenv('G_USER')
gpassw = os.getenv('G_PASSWORD')

conn_guser = psycopg2.connect(database=gdb,
                        user=guser,
                        password=gpassw,
                        host=ghost
                        )


def get_allow_maximum_dimension_check(country_iso):
    def getMaximumAllowed(country_iso, dimension):
        try:
            conn = psycopg2.connect(database=ydb,
                                    user=yuser,
                                    password=ypassw,
                                    host=yhost,
                                    port=yport
                                    )
            yard = conn.cursor()

            schema = "yard_cpp_2"
            sql = "SELECT "
            sql = "SELECT "
            sql += "s.ScopeAdminKey AS country_iso"
            sql += ",c1.valuestring AS unit_of_measurement"
            sql += ",c2.valuelong AS lower_limit"
            sql += ",c3.valuelong AS upper_limit"
            sql += ",c4.valuelong AS lower_limit_motorways"
            sql += ",c5.valuestring AS allowed_on_motorways"
            sql += ",lt.typename"
            sql += " FROM " + schema + ".REFSCOPE s"
            sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
            sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
            sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
            sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = " + dimension['unit']
            sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c2   ON m.MetaDataId = c2.ListId AND c2.listcolumntypeid = " + dimension['lower_limit']
            sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c3   ON m.MetaDataId = c3.ListId AND c3.listcolumntypeid = " + dimension['upper_limit']
            sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c4   ON m.MetaDataId = c4.ListId AND c4.listcolumntypeid = " + dimension['lower_limit_motorways']
            sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c5   ON m.MetaDataId = c5.ListId AND c5.listcolumntypeid = " + dimension['allowed_on_motorways']
            sql += " WHERE lt.typeid = " + dimension['type']
            sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
            sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
            sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
            sql += " AND s.ScopeAdminKey ilike '" + country_iso + "'"
            yard.execute(sql)
            rows = yard.fetchall()
            conn.close()
            return rows
        except:

            conn.close()
            print(
                f'Getting allowed Maximum {dimension["name"]} from YARD failed')

    def getDimensionInfo(dimension_number):
        try:
            # //------Maximum Height: Standard Road Edge or Ferry Edge has Maximum Total Height Allowed
            if dimension_number == '568':
                return {'type': '568', 'name': 'MaximumHeightAllowed', 'unit': '572', 'lower_limit': '570', 'upper_limit': '573', 'lower_limit_motorways': '571', 'allowed_on_motorways': '569'}
            # //------Maximum Length: Standard Road Edge or Ferry Edge has Maximum Total Length Allowed
            elif dimension_number == '574':
                return {'type': '574', 'name': 'MaximumLengthAllowed', 'unit': '578', 'lower_limit': '576', 'upper_limit': '579', 'lower_limit_motorways': '577', 'allowed_on_motorways': '575'}
            # //------Maximum Total Weight Allowed: Standard Road Edge or Ferry Edge has Maximum Total Weight Allowed
            elif dimension_number == '580':
                return {'type': '580', 'name': 'MaximumTotalWeightAllowed', 'unit': '584', 'lower_limit': '582', 'upper_limit': '585', 'lower_limit_motorways': '583', 'allowed_on_motorways': '581'}
            # //------Maximum Weight Per Axle Allowed: Standard Road Edge or Ferry Edge has Maximum Total Weight per Axle Allowed
            elif dimension_number == '586':
                return {'type': '586', 'name': 'MaximumWeightPerAxle', 'unit': '590', 'lower_limit': '588', 'upper_limit': '591', 'lower_limit_motorways': '589', 'allowed_on_motorways': '587'}
            # //------Maximum Width Allowed: Standard Road Edge or Ferry Edge has Maximum Total Width Allowed
            elif dimension_number == '592':
                return {'type': '592', 'name': 'MaximumWidthAllowed', 'unit': '596', 'lower_limit': '594', 'upper_limit': '597', 'lower_limit_motorways': '595', 'allowed_on_motorways': '593'}
        except:
            return print('Dimension not found')


    try:
        dimension_numbers = ['568', '574', '580', '586', '592']
        results = []

        for dimension_number in dimension_numbers:
            dimension = getDimensionInfo(dimension_number)
            if dimension:
                allow_info = getMaximumAllowed(country_iso, dimension)
                results.extend([(dimension['name'], info) for info in allow_info])
            else:
                print(f'Error: Dimension {dimension_number} not found')

        return results
    
    except:
        return print('Getting allowed All Maximum Dimensions from YARD failed')

def dimension_character_check(grip_schema):
    max_dimension_type_collumns = ['maxdim1_type', 'maxdim2_type','maxdim3_type' ]
    max_dimension_value_collumns = ['maxdim1_value', 'maxdim2_value','maxdim3_value']
    list_with_dimension_not_allowed = []

    ## 1 through the list of columns list 
def return_solite_database(grip_schema): 
    action = call_solite_max_dimension(grip_schema)

    return action
    
def list_to_dataframe(yard_dimension_data):
    # Flatten the tuples in the list
    flattened_data = [(d[0], *d[1]) for d in yard_dimension_data]

    columns = ['max_type_yard', 'Country ISO', 'Unit of Measurement', 'Lower Limit', 'Upper Limit', 'Lower Limit Motorways', 'Allowed on Motorways', 'Typename']
    df = pd.DataFrame(flattened_data, columns=columns)
    return df

def compare_value_data(dataframe_yard_maximum, dataframe_solite_maximum):
    internal_id_data = {}

    # Mapping of different ways max_type_yard values can be written
    max_type_mapping = {
    'MaximumHeightAllowed': 'MaximumHeightAllowed',
    'MaximumLengthAllowed': 'MaximumLengthAllowed',
    'MaximumTotalWeightAllowed': 'MaximumTotalWeightAllowed',
    'MaximumWeightPerAxle': 'MaximumWeightPerAxle',
    'MaximumWidthAllowed': 'MaximumWidthAllowed',
    'MaximumTotalWeight': 'MaximumTotalWeightAllowed',
    }

    for index, rows in dataframe_solite_maximum.iterrows():
        internal_id = rows['internal_id']
        max_types = [('maxdim1_type', 'maxdim1_value'), ('maxdim2_type', 'maxdim2_value'), ('maxdim3_type', 'maxdim3_value')]

        for max_type, max_value in max_types:
            max_type_base = rows[max_type]
            value_type_base = rows[max_value]

            if max_type_base is not None and value_type_base is not None:
                # Standardize the max_type_base value
                max_type_base_standardized = max_type_mapping.get(max_type_base, max_type_base)
                yard_rows = dataframe_yard_maximum[dataframe_yard_maximum.max_type_yard == max_type_base_standardized]
                
                if not yard_rows.empty:
                    yard_row = yard_rows.iloc[0]
                    lower_limit = yard_row['Lower Limit']
                    upper_limit = yard_row['Upper Limit']

                    message = ""
                    max_dim = 0
                    if float(lower_limit) <= float(value_type_base) <= float(upper_limit):
                        message = f''
                        max_dim = 1
                    elif float(value_type_base) < float(lower_limit) or float(value_type_base) > float(upper_limit):
                        message = f'Not allowed in maximum dimension on {max_type_base_standardized}; the maximum value {value_type_base} is not within the allowed range of {lower_limit} and {upper_limit}; '
                        max_dim = 0

                    if internal_id not in internal_id_data:
                        internal_id_data[internal_id] = [(message, max_dim)]
                    else:
                        internal_id_data[internal_id].append((message, max_dim))

    return internal_id_data


def update_quality_test(schema, comment, max_dim, internal_id):
    try:
        # Check if the comment and max_dim values already exist
        check_sql = f"SELECT comment, max_dim FROM {schema}.quality WHERE internal_id = '{str(internal_id)}';"
        pre = conn_guser.cursor()
        pre.execute(check_sql)
        current_data = pre.fetchone()
        current_comment = current_data[0]
        current_max_dim = current_data[1]

        if current_comment is None:
            sql = f"UPDATE {schema}.quality SET comment = '{comment}', max_dim = {max_dim} WHERE internal_id = '{str(internal_id)}';"
            pre.execute(sql)
            conn_guser.commit()
            print(f"Inserted: Internal ID = {internal_id}, Comment = {comment}, Max Dim = {max_dim}")

        # If the current_comment is not None, update the row only if the comment and max_dim values have changed
        elif (current_comment != comment) or (current_max_dim != max_dim):
            sql = f"UPDATE {schema}.quality SET comment = '{comment}', max_dim = {max_dim} WHERE internal_id = '{str(internal_id)}';"
            pre.execute(sql)
            conn_guser.commit()
            print(f"Updated: Internal ID = {internal_id}, Comment = {comment}, Max Dim = {max_dim}")

        print(f"Finished updating quality table for internal ID {internal_id} successfully")
    
    except Exception as e:
        print(f"Failed to update quality table for internal ID {internal_id}")
        print(e)

from psycopg2 import sql

def insert_meta_dimension(schema, rule, status, error_count, meters, start_time, end_time, processing_time, job_id):
    try:
        global conn_guser
        cur = conn_guser.cursor()

        # Define the parameterized SQL query
        query = sql.SQL(
            "INSERT INTO {}.meta (id, check_id, status, failed, failed_meter, start_time, end_time, processing_time, job_id) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)"
        ).format(sql.Identifier(schema))

        # Execute the query with the given data
        cur.execute(query, (rule, status, error_count, meters, start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S'), str(processing_time), job_id))

        # Commit the transaction
        conn_guser.commit()

        # Close the cursor
        cur.close()

        print("Successfuly inserting data into {}.meta:".format(schema))
        print(start_time)

    except Exception as e:
        print("Error inserting data into {}.meta:".format(schema), e)

# Call the function with appropriate dataframes
# calculate the length fall within the range. 

def update_quality_comments(schema, internal_id_data):
    for internal_id, data in internal_id_data.items():
        for message, max_dim in data:
            update_quality_test(schema, message, max_dim, internal_id)

def max_dimension_wrapper(grip_schema, checks_status, error_summary, total_checks, successful_checks, job_id, country_iso):
    print("\n!---------------Maximum Dimension Check---------------!")
    start_time = datetime.now()
    rule = 36

    try:
        yard_dimension_data = get_allow_maximum_dimension_check(country_iso)
        dataframe_yard_maximum = list_to_dataframe(yard_dimension_data)

        print(dataframe_yard_maximum)

        dataframe_solite_maximum = return_solite_database(grip_schema)
        print(dataframe_solite_maximum.head(10))

        internal_id_data = compare_value_data(dataframe_yard_maximum, dataframe_solite_maximum)

        # Count the total checks and successful checks
        total_checks += len(internal_id_data)
        passed_checks = sum([all([max_dim == 1 for _, max_dim in messages_dims]) for messages_dims in internal_id_data.values()])
        error_count = sum([all([max_dim == 0 for _, max_dim in messages_dims]) for messages_dims in internal_id_data.values()])
        successful_checks += passed_checks

        status = "Failed" if passed_checks < len(internal_id_data) else "Passed"
        print("->No Maximum Dimension Issues Found" if status == "Passed" else "->Maximum Dimension Issues Found")

        checks_status[rule] = "No Maximum Dimension Issues Found" if status == "Passed" else "Maximum Dimension Issues Found"

        update_quality_comments(grip_schema, internal_id_data)
        print(job_id)

        end_time = datetime.now()
        processing_time = (end_time - start_time)
        incorrect_streetname_meters = 0

        insert_meta_dimension(grip_schema, rule, status, error_count, incorrect_streetname_meters, start_time, end_time, processing_time, job_id)

    except Exception as E:
        print(E)
        error_flag = True
        error_summary[rule] = str(E)
        checks_status[rule] = "Exception"
        print("Error in Max Dimension check")

    return total_checks, successful_checks

max_dimension_wrapper("evaluation_nld_414_temp", {}, {}, 0, 0, 414, "NLD")
    # print_table_with_non_null_comments(grip_schema)
    
# max_dimension_wrapper("SWE", "evaluation_swe_482")
# max_dimension_wrapper("NLD", "evaluation_nld_414_temp")

# schema = "evaluation_nld_414"

# def print_table_with_non_null_comments(schema):
#     try:
#         sql = f"SELECT internal_id, comment, max_dim FROM {schema}.quality WHERE max_dim IS NOT NULL;"
#         pre = conn_guser.cursor()
#         pre.execute(sql)
        
#         print("Internal ID\t\t\tComment\t\t\tMax Dim")
#         print("------------------------------------------------------------")
        
#         row = pre.fetchone()
#         while row:
#             print(f"{row[0]}\t{row[1]}\t{row[2]}")
#             row = pre.fetchone()
            
#     except Exception as e:
#         print(e)


def print_table_with_non_null_comments(schema):
    try:
        sql = f"SELECT internal_id, comment, max_dim FROM {schema}.quality WHERE max_dim IS NOT NULL;"
        pre = conn_guser.cursor()
        pre.execute(sql)
        rows = pre.fetchall()

        # Get column names from the cursor description
        columns = [column[0] for column in pre.description]

        # Print the column names
        print("\t".join(columns))
        
        # Print each row
        for row in rows:
            print("\t".join(map(str, row)))
    
    except Exception as e:
        # If any error occurs
        print("An error occurred: ", e)
    finally:
        # Close the cursor and connection to so the server can allocate
        # bandwidth to other requests
        pre.close()
        conn_guser.close()

print_table_with_non_null_comments("evaluation_nld_414_temp")        


    
