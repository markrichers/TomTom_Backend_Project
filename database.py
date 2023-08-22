# -*- coding: utf-8 -*-
"""
@author: Leo Brakels
"""
import psycopg2
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine
import geopandas as gpd
from shapely.wkt import loads
import shapely
from datetime import datetime

now = datetime.now()
date_time = now.strftime("%Y-%m-%d %H:%M:%S")
print(date_time)
load_dotenv()

### connect to DBS 

os.environ["PG_HOST"] = ""
os.environ["PG_DB"] = ""
os.environ["PG_USER"] = ""
os.environ["PG_PASSWORD"] = ""

os.environ["G_HOST"] = ''
os.environ["G_DB"] = ""
os.environ["G_USER"] = ""
os.environ["G_PASSWORD"] = ""

os.environ["Y_HOST"] = ""
os.environ["Y_DB"] = ""
os.environ["Y_USER"] = ""
os.environ["Y_PASSWORD"] = ""
os.environ["Y_SCHEMA"] = ""
os.environ["Y_PORT"] = ""

host = os.getenv('PG_HOST')
db = os.getenv('PG_DB')
user = os.getenv('PG_USER')
passw = os.getenv('PG_PASSWORD')

ghost = os.getenv('G_HOST')
gdb = os.getenv('G_DB')
guser = os.getenv('G_USER')
gpassw = os.getenv('G_PASSWORD')

yhost = os.getenv('Y_HOST')
ydb = os.getenv('Y_DB')
yuser = os.getenv('Y_USER')
ypassw = os.getenv('Y_PASSWORD')
schema1 = os.getenv('Y_SCHEMA')
yport = os.getenv('Y_PORT')

engine = create_engine('postgresql://' + user + ':' + passw + '@' + host + '/' + db)
engine_guser = create_engine('postgresql://' + guser + ':' + gpassw + '@' + ghost + ':5432/' + gdb)

conn = psycopg2.connect(database=gdb,
                       user=guser,
                       password=gpassw,
                       host=ghost
                      )

conn_guser = psycopg2.connect(database=gdb,
                        user=guser,
                        password=gpassw,
                        host=ghost
                        )

conn_guser1 = psycopg2.connect(database=gdb,
                user=guser,
                password=gpassw,
                host=ghost,
                sslmode='require'
                )
                                                                                            
gcursor1 = conn_guser1.cursor()

def yard_connection(language):
    yhost = os.getenv('Y_HOST')
    ydb = os.getenv('Y_DB')
    yuser = os.getenv('Y_USER')
    ypassw = os.getenv('Y_PASSWORD')
    schema1 = os.getenv('Y_SCHEMA')
    yport = os.getenv('Y_PORT')
    lang_c = language
    conn = psycopg2.connect(database=ydb,
                            user=yuser,
                            password=ypassw,
                            host=yhost,
                            port=yport,
                            options=f'-c search_path={schema1}'
                            )

    # Name has allowed normalized Name Component Type Prefix or Suffix
    # country
    lan_pre = conn.cursor()
    lan_pre.execute("""SELECT * FROM yard_cpp_2.refscope s 
    LEFT JOIN yard_cpp_2.refscopemetadata sm ON s.scopeid = sm.scopeid 
    LEFT JOIN yard_cpp_2.refmetadata m ON sm.metadataid = m.metadataid 
    LEFT JOIN yard_cpp_2.REFENTITYTYPE lt ON m.MetaDataTypeId = lt.TypeId 
    LEFT JOIN yard_cpp_2.REFLISTELEMENT c1 ON m.MetaDataId = c1.ListId 
    WHERE typename like '%Prefix%' AND scopeadminkey like '%{0}%' 
    """.format(lang_c))
    rowslan_pre = lan_pre.fetchall()

    # Street Name has not allowed First Character
    lan_str = conn.cursor()
    lan_str.execute("""SELECT * FROM yard_cpp_2.refscope s 
    LEFT JOIN yard_cpp_2.refscopemetadata sm ON s.scopeid = sm.scopeid 
    LEFT JOIN yard_cpp_2.refmetadata m ON sm.metadataid = m.metadataid 
    LEFT JOIN yard_cpp_2.REFENTITYTYPE lt ON m.MetaDataTypeId = lt.TypeId 
    LEFT JOIN yard_cpp_2.REFLISTELEMENT c1 ON m.MetaDataId = c1.ListId 
    WHERE typename like 'Street Name has not allowed First Character' AND scopeadminkey like '%{0}%' 
    """.format(lang_c))
    rowslan_str = lan_str.fetchall()

    # Route Number has allowed Character
    lan_rt = conn.cursor()
    lan_rt.execute("""SELECT * FROM yard_cpp_2.refscope s 
    LEFT JOIN yard_cpp_2.refscopemetadata sm ON s.scopeid = sm.scopeid 
    LEFT JOIN yard_cpp_2.refmetadata m ON sm.metadataid = m.metadataid 
    LEFT JOIN yard_cpp_2.REFENTITYTYPE lt ON m.MetaDataTypeId = lt.TypeId 
    LEFT JOIN yard_cpp_2.REFLISTELEMENT c1 ON m.MetaDataId = c1.ListId 
    WHERE typename like 'Route Number has allowed Character' AND scopeadminkey like '%{0}%' 
    """.format(lang_c))
    rowslan_rt = lan_rt.fetchall()

    return rowslan_pre, rowslan_str, rowslan_rt

#-----------------------------Import the yards connection function--------------------------------- 

#Get allowed language code(s) for a country

def getAllowedLanguageCode(country_code):
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
            sql += "s.ScopeAdminKey AS country_code"
            sql += ",c1.valuestring AS language_notation"
            sql += " FROM " + schema + ".REFSCOPE s"
            sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
            sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
            sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
            sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 1667"
            sql += " WHERE lt.typeid = 88"
            sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
            sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
            sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
            sql += " AND s.ScopeAdminKey ilike '" + country_code + "'"
            yard.execute(sql)
            rows = yard.fetchall()
            conn.close()
            return rows
        except:
            conn.close()
            print('Getting allowed language code(s) from YARD failed')

#-----------------------------Get street name has allowed character from YARD database--------------------------------- 
def getAllowedStreetNameCharacters(language_notation):
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
        sql += "s.ScopeAdminKey AS language_notation"
        sql += ",c1.valuestring AS allowed_character"
        sql += " FROM " + schema + ".REFSCOPE s"
        sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
        sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
        sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 426"
        sql += " WHERE lt.typeid = 425"
        sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
        sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
        sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
        sql += " AND s.ScopeAdminKey ilike '" + language_notation + "'"
        yard.execute(sql)
        rows = yard.fetchall()
        conn.close()
        return rows
    except:
        conn.close()
        print('Getting street name has allowed character from YARD failed')

#-----------------------------Get street name has not allowed first character from YARD database--------------------------------- 
def getNotAllowedFirstStreetNameCharacters(language_notation):
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
        sql += "s.ScopeAdminKey AS language_notation"
        sql += ",c1.valuestring AS not_allowed_character"
        sql += " FROM " + schema + ".REFSCOPE s"
        sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
        sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
        sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 428"
        sql += " WHERE lt.typeid = 427"
        sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
        sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
        sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
        sql += " AND s.ScopeAdminKey ilike '" + language_notation + "'"
        yard.execute(sql)
        rows = yard.fetchall()
        conn.close()
        return rows
    except:
        conn.close()
        print('Getting street name has not allowed first character from YARD failed')

#-----------------------------Get main allowed name components from YARD database--------------------------------- 
def getAllowedNameComponentsMain(language_notation, component_type):
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
        sql += "s.ScopeAdminKey AS language_notation"
        sql += ",c1.valuestring AS name_component_code"
        sql += ",CASE"
        sql += " WHEN c1.valuestring = '1' THEN 'Pre-Directional'"
        sql += " WHEN c1.valuestring = '2' THEN 'Prefix'"
        sql += " WHEN c1.valuestring = '3' THEN 'Body'"
        sql += " WHEN c1.valuestring = '4' THEN 'Suffix'"
        sql += " WHEN c1.valuestring = '5' THEN 'Post_Directional'"
        sql += " WHEN c1.valuestring = '6' THEN 'Key'"
        sql += " WHEN c1.valuestring = '7' THEN 'Surname'"
        sql += " WHEN c1.valuestring = '8' THEN 'Article/Preposition'"
        sql += " WHEN c1.valuestring = '9' THEN 'Exit Number'"
        sql += " WHEN c1.valuestring = '10' THEN 'Route Shield Number'"
        sql += " WHEN c1.valuestring = '11' THEN 'Route Directional'"
        sql += " END as name_component_type"
        sql += ",c2.valuestring AS name_component_text"
        sql += ",c3.valuestring AS country_dependency"
        sql += " FROM " + schema + ".REFSCOPE s"
        sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
        sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
        sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 1063 AND c1.valuestring = '" + str(component_type) + "'"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c2   ON m.MetaDataId = c2.ListId AND c1.listrowNr = c2.listRowNr AND c2.ListColumnTypeId = 1062"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c3   ON m.MetaDataId = c3.ListId AND c1.listrowNr = c3.listRowNr AND c3.ListColumnTypeId = 1061"
        sql += " WHERE lt.typeid = 1059 AND c3.valuestring IS NULL"
        sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
        sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
        sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
        sql += " AND s.ScopeAdminKey ilike '" + language_notation + "'"
        yard.execute(sql)
        rows = yard.fetchall()
        conn.close()
        return rows
    except:
        conn.close()
        print('Getting allowed name components from YARD failed')

#-----------------------------Get aditional allowed name components from YARD database with special country attributes---------------------------- 
def getAllowedNameComponentsAdditional(language_notation, country_dependency, component_type):
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
        sql += "s.ScopeAdminKey AS language_notation"
        sql += ",c1.valuestring AS name_component_code"
        sql += ",CASE"
        sql += " WHEN c1.valuestring = '1' THEN 'Pre-Directional'"
        sql += " WHEN c1.valuestring = '2' THEN 'Prefix'"
        sql += " WHEN c1.valuestring = '3' THEN 'Body'"
        sql += " WHEN c1.valuestring = '4' THEN 'Suffix'"
        sql += " WHEN c1.valuestring = '5' THEN 'Post_Directional'"
        sql += " WHEN c1.valuestring = '6' THEN 'Key'"
        sql += " WHEN c1.valuestring = '7' THEN 'Surname'"
        sql += " WHEN c1.valuestring = '8' THEN 'Article/Preposition'"
        sql += " WHEN c1.valuestring = '9' THEN 'Exit Number'"
        sql += " WHEN c1.valuestring = '10' THEN 'Route Shield Number'"
        sql += " WHEN c1.valuestring = '11' THEN 'Route Directional'"
        sql += " END as name_component_type"
        sql += ",c2.valuestring AS name_component_text"
        sql += ",c3.valuestring AS country_dependency"
        sql += " FROM " + schema + ".REFSCOPE s"
        sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
        sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
        sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 1063 AND c1.valuestring = '" + str(component_type) + "'"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c2   ON m.MetaDataId = c2.ListId AND c1.listrowNr = c2.listRowNr AND c2.ListColumnTypeId = 1062"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c3   ON m.MetaDataId = c3.ListId AND c1.listrowNr = c3.listRowNr AND c3.ListColumnTypeId = 1061"
        sql += " WHERE lt.typeid = 1059 "
        sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
        sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
        sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
        sql += " AND s.ScopeAdminKey ilike '" + language_notation + "'"
        sql += " AND (c3.valuestring IS NULL OR c3.valuestring = '" + str(country_dependency) + "')"
        yard.execute(sql)
        rows = yard.fetchall()
        conn.close()
        return rows
    except:
        conn.close()
        print('Getting allowed name components from YARD failed')

def getAllowedRouteNumberStructure(country_code):
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
        sql += "s.ScopeAdminKey AS country_code"
        sql += ",c1.valuestring AS structure"
        sql += " FROM " + schema + ".REFSCOPE s"
        sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
        sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
        sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
        sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 1082"
        sql += " WHERE lt.typeid = 1079"
        sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
        sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
        sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
        sql += " AND s.ScopeAdminKey ilike '" + country_code + "'"
        yard.execute(sql)
        rows = yard.fetchall()
        conn.close()
        return rows
    except:
        conn.close()
        print('Getting allowed language code(s) from YARD failed')

def yard_connection(language):
    yhost = os.getenv('Y_HOST')
    ydb = os.getenv('Y_DB')
    yuser = os.getenv('Y_USER')
    ypassw = os.getenv('Y_PASSWORD')
    schema1 = os.getenv('Y_SCHEMA')
    yport = os.getenv('Y_PORT')
    lang_c = language
    conn = psycopg2.connect(database=ydb,
                            user=yuser,
                            password=ypassw,
                            host=yhost,
                            port=yport,
                            options=f'-c search_path={schema1}'
                            )


def mnr_network(schema, polygon, host):
    try:
        con = psycopg2.connect(database=db,
                            user=user,
                            password=passw,
                            host=host,
                            port='5432'
                            )
        mnr = con.cursor()

        sql = """SELECT gl.feat_id, feat_type, ferry_type, name, lang_code, back_road, stubble, display_class, routing_class, form_of_way, freeway,
                road_condition, restricted_access, part_of_structure, simple_traffic_direction, centimeters, gl.geom
                FROM {0}.mnr_netw_geo_link as gl,
                    {0}.mnr_netw_route_link as rl
                WHERE ST_Intersects(gl.geom ,ST_GeomFromText('{1}', 4326)) AND feat_type IN(4110, 4130) AND gl.feat_id = rl.netw_geo_id""".format(schema, polygon)
        mnr.execute(sql)
        return mnr
    except Exception as error:
        close_mnr = con.close()
        print(error)

def get_road_geometry_and_id(grip_schema):

    sql = f"""SELECT internal_id, ST_AsText(geom) FROM {grip_schema}.solite"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    geometries = pre.fetchall()

    return geometries

def get_start_and_end_point_for_sharp_angle(grip_schema):

    sql = f"""SELECT ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), ST_X(ST_PointN(geom, 2)), ST_Y(ST_PointN(geom, 2)), ST_X(ST_PointN(geom, -2)), ST_Y(ST_PointN(geom, -2)), internal_id FROM {grip_schema}.solite"""
    pre = conn_guser.cursor()
    pre.execute(sql)

    point_lists = []
    
    for i in pre.fetchall():
        entity_dic = {
            'x_start': i[0],
            'y_start': i[1],
            'x_end': i[2],
            'y_end': i[3],
            'x_second': i[4],
            'y_second': i[5],
            'x_second_last': i[6],
            'y_second_last': i[7],
            'internal_id': i[8]
        }
        point_lists.append(entity_dic)

    return point_lists

def get_start_and_end_point_for_dtfr(grip_schema):

    sql = f"""SELECT ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), ST_X(ST_PointN(geom, 2)), ST_Y(ST_PointN(geom, 2)), ST_X(ST_PointN(geom, -2)), ST_Y(ST_PointN(geom, -2)), internal_id, dtfr FROM {grip_schema}.solite WHERE (dtfr = 'InPositiveDirection' OR dtfr = 'InNegativeDirection')"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    point_list = pre.fetchall()
    return point_list

def get_start_and_end_point_for_sn(grip_schema, streetname_column):

    sql = f"""SELECT ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), ST_X(ST_PointN(geom, 2)), ST_Y(ST_PointN(geom, 2)), ST_X(ST_PointN(geom, -2)), ST_Y(ST_PointN(geom, -2)), internal_id, {streetname_column} FROM {grip_schema}.solite WHERE ({streetname_column} != '' AND {streetname_column} IS NOT NULL)"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    point_list = pre.fetchall()
    return point_list

def get_frc_attributes(grip_schema):

    entities_with_attribute_for_frc_checks = []

    sql = f"""SELECT internal_id, isocountrycode, restrictedaccess, dtfr, formofway, backroad, roadcondition,
        stubble, routenumber1, routenumber2, routenumber3, frc FROM {grip_schema}.solite
        GROUP BY internal_id, isocountrycode, restrictedaccess, dtfr, formofway, backroad, roadcondition, stubble, routenumber1, routenumber2, routenumber3, frc
		HAVING SUM((SELECT COUNT(frc) FROM {grip_schema}.solite WHERE frc IS NOT NULL)) <> 0"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    attributes_dict = {}

    for i in pre.fetchall():
        if i is not None:
            attributes_dict = {
                'internal_id' : i[0],
                'isocountrycode': i[1],
                'restrictedaccess': i[2],
                'dtfr': i[3],
                'formofway': i[4],
                'backroad': i[5],
                'roadcondition': i[6],
                'stubble': i[7],
                'routenumber1': i[8],
                'routenumber2': i[9],
                'routenumber3': i[10],
                'frc': i[11]
            }

        entities_with_attribute_for_frc_checks.append(attributes_dict)
    
    return entities_with_attribute_for_frc_checks

def get_dtfr_attributes(grip_schema):

    entities_with_attribute_for_dtfr_checks = []

    sql = f"""SELECT internal_id, isocountrycode, dtfr, dtfr_pc, dtfr_pb, dtfr_mt, dtfr_tx, dtfr_rs, dtfr_pc_td, dtfr_pb_td, dtfr_mt_td, dtfr_tx_td, dtfr_rs_td, formofway, frc FROM {grip_schema}.solite
                WHERE dtfr IS NOT NULL OR dtfr_pc IS NOT NULL OR dtfr_pb IS NOT NULL OR dtfr_mt IS NOT NULL OR dtfr_tx IS NOT NULL OR dtfr_rs IS NOT NULL OR dtfr_pc_td IS NOT NULL OR dtfr_pb_td IS NOT NULL OR dtfr_mt_td IS NOT NULL OR dtfr_tx_td IS NOT NULL OR dtfr_rs_td IS NOT NULL"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    
    attributes_dict = {}

    for i in pre.fetchall():
        if i is not None:
            attributes_dict = {
                'internal_id' : i[0],
                'isocountrycode': i[1],
                'dtfr': i[2],
                'dtfr_pc': i[3],
                'dtfr_pb': i[4],
                'dtfr_mt': i[5],
                'dtfr_tx': i[6],
                'dtfr_rs': i[7],
                'dtfr_pc_td': i[8],
                'dtfr_pb_td': i[9],
                'dtfr_mt_td': i[10],
                'dtfr_tx_td': i[11],
                'dtfr_rs_td': i[12],
                'formofway': i[13],
                'frc': i[14]
            }

        entities_with_attribute_for_dtfr_checks.append(attributes_dict)
    
    return entities_with_attribute_for_dtfr_checks

def get_road_condition(grip_schema):

    entities_with_attribute_for_roadcondition_checks = []

    sql = f"""SELECT internal_id, roadcondition FROM {grip_schema}.solite
                WHERE roadcondition IS NOT NULL"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    
    attributes_dict = {}

    for i in pre.fetchall():
        if i is not None:
            attributes_dict = {
                'internal_id' : i[0],
                'roadcondition': i[1]
            }

        entities_with_attribute_for_roadcondition_checks.append(attributes_dict)
    
    return entities_with_attribute_for_roadcondition_checks

def get_restricted_access(grip_schema):

    entities_with_attribute_for_restrictedaccess_checks = []

    sql = f"""SELECT internal_id, restrictedaccess, isocountrycode FROM {grip_schema}.solite
                WHERE restrictedaccess IS NOT NULL"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    
    attributes_dict = {}

    for i in pre.fetchall():
        if i is not None:
            attributes_dict = {
                'internal_id' : i[0],
                'restrictedaccess': i[1],
                'isocountrycode': i[2]
            }

        entities_with_attribute_for_restrictedaccess_checks.append(attributes_dict)
    
    return entities_with_attribute_for_restrictedaccess_checks

def get_backroad(grip_schema):

    entities_with_attribute_for_backroad_checks = []

    sql = f"""SELECT internal_id, backroad, isocountrycode FROM {grip_schema}.solite
                WHERE backroad IS NOT NULL"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    
    attributes_dict = {}

    for i in pre.fetchall():
        if i is not None:
            attributes_dict = {
                'internal_id' : i[0],
                'backroad': i[1],
                'isocountrycode': i[2]
            }

        entities_with_attribute_for_backroad_checks.append(attributes_dict)
    
    return entities_with_attribute_for_backroad_checks

def check_street_names(grip_schema):

    sql = f"""SELECT internal_id
		        FROM {grip_schema}.solite
		        WHERE streetname1 IS NOT NULL OR streetname2 IS NOT NULL OR streetname3 IS NOT NULL  OR streetname4 IS NOT NULL"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    if pre.fetchone() is None:
        return "pass"
    else:
        return "check"

import pandas as pd
# SELECT internal_id, maxdim1_type, maxdim1_value, maxdim2_type, maxdim2_value, maxdim3_type, maxdim3_value, length
def call_solite_max_dimension(grip_schema):

    sql = f"""SELECT internal_id, maxdim1_type, maxdim1_value, maxdim2_type, maxdim2_value, maxdim3_type, maxdim3_value
		        FROM {grip_schema}.solite
		        WHERE (maxdim1_TYPE IS NOT NULL OR maxdim1_value IS NOT NULL)
                OR (maxdim2_TYPE IS NOT NULL OR maxdim2_value IS NOT NULL)
                OR (maxdim3_TYPE IS NOT NULL OR maxdim3_value IS NOT NULL)"""
    

    pre = conn_guser.cursor()
    pre.execute(sql)

    rows = pre.fetchall()
    column_names = [desc[0] for desc in pre.description]
    
    dataframe = pd.DataFrame(rows, columns=column_names)
    
    return dataframe 



def check_maximum_dimension(grip_schema):

    sql = f"""SELECT internal_id, maxdim1_type, maxdim1_value, maxdim2_type, maxdim2_value, maxdim3_type, maxdim3_value
		        FROM {grip_schema}.solite
		        WHERE (maxdim1_TYPE IS NOT NULL OR maxdim1_value IS NOT NULL)
                OR (maxdim2_TYPE IS NOT NULL OR maxdim2_value IS NOT NULL)
                OR (maxdim3_TYPE IS NOT NULL OR maxdim3_value IS NOT NULL)"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    rows = pre.fetchall()
    column_names = [desc[0] for desc in pre.description]
    
    dataframe = pd.DataFrame(rows, columns=column_names)

    print(dataframe)

    if pre.fetchone() is None:
        return "pass"
    else:
        return "check"

def get_unique_street_names(grip_schema, streetname):

    sql = f"""SELECT {streetname}, {streetname + '_lng'}
		        FROM {grip_schema}.solite
		        WHERE {streetname} IS NOT NULL
                GROUP BY {streetname}, {streetname + '_lng'}"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    names = pre.fetchall()
    return names


def check_routenumber(grip_schema):

    sql = f"""SELECT internal_id
		        FROM {grip_schema}.solite
		        WHERE routenumber1 IS NOT NULL OR routenumber2 IS NOT NULL OR routenumber3 IS NOT NULL"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    if pre.fetchone() is None:
        return "pass"
    else:
        return "check"


def get_isolated_geometry(grip_schema):

    # construct a graph from the entity ids
    sql = f"""SELECT internal_id, ST_Buffer(ST_Endpoint(geom)::geography, 7.5, 8), ST_Buffer(ST_Startpoint(geom)::geography, 7.5, 8), ST_AsText(geom), ST_Length(geom::geography)
        FROM {grip_schema}.solite
        """

    pre = conn_guser.cursor()
    pre.execute(sql)
    vertices = pre.fetchall()
    graph = [[0]*len(vertices)]*len(vertices)
    
    entity_by_index = {}

    for i in range(len(vertices)):
        entity_by_index[vertices[i][0]] = i


    for j in range(len(vertices)):
        get_intersects = f"""SELECT internal_id
            FROM {grip_schema}.solite
            WHERE ST_Intersects(geom, 'SRID=4326;{vertices[j][3]}'::geometry) OR ST_Intersects('{vertices[j][1]}', geom) OR ST_Intersects('{vertices[j][2]}', geom);"""

        #print(get_intersects)
    
        pre.execute(get_intersects)
        intersections = pre.fetchall()
        
        intersection_indices = []
        for intersection in intersections:
            intersection_indices.append(entity_by_index[intersection[0]])

        graph[j] = intersection_indices


    # Find all connected components in a graph, save it as a list
    # Pseudo: DFS on graph, each v found -> add to list. No v found -> finish list
    visited = set() 
    graph_components = []
    max_component_size = -1

    for i in range(len(graph)):
        if i not in visited:
            found_component = []
            dfs_stack = []
            connections = 0
            dfs_stack.append(i)
            while len(dfs_stack) != 0:
                current_vertex = dfs_stack.pop() 
                if current_vertex not in visited:
                    connections += len(graph[current_vertex])
                    for j in graph[current_vertex]:
                        dfs_stack.append(j)
                visited.add(current_vertex)
                if current_vertex not in found_component:
                    found_component.append(current_vertex)   
            graph_components.append(found_component)

            if len(found_component) > max_component_size:
                max_component_size = len(found_component)
    #print(graph_components)


    found_isolated_geom = set()
    print("Max component size is: ", max_component_size)

    for i in graph_components:
        if len(i) != max_component_size:
            for geometry in i:
                found_isolated_geom.add(vertices[geometry][0])
    length = 0
    for vertice in vertices:
        for isolated_feature in found_isolated_geom:
            if isolated_feature == vertice[0]:
                length += vertice[4]

    return found_isolated_geom, length

def get_point_of_interesection(grip_schema):

    sql = f"""SELECT a.internal_id AS internal_id1, b.internal_id AS internal_id2, ST_AsText(ST_StartPoint(a.geom)) AS startpoint, ST_AsText(ST_EndPoint(a.geom)) AS endpoint, ST_AsText(ST_Intersection(a.geom, b.geom)) AS intersection_point
            FROM {grip_schema}.solite a, {grip_schema}.solite b
            WHERE ST_Intersects(a.geom, b.geom) AND ST_AsText(ST_Intersection(a.geom, b.geom)) != ST_AsText(ST_StartPoint(a.geom))
            AND ST_AsText(ST_Intersection(a.geom, b.geom)) != ST_AsText(ST_EndPoint(a.geom)) AND a.internal_id != b.internal_id"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    
    # # This dictionary will contain a list of point of intersection and where they are intersected 
    intersection_dict = {}
    # # This set is to prevent duplicate points


    # Point of intersection would be at index 4
    # Start point at index 2
    # End point at index 3    
    # Load the point of intersection between 2 lines. The key of the dictionary is the id of the line and the value would be a pair of line it intersect with and the point(s) of intersection
    for i in pre.fetchall():

        point_of_intersection = i[4]

        start_point = i[2]
        end_point = i[3]

        if point_of_intersection.find('MULTIPOINT') != -1:
            sql = f"""SELECT ST_AsText(geom)
                        FROM (
                        SELECT (ST_DumpPoints(g.geom)).*
                        FROM
                            (SELECT
                            '{point_of_intersection}'::geometry AS geom
                            ) AS g
                        ) pt;"""
            pre.execute(sql)
            points = pre.fetchall()
            for point in points:
                if (point[0] != end_point and point[0] != start_point):
                    intersection_dict[i[0]] = [i[1], point[0]]
        else:
            intersection_dict[i[0]] = [i[1], i[4]]
    
    intersection_dict['error_count'] = len(intersection_dict)
    return intersection_dict

def get_spikes(grip_schema, threshold):

    sql = f"""WITH result AS (SELECT points.id, points.anchor, (degrees
                (
                ST_Azimuth(points.anchor, points.pt1) - ST_Azimuth(points.anchor, points.pt2)
                )::decimal + 360) % 360 as angle
                    FROM
                    (SELECT
                        ST_PointN(geom, generate_series(1, ST_NPoints(geom)-2)) as pt1,
                        ST_PointN(geom, generate_series(2, ST_NPoints(geom)-1)) as anchor,
                        ST_PointN(geom, generate_series(3, ST_NPoints(geom))) as pt2,
                        linestrings.id as id
                        FROM
                            (SELECT internal_id as id, geom
                                FROM {grip_schema}.solite WHERE ST_NPoints(geom) > 2
                                ) AS linestrings) as points)
            select distinct id, ST_AsText(anchor), angle from result where (result.angle % 360) < {threshold} or result.angle > (360.0 - ({threshold} % 360.0))"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    geometries = pre.fetchall()

    return geometries

def get_deadend_branch_geometry(grip_schema):
    """Function to get the dead-end branch geometry that will create a restriced access island

    The query performs the following actions:
     -- Iterate through linestrings, assigning each to a cluster (if there is an intersection) or creating a new cluster (if there is not)
     -- Iterate through the clusters, combining clusters that intersect each other
     -- Remove cluster with the highest feature count (Main network)
     -- Remove cluster(s) that don't intersect with a road with Restricted Access value

    """

    try:
        sql = f"""
        DO
        $$
        DECLARE
        this_id character varying (50);
        this_geom geometry;
        this_ra character varying(40);
        cluster_id_match integer;

        id_a bigint;
        id_b bigint;

        BEGIN
        DROP TABLE IF EXISTS {grip_schema}.clusters;
        CREATE TABLE {grip_schema}.clusters (cluster_id serial, ids character varying[], ra character varying[], geom geometry);
        CREATE INDEX ON {grip_schema}.clusters USING GIST(geom);

        FOR this_id, this_ra, this_geom IN SELECT internal_id, restrictedaccess, geom FROM {grip_schema}.solite WHERE (restrictedaccess = 'NotRestrictedAccess' OR restrictedaccess IS NULL) LOOP
        SELECT cluster_id FROM {grip_schema}.clusters WHERE ST_Intersects(this_geom, clusters.geom)
            LIMIT 1 INTO cluster_id_match;

        IF cluster_id_match IS NULL THEN
            INSERT INTO {grip_schema}.clusters (ids, ra, geom) VALUES (ARRAY[this_id], ARRAY[this_ra], this_geom);
        ELSE
            UPDATE {grip_schema}.clusters SET geom = ST_Union(this_geom, geom),
                                ids = array_prepend(this_id, ids),
                                ra = array_prepend(this_ra, ra)
            WHERE clusters.cluster_id = cluster_id_match;
        END IF;
        END LOOP;

        LOOP
            SELECT a.cluster_id, b.cluster_id FROM {grip_schema}.clusters a, {grip_schema}.clusters b 
            WHERE ST_Intersects(a.geom, b.geom)
            AND a.cluster_id < b.cluster_id
            INTO id_a, id_b;

            EXIT WHEN id_a IS NULL;
            UPDATE {grip_schema}.clusters a SET geom = ST_Union(a.geom, b.geom), ids = array_cat(a.ids, b.ids), ra = array_cat(a.ra, b.ra)
            FROM {grip_schema}.clusters b
            WHERE a.cluster_id = id_a AND b.cluster_id = id_b;

            DELETE FROM {grip_schema}.clusters WHERE cluster_id = id_b;
        END LOOP;
        DELETE FROM {grip_schema}.clusters WHERE cluster_id = (SELECT cluster_id FROM {grip_schema}.clusters WHERE array_length(ids, 1) = (SELECT array_length(ids, 1)
            FROM {grip_schema}.clusters
            ORDER BY array_length DESC
            LIMIT 1));
        DELETE FROM {grip_schema}.clusters
        WHERE {grip_schema}.clusters.cluster_id NOT IN (SELECT cluster_id
            FROM {grip_schema}.clusters cl, {grip_schema}.solite ra
            WHERE ST_Intersects(cl.geom, ra.geom) AND ra.restrictedaccess != 'NotRestrictedAccess' AND restrictedaccess IS NOT NULL);
        END;
        $$ language plpgsql;"""

        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()

        sql = f"""SELECT ids, ra, ST_Length(geom::geography) AS length FROM {grip_schema}.clusters"""
        pre.execute(sql)
        query_results = pre.fetchall()


    except Exception as E:
        print(E)
    
    return query_results

def backroad_clustering_query(grip_schema, backroad_cluster_parameters, backroad_issue_parameters):
    backroad_issues = []
    try:
        sql = f"""
            DO
            $$
            DECLARE
            this_id character varying (50);
            this_geom geometry;
            this_br character varying(40);
            cluster_id_match integer;
            id_a bigint;
            id_b bigint;
            BEGIN
            -- Create a cluster for all geometries regardless of their backroad value --
            DROP TABLE IF EXISTS {grip_schema}.clusters;
            CREATE TABLE {grip_schema}.clusters (cluster_id serial, ids character varying[], br character varying[], geom geometry);
            CREATE INDEX ON {grip_schema}.clusters USING GIST(geom);
            FOR this_id, this_br, this_geom IN SELECT internal_id, backroad, geom FROM {grip_schema}.solite WHERE ({backroad_cluster_parameters}) LOOP
            SELECT cluster_id FROM {grip_schema}.clusters WHERE ST_Intersects(this_geom, clusters.geom)
                LIMIT 1 INTO cluster_id_match;
            IF cluster_id_match IS NULL THEN
                INSERT INTO {grip_schema}.clusters (ids, br, geom) VALUES (ARRAY[this_id], ARRAY[this_br], this_geom);
            ELSE
                UPDATE {grip_schema}.clusters SET geom = ST_Union(this_geom, geom),
                                    ids = array_prepend(this_id, ids),
                                    br = array_prepend(this_br, br)
                WHERE clusters.cluster_id = cluster_id_match;
            END IF;
            END LOOP;
            LOOP
                SELECT a.cluster_id, b.cluster_id FROM {grip_schema}.clusters a, {grip_schema}.clusters b 
                WHERE ST_Intersects(a.geom, b.geom)
                AND a.cluster_id < b.cluster_id
                INTO id_a, id_b;
                EXIT WHEN id_a IS NULL;
                UPDATE {grip_schema}.clusters a SET geom = ST_Union(a.geom, b.geom), ids = array_cat(a.ids, b.ids), br = array_cat(a.br, b.br)
                FROM {grip_schema}.clusters b
                WHERE a.cluster_id = id_a AND b.cluster_id = id_b;
                DELETE FROM {grip_schema}.clusters WHERE cluster_id = id_b;
            END LOOP;
            DELETE FROM {grip_schema}.clusters WHERE cluster_id = (SELECT cluster_id FROM {grip_schema}.clusters WHERE array_length(ids, 1) = (SELECT array_length(ids, 1)
                FROM {grip_schema}.clusters
                ORDER BY array_length DESC
                LIMIT 1));
            DELETE FROM {grip_schema}.clusters
        WHERE {grip_schema}.clusters.cluster_id NOT IN (SELECT cluster_id
            FROM {grip_schema}.clusters cl, {grip_schema}.solite ba
            WHERE ST_Intersects(cl.geom, ba.geom) AND {backroad_issue_parameters});
            END;
            $$ language plpgsql;
            """

        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()

        # Get all backroad issue clusters
        sql = f"""SELECT ids, br, ST_Length(geom::geography) AS length FROM {grip_schema}.clusters"""
        pre.execute(sql)
        backroad_issues = pre.fetchall()

    except Exception as E:
        print(E)

    return backroad_issues

def get_backroad_deadend_geometry(grip_schema):
    pre = conn_guser.cursor()

    sql_group = f"""SELECT backroad FROM {grip_schema}.solite GROUP BY backroad"""
    pre.execute(sql_group)
    available_backroads = pre.fetchall()
    seen_backroads = set()

    for available_backroad in available_backroads:
        if available_backroad[0] not in seen_backroads:
            seen_backroads.add(available_backroad[0])
    
    backroad_issues = []

    # Query 1
    for backroad_issue in backroad_clustering_query(grip_schema, "backroad = 'NotABackRoad' OR backroad IS NULL", "backroad != 'NotABackRoad' AND backroad IS NOT NULL"):
        if backroad_issue not in backroad_issues:
            backroad_issues.append(backroad_issue)

    # Query 2
    if "DestinationRoad" in seen_backroads and "Driveway" not in seen_backroads:
        for backroad_issue_1 in backroad_clustering_query(grip_schema, "backroad = 'NotABackRoad' OR backroad IS NULL OR backroad = 'DestinationRoad'", "backroad != 'NotABackRoad' AND backroad IS NOT NULL AND backroad != 'DestinationRoad'"):
            # Make sure no duplicate are added
            if backroad_issue_1 not in backroad_issues:
                backroad_issues.append(backroad_issue_1)
    
    # Query 3
    if "Driveway" in seen_backroads and "DestinationRoad" not in seen_backroads:
        for backroad_issue_2 in backroad_clustering_query(grip_schema, "backroad = 'NotABackRoad' OR backroad IS NULL OR backroad = 'Driveway'", "backroad != 'NotABackRoad' AND backroad IS NOT NULL AND backroad != 'Driveway'"):
            # Make sure no duplicate are added
            if backroad_issue_2 not in backroad_issues:
                backroad_issues.append(backroad_issue_2)

    # Query 4
    if "Driveway" in seen_backroads and "DestinationRoad" in seen_backroads:
        for backroad_issue_2 in backroad_clustering_query(grip_schema, "backroad = 'NotABackRoad' OR backroad IS NULL OR backroad = 'Driveway' OR backroad = 'DestinationRoad'", "backroad != 'NotABackRoad' AND backroad IS NOT NULL AND backroad != 'Driveway' AND backroad != 'DestinationRoad'"):
            # Make sure no duplicate are added
            if backroad_issue_2 not in backroad_issues:
                backroad_issues.append(backroad_issue_2)
    
    
    return backroad_issues


def get_water_area(mnr_schema, host, xmin, ymin, xmax, ymax):

    con = psycopg2.connect(database=db,
                           user=user,
                           password=passw,
                           host=host,
                           port='5432'
                           )
    mnr = con.cursor()

    sql = f"""SELECT ST_Union(geom)
                    FROM {mnr_schema}.mnr_water_area
                    WHERE ST_Intersects(ST_Buffer(ST_MakeEnvelope({xmin}, {ymin}, 
                    {xmax}, {ymax}, 4326), 0.0005), geom);"""
    mnr.execute(sql)
    water_polygon = mnr.fetchone()[0]
    close_mnr = con.close()
    return water_polygon

def get_streetname_from_schema(grip_schema, field_to_check, language_field):

    sql = f"""SELECT internal_id, {field_to_check}, {language_field} FROM {grip_schema}.solite WHERE {field_to_check} IS NOT NULL AND {field_to_check} != ''"""
    
    pre = conn_guser.cursor()
    pre.execute(sql)
    response = pre.fetchall()
    
    return response

def get_routenumber_from_schema(grip_schema, field_to_check):

    sql = f"""SELECT internal_id, {field_to_check} FROM {grip_schema}.solite WHERE {field_to_check} IS NOT NULL AND {field_to_check} != ''"""
    
    pre = conn_guser.cursor()
    pre.execute(sql)
    response = pre.fetchall()
    
    return response

def get_water_count(mnr_schema, host, xmin, ymin, xmax, ymax):

    con = psycopg2.connect(database=db,
                           user=user,
                           password=passw,
                           host=host,
                           port='5432'
                           )
    mnr = con.cursor()

    sql =  f"""SELECT count(*)
                FROM {mnr_schema}.mnr_water_area
                WHERE ST_Intersects(ST_MakeEnvelope({xmin}, {ymin}, 
                {xmax}, {ymax}, 4326) , geom) = true;"""

    mnr.execute(sql)
    water_cnt = mnr.fetchone()[0]
    close_mnr = con.close()

    return water_cnt


def mnr_nw_names(schema, country, host):

    sql_q = """select feat_id as primary, feat_type, line_side, primary_name,
            string_agg((case when nametype='standard' then name end), ',') as st_name,
            string_agg((case when nametype='alternate' then name end), ',') as an_name,
            string_agg((case when nametype='RouteName' then name end), ',') as rt_name,
            string_agg((case when nametype='RouteNum' then name end), ',') as rt_num,
            string_agg((case when nametype='standard' then iso_lang_code end), ',') as st_isolc,
            string_agg((case when nametype='alternate' then iso_lang_code end), ',') as an_isolc,
            string_agg((case when nametype='RouteName' then iso_lang_code end), ',') as rt_isolc,
            string_agg((case when nametype='RouteNum' then iso_lang_code end), ',') as rtn_isolc,
            string_agg((case when nametype='standard' then lang_code end), ',') as st_lc,
            string_agg((case when nametype='alternate' then lang_code end), ',') as an_lc,
            string_agg((case when nametype='RouteName' then lang_code end), ',') as rt_lc,
            string_agg((case when nametype='RouteNum' then lang_code end), ',') as rtn_lc,
            string_agg((case when nametype='standard' then iso_script end), ',') as st_srpt,
            string_agg((case when nametype='alternate' then iso_script end), ',') as an_srpt,
            string_agg((case when nametype='RouteName' then iso_script end), ',') as rt_srpt,
            string_agg((case when nametype='RouteNum' then iso_script end), ',') as rtn_srpt,
            frc, n2c, fow, geom
            from
            (select xgr.feat_id, xgr.feat_type, mnn.line_side, xgr.name as primary_name,
            case
            when nt_standard=true then 'standard'
            when nt_alternate=true then 'alternate'
            when nt_route_name=true then 'RouteName'
            when nt_route_num=true then 'RouteNum'
            end nametype,
            mn.name as name, mn.iso_lang_code, mn.lang_code, mn.iso_script, display_class as frc, routing_class as n2c, form_of_way as fow, geom
            from {0}.x_geom_routes xgr
            left outer join {0}.mnr_netw2admin_area mnaa on mnaa.netw_id = xgr.feat_id
            left outer join {0}.mnr_netw2nameset mnn on mnn.netw_id = xgr.feat_id
            left outer join {0}.mnr_nameset2name mnn2 on mnn2.nameset_id = mnn.nameset_id
            left outer join {0}.mnr_name mn on mn.name_id = mnn2.name_id
            where mnaa.code in ('{1}')
            group by xgr.feat_id, xgr.feat_type, mnn.line_side, primary_name, mn.name, mnn.nt_standard, mnn.nt_alternate, mnn.nt_route_name, mnn.nt_route_num,
            mn.iso_lang_code, mn.lang_code, mn.iso_script, frc, n2c, fow, geom) as grouped
            group by feat_id, primary_name, line_side, feat_type, frc, n2c, fow, geom
                """.format(schema, country)
    engine_custom = create_engine('postgresql://' + user + ':' + passw + '@' + host + '/' + db)
    mnr_nw_n = gpd.read_postgis(sql_q, con=engine_custom)
    return mnr_nw_n


def mnr_street_name(schema, country, lang, host, polygon):
    con = psycopg2.connect(database=db,
                           user=user,
                           password=passw,
                           host=host,
                           port='5432'
                           )
    ara = con.cursor()
    sn = []
    sql = """SELECT DISTINCT b.name as street_name
                FROM {0}.mnr_nameset2name as a
                Join {0}.mnr_name as b ON a.name_id = b.name_id
                Join {0}.x_geopol_netw2nameset as c ON a.nameset_id = c.nameset_id
                Join {0}.x_geom_routes as d ON d.geo_id = c.netw_id
                WHERE ST_Intersects(d.geom, ST_GeomFromText('{1}', 4326)) AND country_left = '{2}' AND b.lang_code = '{3}'
                """.format(schema, polygon, country, lang)
    ara.execute(sql)
    rowsara = ara.fetchall()
    for i in rowsara:
        sn.append(i[0])
    close_mnr = con.close()
    return sn


def mnr_rt_number(schema, country, host, polygon):
    con = psycopg2.connect(database=db,
                           user=user,
                           password=passw,
                           host=host,
                           port='5432'
                           )
    ara = con.cursor()
    rt = []
    sql = """SELECT DISTINCT n.nc_routenumshield
                    FROM 
                      {0}.mnr_netw_geo_link as ngl,
                      {0}.mnr_netw_route_link as nrl,
                      {0}.mnr_netw2routenumset as n2rns,
                      {0}.mnr_routenumset as rns,
                      {0}.mnr_nameset as ns,
                      {0}.mnr_nameset2name as ns2n,
                      {0}.mnr_name as n
                    WHERE ngl.country_left = '{1}'
                    AND ST_Intersects(ngl.geom ,ST_GeomFromText('{2}', 4326))
                    AND ngl.feat_ID = nrl.netw_geo_id --geo link --> route link
                    AND nrl.feat_id = n2rns.netw_id --route link --> netw 2 route num set
                    AND n2rns.routeset_id = rns.routeset_id --netw 2 route num set --> route num set
                    AND rns.routenumset_id = ns.nameset_id --route num set --> name set
                    AND ns.nameset_id = ns2n.nameset_id --name set --> name set 2 name
                    AND ns2n.name_id = n.name_id --name set 2 name --> name
                """.format(schema, country, polygon)

    ara.execute(sql)
    rowsara = ara.fetchall()
    for i in rowsara:
        rt.append(i[0])
    close_mnr = con.close()
    return rt


def update_table_mnr(table, gdf, schema):
    try:
        gdf.to_postgis(table, engine_guser, schema, if_exists='append', geom_col='geom')
    except Exception as error:
        print(error)



def update_table(table, column, value):

    f = '(' + ','.join(column) + ')'

    for i in value:
        pre = conn_guser.cursor()
        query='''INSERT INTO grip.{0} {1} VALUES {2}'''.format(table, f, i)
        pre.execute('''INSERT INTO grip.{0} {1} VALUES {2};'''.format(table, f, i))
        conn_guser.commit()


def evaluation_update_status(success, source_km, mnr_km ,summary,filename,error_summary, potential_km):
    #print(success, source_km, mnr_km ,summary,filename,error_summary,potential_km)
    if success == 'yes':
        try:
            print(potential_km)
            now = datetime.now()
            date_time = now.strftime("%Y-%m-%d %H:%M:%S")
            #print(date_time)
            pre = conn_guser.cursor()
            query="""UPDATE grip_meta.evaluation_jobs SET status = 2,complete_date = '{0}',total_source_road = '{1}',total_ref_road = '{2}', summary='{4}',error_summary='{5}',potential_km='{6}' WHERE file_name = '{3}';""".format(date_time, source_km, mnr_km,filename,summary,error_summary,potential_km)
            print(query)
            pre.execute("""UPDATE grip_meta.evaluation_jobs SET status = 2,complete_date = '{0}',total_source_road = '{1}',total_ref_road = '{2}', summary='{4}',error_summary='{5}',potential_km='{6}' WHERE file_name = '{3}';""".format(date_time, source_km, mnr_km,filename,summary,error_summary,potential_km))
            conn_guser.commit()
        except:
            pre.execute("ROLLBACK")
            conn_guser.commit()


    else:
        try:
            now = datetime.now()
            date_time = now.strftime("%Y-%m-%d %H:%M:%S")
            #print(date_time)
            pre = conn_guser.cursor()
            query="""UPDATE grip_meta.evaluation_jobs SET status = 3,complete_date = '{0}' WHERE file_name = '{2}';""".format(date_time,summary,filename,error_summary)
            print(query)
            pre.execute("""UPDATE grip_meta.evaluation_jobs SET status = 3,complete_date = '{0}' ,summary='{1}',error_summary='{3}' WHERE file_name = '{2}';""".format(date_time,summary,filename,error_summary))
            conn_guser.commit()
        except:
            pre.execute("ROLLBACK")
            conn_guser.commit()



def mnr_server_info(country):

    ara = conn_guser.cursor()
    ara.execute("""SELECT mnr_schema, mnr_server, mnr_server2
                    FROM grip_meta.grip_global
                    WHERE isocode = '{0}'
                """.format(country))

    mnr_server = ara.fetchone()
    return mnr_server

def update_attribute_mapping(mappingfilename,userid,filename,solite_column_count,replaced_column_count):
    #date_time = datetime.datetime.now()
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(date_time)
    pre = conn_guser.cursor()
    pre.execute("""UPDATE grip.attributemapping_json SET mappingfilename='{0}',mapping_date = '{1}',sourceattributes={4},mappingattributes={5} WHERE sourcefilename = '{2}' and userid='{3}';""".format(mappingfilename,date_time,filename,userid,solite_column_count,replaced_column_count))
    conn_guser.commit()
    print("Attribute mapping database update completed.")

#-----------------------------Get job details--------------------------------- 
def get_job_details(filename, job_type):
    """Get the details of the uploaded source from the jobs table"""
    schema = "grip_meta"
    table = job_type + "_jobs"
    fields = "country_iso, schema_name, job_id, userid, bufferdistance, lang1, lang2, selectedlayer, summary, run_type"
    sql = "SELECT " + fields
    sql += " FROM " + schema + "." + table
    sql += " WHERE file_name = " +"'"+ str(filename) +"'"+ ";"
    pre = conn_guser.cursor()
    pre.execute(sql)
    schema = pre.fetchone()
    return schema

#-----------------------------Get job details based on schema name--------------------------------- 
def get_job_details_schema(schemaname, job_type):
    """Get the details of the uploaded source from the jobs table"""
    schema = "grip_meta"
    table = job_type + "_jobs"
    fields = "country_iso, file_name, job_id, userid, bufferdistance,selectedlayer,run_type"
    sql = "SELECT " + fields
    sql += " FROM " + schema + "." + table
    sql += " WHERE schema_name = " +"'"+ str(schemaname) +"'"+ ";"
    pre = conn_guser.cursor()
    pre.execute(sql)
    schema = pre.fetchone()
    return schema


def get_solite_centroid(schema):
    sql = """SELECT ST_AsText(ST_PointOnSurface(ST_Collect(geom)))
                FROM {0}.solite
            """.format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    centroid = pre.fetchone()[0]
    return centroid

def get_utm(pt):
    sql = "SELECT utm_zone FROM grip_meta.grip_global as gbl WHERE ST_Intersects(gbl.geom, ST_GeomFromText('" + pt + "',4326))"
    pre = conn_guser.cursor()
    pre.execute(sql)
    utm_zone = pre.fetchone()[0]
    return utm_zone

def get_solite_convex_hull(schema):
    sql = """SELECT ST_AsText(ST_ConvexHull(ST_Collect(geom)))
                FROM {0}.solite
            """.format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    convex_hull = pre.fetchone()[0]
    return convex_hull


# get road and attribute lengths
def get_solite_km(schema):
    """Get total road length of loaded source from solite table"""
    sql = """SELECT SUM(length)
                FROM {0}.solite
            """.format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    total_km = pre.fetchone()[0]/1000
    return total_km

def get_mnr_km(schema):
    """Get total road length of MN-R from ref_roads_staging table"""
    sql = """SELECT SUM(centimeters)
                FROM {0}.ref_roads_staging
            """.format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    total_km = pre.fetchone()[0]/100000
    return total_km

# Create ref_roads_staging table to insert data from mnr of osm for processing only
def create_ref_roads_staging(schema):
    sql = "CREATE TABLE IF NOT EXISTS " + schema + ".ref_roads_staging"
    sql += "(feat_id uuid"
    sql += ",feat_type integer"
    sql += ",ferry_type character varying(15)"
    sql += ",name character varying(200)"
    sql += ",lang_code character varying(3)"
    sql += ",back_road integer"
    sql += ",stubble boolean"
    sql += ",display_class integer"
    sql += ",routing_class character varying(10)"
    sql += ",form_of_way integer"
    sql += ",freeway boolean"
    sql += ",road_condition integer"
    sql += ",restricted_access integer"
    sql += ",part_of_structure integer"
    sql += ",simple_traffic_direction integer"
    sql += ",centimeters integer"
    sql += ",geom geometry(Linestring,4326))"
    sql += "; CREATE INDEX IF NOT EXISTS ref_roads_staging_uuid_idx ON " + schema + ".ref_roads_staging USING btree (feat_id)"
    sql += "; CREATE INDEX IF NOT EXISTS ref_roads_staging_geom_idx ON " + schema + ".ref_roads_staging USING gist (geom);"
    pre = conn_guser.cursor()
    pre.execute(sql)
    conn_guser.commit()

# Drop ref_roads_staging table
def drop_ref_roads_staging(schema):
    sql = "DROP TABLE IF EXISTS " + schema + ".ref_roads_staging"
    pre = conn_guser.cursor()
    pre.execute(sql)
    conn_guser.commit()

# Insert the mnr road geometry of the scope fetched from the mnr server to the ref_staging table
def insert_ref_staging(schema, data):
    table = "ref_roads_staging"
    records_list_template = ','.join(['%s'] * len(data))
    sql = "INSERT INTO " + schema + "." + table + " VALUES {}".format(records_list_template)
    pre = conn_guser.cursor()
    pre.execute(sql, data)
    conn_guser.commit()

# Get the too short (<3.5m) geometry from the solite table
def get_too_short_geo(schema):
    try:
        sql = """SELECT internal_id, ST_Length(geom::geography) AS length
            FROM {0}.solite
            WHERE ST_Length(geom::geography) <3.5""".format(schema)
        pre = conn_guser.cursor()
        pre.execute(sql)
        too_short = pre.fetchall()
        return too_short
    except Exception as e:
        print(e)

# Get the too short dangles(between 3.5 and 8.5m (shorter than 3.5m will be detected by too short check) geometry from the solite table
def get_too_short_dangles(schema):
    sql = """SELECT internal_id, ST_Length(geom::geography) AS length
            FROM {0}.solite a
            WHERE (NOT EXISTS
                (SELECT 1 FROM {0}.solite b 
                WHERE a.internal_id != b.internal_id
                AND (ST_Distance(ST_StartPoint(a.geom), b.geom) < 0.00000008))
                OR NOT EXISTS (SELECT 1 FROM {0}.solite c 
                WHERE a.internal_id != c.internal_id
                AND (ST_Distance(ST_EndPoint(a.geom), c.geom) < 0.00000008)))
            AND ST_Length(geom::geography) BETWEEN 3.5 AND 8.5""".format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    too_short_dangles = pre.fetchall()
    return too_short_dangles

# Get duplicate geometry from the solite table
def get_duplicate_geo(schema):
    sql = """SELECT * FROM (
                SELECT internal_id, ST_Length(geom::geography) AS length, ROW_NUMBER() OVER(PARTITION BY ST_AsBinary(geom) ORDER BY internal_id ASC) AS row,
                geom FROM ONLY {0}.solite)
                dups WHERE dups.row > 1""".format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    duplicate = pre.fetchall()
    return duplicate

# Get (partly) overlapping and duplicate geometry from the solite table
def get_overlapping_geo(schema):
    sql = """SELECT a.internal_id AS id1, b.internal_id AS id2, ST_Length(ST_INTERSECTION(a.geom, b.geom)::geography) AS length
            FROM {0}.solite a
            INNER JOIN {0}.solite b ON a.internal_id <> b.internal_id
            WHERE ST_CONTAINS(a.geom,b.geom) OR ST_OVERLAPS(a.geom,b.geom)""".format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    duplicate = pre.fetchall()
    return duplicate

# Get self-intersecting geometry from the solite table
def get_self_intersecting_geo(schema):
    sql = """SELECT internal_id, ST_Length(geom::geography) AS length
            FROM {0}.solite
            WHERE ST_IsSimple(geom) = false OR ST_IsRing(geom) = true""".format(schema)
    pre = conn_guser.cursor()
    pre.execute(sql)
    self_intersect = pre.fetchall()
    return self_intersect

# Get water intersecting geometry from the solite table
def get_water_intersecting_geo(schema, water_polygon, xmin, ymin, xmax, ymax):
    sql = f"""SELECT internal_id, length
                FROM {schema}.solite
                WHERE ST_Intersects(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), geom)
                AND ST_Intersects(geom, '{water_polygon}');"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    self_intersect = pre.fetchall()
    return self_intersect

# Get geometry to snap (<7.5m) with from the solite table
def get_snapping(grip_schema, xmin, ymin, xmax, ymax):

    geometry_dictionary_start = {}
    geometry_dictionary_end = {}

    try:
        # First filter, get all lines with length more than 7.5 meters
        sql = f"""SELECT internal_id, ST_Endpoint(geom), ST_Startpoint(geom), ST_Buffer(ST_Endpoint(geom)::geography, 7.5, 8), ST_Buffer(ST_Startpoint(geom)::geography, 7.5, 8)
        FROM {grip_schema}.solite a
        WHERE ST_Length(geom::geography) > 7.5 AND ST_Intersects(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), geom)
        AND (NOT EXISTS
                (SELECT 1 FROM {grip_schema}.solite b 
                WHERE a.internal_id != b.internal_id
                AND ST_Intersects(ST_Buffer(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), 0.005), b.geom)
                AND (ST_Distance(ST_StartPoint(a.geom), b.geom) < 0.0000000000000001))
                OR NOT EXISTS (SELECT 1 FROM {grip_schema}.solite c 
                WHERE a.internal_id != c.internal_id
                AND ST_Intersects(ST_Buffer(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), 0.005), c.geom)
                AND (ST_Distance(ST_EndPoint(a.geom), c.geom) < 0.0000000000000001)))
        """

        pre = conn_guser.cursor()
        pre.execute(sql)
        snapping_points = pre.fetchall()

        entries = 0

        # Second filter, get all lines that are within 7.5 meters of the current line ending or starting point, update the dictionary
        for snapping_point in snapping_points:
            
            sql_startpoint = f"""SELECT internal_id, ST_Distance('{snapping_point[1]}'::geography, geom::geography) AS distance
                FROM {grip_schema}.solite
                WHERE ST_Intersects('{snapping_point[3]}', geom) AND internal_id <> '{snapping_point[0]}'
                AND ST_Intersects(ST_Buffer(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), 0.05), geom)
                ORDER BY distance DESC
            """

            sql_endpoint = f"""SELECT internal_id, ST_Distance('{snapping_point[2]}'::geography, geom::geography) AS distance
                FROM {grip_schema}.solite
                WHERE ST_Intersects('{snapping_point[4]}', geom) AND internal_id <> '{snapping_point[0]}'
                AND ST_Intersects(ST_Buffer(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), 0.05), geom)
                ORDER BY distance DESC
            """

            pre.execute(sql_startpoint)
            startpoint_snapping = pre.fetchall()
            pre.execute(sql_endpoint)
            endpoint_snapping = pre.fetchall()
            entries += 1

            #third filter, make sure all lines that are within 7.5 meters of the start/end point have a distance larger than 0
            startpoint_issue = 0
            if len(startpoint_snapping) != 0:
                startpoint_issue = 1
                for invalid_snap in startpoint_snapping:
                    if invalid_snap[1] == 0.0:
                        startpoint_issue = 0
                        break
                if startpoint_issue == 1:
                    geometry_dictionary_start[snapping_point[0]] = invalid_snap
            endpoint_issue = 0
            if len(endpoint_snapping) != 0:
                endpoint_issue = 1
                for invalid_snap in endpoint_snapping:
                    if invalid_snap[1] == 0.0:
                        endpoint_issue = 0
                        break
                if endpoint_issue == 1:
                    geometry_dictionary_end[snapping_point[0]] = invalid_snap

        #print("Finished fetching snapping points")
    except Exception as E:
        print(E)

    return geometry_dictionary_start, geometry_dictionary_end

# Insert the high level results of a check to the meta table
def insert_meta(schema, rule, status, error_count, meters, start_time, end_time, processing_time, job_id):
    table = "meta"
    columns = "(check_id, status, failed, failed_meter, start_time, end_time, processing_time, job_id)"
    values = "(" + str(rule) + ",'" + status + "'," + str(error_count) + "," + "{:.2f}".format(meters) + ",'" + start_time + "','" + end_time + "','" + processing_time + "'," + str(job_id) + ")"
    sql = "INSERT INTO " + schema + "." + table + columns + " VALUES " + values
    pre = conn_guser.cursor()
    pre.execute(sql)
    conn_guser.commit()

# Update quality table with the results of the check for a feature
def update_quality(schema, comment, check, internal_id):
    try:
        sql = "UPDATE " + schema + ".quality SET " + check + " = 0"
        if comment != "":
            sql += ", comment = CONCAT(comment, '" + comment + ";')"
        sql += " WHERE internal_id = '" + str(internal_id) + "';"
        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as e:
        print(e)

def update_quality_test(comment, check, internal_id):
    try:
        sql = "UPDATE " + "evaluation_nld_414_test.quality SET " + check + " = 0"
        if comment != "":
            sql += ", comment = CONCAT(comment, '" + comment + ";')"
        sql += " WHERE internal_id = '" + str(internal_id) + "';"
        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as e:
        print(e)

# Get solite data and load into dataframe
def solite_from_postgis(schema):
    fields = "internal_id, featuretype, ferrytype, frc, formofway, net2class, backroad, freeway, restrictedaccess, roadcondition, dtfr, streetname1, streetname1_lng, streetname2, streetname2_lng, streetname3, streetname3_lng, streetname4, streetname4_lng, routenumber1, routenumber1_type, routenumber1_lng, routenumber2, routenumber2_type, routenumber2_lng, routenumber3, routenumber3_type, routenumber3_lng, mqslevel, length, geom"
    sql = "SELECT " + fields
    sql += " FROM " + schema + ".solite"
    solite_dataframe = gpd.GeoDataFrame.from_postgis(sql, con=engine_guser)
    return solite_dataframe

# Get bbox for solite data
def get_bbox(schema):
    try:
        sql = f"""SELECT
                                min(ST_XMin(geom)),
                                min(ST_YMin(geom)),
                                max(ST_XMax(geom)),
                                max(ST_YMax(geom))
                                FROM {schema}.solite;"""
        pre = conn_guser.cursor()
        pre.execute(sql)
        bounding_box = pre.fetchall()
        return bounding_box
    except Exception as e:
        print(e)

#Get spatial matched solite data against reference data (mnr or osm) based on solite centerpoint of roadline
def get_ref_geometry_match(schema, buffer, xmin, ymin, xmax, ymax):
    sql = f"""SELECT DISTINCT ON (s.internal_id) s.internal_id, s.length, s.streetname1, r.name, s.dtfr, r.simple_traffic_direction, s.roadcondition, r.road_condition, s.backroad, r.back_road, s.restrictedaccess, r.restricted_access
                FROM {schema}.solite AS s
                INNER JOIN {schema}.ref_roads_staging AS r
                ON ST_DWithin(r.geom::geography, ST_LineInterpolatePoint(s.geom, 0.50)::geography, {buffer}, false)
                WHERE ST_Intersects(ST_Buffer(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), 0.0005), r.geom)
                AND ST_Intersects(ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax},4326), s.geom)
                ORDER BY s.internal_id, ST_Distance(r.geom, ST_LineInterpolatePoint(s.geom, 0.50)) ASC"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    matched_geometry = pre.fetchall()
    return matched_geometry

#Get unmatched solite data against reference data (mnr or osm)
def get_unmatched_solite_geometry(schema, internal_ids):
    sql = f"""SELECT internal_id, length, streetname1
                FROM {schema}.solite
                WHERE internal_id IN {str(internal_ids)}"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    unmatched_geometry = pre.fetchall()
    return unmatched_geometry

#Get solite internal_ids
def get_solite_internal_ids(schema):
    sql = f"""SELECT internal_id FROM {schema}.solite"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    solite_internal_ids = pre.fetchall()
    return solite_internal_ids

def get_mnr_12_schema(host, country_iso, centroid):

    con = psycopg2.connect(database=db,
                           user=user,
                           password=passw,
                           host=host,
                           port='5432'
                           )
    mnr = con.cursor()

    if country_iso == 'USA':
        try:
            # get latest schema name
            state_code = get_state_code(centroid)
            if state_code == 'MP':
                state_code = 'MNP'
            elif state_code == 'AS':
                state_code = 'ASM'
            else:
                state_code = 'U' + state_code
            sql = "SELECT MAX(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + state_code.lower() + "%';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            #check or data is avaialble else except
            sql = "SELECT feat_id"
            sql += " FROM " + mnr_schema +".mnr_admin_area"
            sql += " LIMIT 1;"
            mnr.execute(sql)
            mnr.fetchone()
            return mnr_schema
        except:
            mnr.execute("ROLLBACK")
            con.commit()
            state_code = get_state_code(centroid)
            if state_code == 'MP':
                state_code = 'MNP'
            elif state_code == 'AS':
                state_code = 'ASM'
            else:
                state_code = 'U' + state_code
            sql = "SELECT MIN(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + state_code.lower() + "%';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            return mnr_schema

    elif country_iso == 'CAN':
        try:
            # get latest schema name
            state_code = get_state_code(centroid)
            state_code = 'C' + state_code
            sql = "SELECT MAX(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + state_code.lower() + "%';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            #check or data is avaialble else except
            sql = "SELECT feat_id"
            sql += " FROM " + mnr_schema +".mnr_admin_area"
            sql += " LIMIT 1;"
            mnr.execute(sql)
            mnr.fetchone()
            return mnr_schema
        except:
            mnr.execute("ROLLBACK")
            con.commit()
            state_code = get_state_code(centroid)
            state_code = 'C' + state_code
            sql = "SELECT MIN(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + state_code.lower() + "%';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            return mnr_schema
    elif country_iso in ['ISR', 'IND', 'PAK', 'SYR', 'CHN']:
        try:
            # get latest schema name
            sql = "SELECT MAX(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + country_iso.lower() + "_" + country_iso.lower() + "';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            #check or data is avaialble else except
            sql = "SELECT feat_id"
            sql += " FROM " + mnr_schema +".mnr_admin_area"
            sql += " LIMIT 1;"
            mnr.execute(sql)
            mnr.fetchone()
            return mnr_schema
        except:
            mnr.execute("ROLLBACK")
            con.commit()
            sql = "SELECT MIN(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + country_iso.lower() + "_" + country_iso.lower() + "';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            return mnr_schema
    else:
        try:
            # get latest schema name
            sql = "SELECT MAX(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + country_iso.lower() + "%';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            #check or data is avaialble else except
            sql = "SELECT feat_id"
            sql += " FROM " + mnr_schema +".mnr_admin_area"
            sql += " LIMIT 1;"
            mnr.execute(sql)
            mnr.fetchone()
            return mnr_schema
        except:
            mnr.execute("ROLLBACK")
            con.commit()
            sql = "SELECT MIN(nspname)"
            sql += " FROM pg_catalog.pg_namespace"
            sql += " WHERE nspname LIKE '%" + country_iso.lower() + "_" + country_iso.lower() + "%';"
            mnr.execute(sql)
            mnr_schema = mnr.fetchall()[0][0]
            return mnr_schema

def get_state_code(centroid):
    sql = "SELECT state_code FROM grip_meta.name_state_codes as sc WHERE ST_Intersects(sc.geom, ST_GeomFromText('" + centroid + "',4326))"
    pre = conn_guser.cursor()
    pre.execute(sql)
    state_code = pre.fetchone()[0]
    return state_code

def get_preact_intersection(grip_schema, list_of_geometry):

    preact_intersections = []
    pre = conn_guser.cursor()

    for geometry in list_of_geometry:
        sql = f"""SELECT internal_id, length, geom
                    FROM {grip_schema}.solite 
                    WHERE ST_Intersects(geom, 'SRID=4326;{geometry}');"""
        pre.execute(sql)
        for i in pre.fetchall():
            entity = [i[0], i[1]]
            preact_intersections.append(entity)
    return preact_intersections

def get_azimuth(currentPoint, previousPoint):
    sql = f"""SELECT degrees(ST_Azimuth(ST_Point({currentPoint[0]}, {currentPoint[1]}), ST_Point({previousPoint[0]}, {previousPoint[1]}))) AS degA_B"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    azimuth = pre.fetchone()[0]
    return azimuth

def get_solite_count(schema):
    sql = f"""SELECT COUNT(internal_id) FROM {schema}.solite"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    solite_count = pre.fetchone()[0]
    return solite_count

#-----------------------------Get street name uuid(s) from solite table--------------------------------- 
def get_streetname_uuid(grip_schema, streetname, column):
    """Get the uuid's for a given street name from the solite table"""
    sql = "SELECT internal_id"
    sql += " FROM " + grip_schema + ".solite"
    sql += " WHERE " + column + " = '" + streetname.replace("'", "''") + "'"
    pre = conn_guser.cursor()
    pre.execute(sql)
    uuid = pre.fetchall()
    return uuid

#-----------------------------Get street name uuid(s) from solite table--------------------------------- 
def get_transliterated_streetname(grip_schema, transliteration_streetname_column, streetname, streetname_column):
    """Get the uuid's for a given street name from the solite table"""
    sql = "SELECT " + transliteration_streetname_column
    sql += " FROM " + grip_schema + ".solite"
    sql += " WHERE " + streetname_column + " = '" + streetname.replace("'", "''") + "'"
    sql += " GROUP BY " + transliteration_streetname_column
    pre = conn_guser.cursor()
    pre.execute(sql)
    transliterated_name = pre.fetchone()
    return transliterated_name

#-----------------------------Insert updated street name(s) into attributes_norm table--------------------------------- 
def insert_streetname_norm(grip_schema, updated_names):
    """Insert updated street name(s) into the attributes_norm table"""
    sql = f"""INSERT INTO {grip_schema}.attributes_norm (internal_id, component, original, normalized, language_notation)
                                                VALUES {updated_names};"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    conn_guser.commit()

#-----------------------------Update solite street name(s) based on attributes_norm table--------------------------------- 
def update_solite_streetname(grip_schema, column):
    """Update solite street name(s) based on attributes_norm table"""
    sql_update = f"UPDATE {grip_schema}.solite SET {column} = {grip_schema}.attributes_norm.normalized  "
    sql_update += f"FROM {grip_schema}.attributes_norm "
    sql_update += f"WHERE {grip_schema}.solite.internal_id = {grip_schema}.attributes_norm.internal_id AND {grip_schema}.attributes_norm.component = '{column}'"
    pre = conn_guser.cursor()
    pre.execute(sql_update)
    conn_guser.commit()

#-----------------------------Update solite street name(s) based on attributes_norm table--------------------------------- 
def update_solite_transliterated_streetname(grip_schema, column, language_column, type_column):
    """Update solite street name(s) based on attributes_norm table"""
    sql_update = f"UPDATE {grip_schema}.solite SET {column} = {grip_schema}.attributes_norm.normalized, {language_column} = {grip_schema}.attributes_norm.language_notation, {type_column} = 'StandardName' "
    sql_update += f"FROM {grip_schema}.attributes_norm "
    sql_update += f"WHERE {grip_schema}.solite.internal_id = {grip_schema}.attributes_norm.internal_id AND {grip_schema}.attributes_norm.component = '{column}'"
    pre = conn_guser.cursor()
    pre.execute(sql_update)
    conn_guser.commit()

# Update quality table when a feature is normalized
def update_quality_norm(schema, check, internal_id):
    try:
        sql = "UPDATE " + schema + ".quality SET " + check + " = 2"
        sql += " WHERE internal_id IN (" + str(internal_id) + ");"
        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as e:
        print(e)

# Update quality table when a feature is for review
def update_quality_review(schema, comment, check, internal_id):
    try:
        sql = "UPDATE " + schema + ".quality SET " + check + " = 3"
        if comment != "":
            sql += ", comment = CONCAT(comment, '" + comment + ";')"
        sql += " WHERE internal_id = '" + str(internal_id) + "';"
        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as e:
        print(e)

#-----------------------------Get attribute mapping details--------------------------------- 
def get_streetname_attribute_mapping(filename):
    """Get the atribute mapping details of the uploaded source from the attributemapping table"""
    table = "grip_meta.attributemapping"
    fields = "streetname1_lng,streetname2_lng,streetname3_lng,streetname4_lng"
    sql = "SELECT " + fields
    sql += " FROM " + table
    sql += " WHERE sourcefilename = " +"'"+ str(filename) +"'"+ ";"
    pre = conn_guser.cursor()
    pre.execute(sql)
    row = pre.fetchall()
    return row

def get_start_and_end_point_for_path(grip_schema, ids, streetname_column):

    sql = f"""SELECT ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), ST_X(ST_PointN(geom, 2)), ST_Y(ST_PointN(geom, 2)), ST_X(ST_PointN(geom, -2)), ST_Y(ST_PointN(geom, -2)), internal_id, {streetname_column} FROM {grip_schema}.solite WHERE internal_id IN {ids}"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    point_list = pre.fetchall()
    return point_list

def get_unique_street_names_order8(grip_schema, streetname):

    sql = f"""SELECT {streetname}, {streetname + '_lng'}, admin8code
		        FROM {grip_schema}.solite
		        WHERE {streetname} IS NOT NULL
                GROUP BY {streetname}, {streetname + '_lng'}, admin8code"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    names = pre.fetchall()
    return names

def streetname_chain_query(grip_schema, streetname_column, streetname, admin8code):
    """Check for street name continuity for a given street name. When a street name has gaps it will have multiple groups of road elements (chains) and write it to the chains table for further processing"""
    try:
        sql = f"""
            DO
            $$
            DECLARE
            this_id character varying (50);
            this_geom geometry;
            this_sn character varying(100);
            chain_id_match integer;
            id_a bigint;
            id_b bigint;
            BEGIN
            -- Create a chain of all geometries for a street name --
            DROP TABLE IF EXISTS {grip_schema}.chains;
            CREATE TABLE {grip_schema}.chains (chain_id serial, ids character varying[], sn character varying[], geom geometry);
            CREATE INDEX ON {grip_schema}.chains USING GIST(geom);
            FOR this_id, this_sn, this_geom IN SELECT internal_id, {streetname_column}, geom FROM {grip_schema}.solite WHERE {streetname_column} = '{streetname}' AND admin8code = '{admin8code}' AND (formofway != 4 OR formofway IS NULL) LOOP
            SELECT chain_id FROM {grip_schema}.chains WHERE ST_Intersects(this_geom, chains.geom)
                LIMIT 1 INTO chain_id_match;
            IF chain_id_match IS NULL THEN
                INSERT INTO {grip_schema}.chains (ids, sn, geom) VALUES (ARRAY[this_id], ARRAY[this_sn], this_geom);
            ELSE
                UPDATE {grip_schema}.chains SET geom = ST_Union(this_geom, geom),
                                    ids = array_prepend(this_id, ids),
                                    sn = array_prepend(this_sn, sn)
                WHERE chains.chain_id = chain_id_match;
            END IF;
            END LOOP;
            LOOP
                SELECT a.chain_id, b.chain_id FROM {grip_schema}.chains a, {grip_schema}.chains b 
                WHERE ST_Intersects(a.geom, b.geom)
                AND a.chain_id < b.chain_id
                INTO id_a, id_b;
                EXIT WHEN id_a IS NULL;
                UPDATE {grip_schema}.chains a SET geom = ST_Union(a.geom, b.geom), ids = array_cat(a.ids, b.ids), sn = array_cat(a.sn, b.sn)
                FROM {grip_schema}.chains b
                WHERE a.chain_id = id_a AND b.chain_id = id_b;
                DELETE FROM {grip_schema}.chains WHERE chain_id = id_b;
            END LOOP;
            END;
            $$ language plpgsql;
            """

        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()

        # Get streetname chain(s)
        sql = f"""SELECT ids, sn, ST_Length(geom::geography) AS length, geom, chain_id FROM {grip_schema}.chains"""
        pre.execute(sql)
        streetname_chains = pre.fetchall()

        # Drop chains table
        sql = f"""DROP TABLE IF EXISTS {grip_schema}.chains"""
        pre.execute(sql)
        conn_guser.commit()

    except Exception as E:
        print(E)

    return streetname_chains

def create_streetname_nodes_view(grip_schema):
    """Create view to get the start and end point (nodes) of street name geometry"""
    try:
        sql = f"""
            CREATE OR REPLACE VIEW {grip_schema}.solite_nodes AS
            SELECT internal_id, streetname1, streetname1_lng, streetname2, streetname2_lng, streetname3, streetname3_lng, streetname4, streetname4_lng, ST_Startpoint(geom) AS startpoint, ST_Endpoint(geom) AS endpoint, length, geom
            FROM {grip_schema}.solite;
            """

        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as e:
        print(e)

def create_streetname_nodes(grip_schema):
    """Create nodes table based on nodes view to be able to create network table for route calculation"""
    try:
        sql = f"""
            CREATE TABLE {grip_schema}.node AS
            SELECT row_number() OVER (ORDER BY foo.p)::integer AS id, 
            foo.p AS the_geom
            FROM (         
                SELECT DISTINCT solite_nodes.startpoint AS p FROM {grip_schema}.solite_nodes
                UNION
                SELECT DISTINCT solite_nodes.endpoint AS p FROM {grip_schema}.solite_nodes
                ) foo
            GROUP BY foo.p;
            """

        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as E:
        print(E)

def create_streetname_network(grip_schema):
    """Create network table based on node table and solite_nodes view for route calculation"""
    try:
        sql = f"""
            CREATE TABLE {grip_schema}.network AS
            SELECT a.*, b.id as start_id, c.id as end_id
            FROM {grip_schema}.solite_nodes AS a
            JOIN {grip_schema}.node AS b ON a.startpoint = b.the_geom
            JOIN {grip_schema}.node AS c ON a.endpoint = c.the_geom;
            ALTER TABLE {grip_schema}.network
            ADD id serial;
            """

        pre = conn_guser.cursor()
        pre.execute(sql)
        conn_guser.commit()
    except Exception as E:
        print(E)

def calculate_streetname_path(grip_schema, start_node, end_node, directional):
    """Calculate street name path for given street name gap based on start node and end node"""
    try:
        sql = f"""
            SELECT * 
            FROM {grip_schema}.network
            JOIN
                (SELECT * FROM pgr_dijkstra('
                    SELECT id, 
                    start_id::int4 AS source, 
                    end_id::int4 AS target, 
                    length::float8 AS cost
                    FROM {grip_schema}.network',
                {start_node},
                {end_node},
                {directional})) AS route
            ON
            network.id = route.edge;
            """
        pre = conn_guser.cursor()
        pre.execute(sql)
        streetname_path = pre.fetchall()
        return streetname_path
    except Exception as E:
        print(E)

def check_streetname_network_table(grip_schema):
    """Check or network table already exist"""
    try:
        sql = f"""
            SELECT * 
            FROM {grip_schema}.network
            LIMIT 1;
            """
        pre = conn_guser.cursor()
        pre.execute(sql)
        record = pre.fetchone()
        exist = 1
        return exist
    except Exception as E:
        pre.execute("ROLLBACK")
        exist = 0
        return exist

def get_node_id(grip_schema, id):
    """Get node id of the chain used for calculating the path"""
    try:
        sql = f"""
            SELECT start_id, startpoint, end_id, endpoint FROM {grip_schema}.network
            WHERE internal_id = '{id}';
            """
        pre = conn_guser.cursor()
        pre.execute(sql)
        start_node = pre.fetchone()
        return start_node
    except Exception as E:
        print(E)

#Get the distance in meters between 2 points
def get_distance(point1, point2):
    sql = f"""SELECT ST_Distance('{point1}'::geography, '{point2}'::geography)"""
    pre = conn_guser.cursor()
    pre.execute(sql)
    distance = pre.fetchall()
    return distance

# Get dtfr from the solite table
def get_dtfr_of_geometry(grip_schema):
    entities_with_dtfr = {}

    sql = f"""SELECT internal_id, ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), ST_AsText(geom), dtfr, ST_AsText(ST_LineInterpolatePoint(geom, 0.5)) FROM {grip_schema}.solite WHERE dtfr IS NOT NULL and FOW != 4"""

    pre = conn_guser.cursor()
    pre.execute(sql)
    
    result = pre.fetchall()

    for i in result:
        entities_with_dtfr[i[0]] = (i[1], i[2], i[3], i[4], i[5], i[6], i[7])
        
    return entities_with_dtfr 

# Get dtfr and relevant attributes from mnr table
def get_mnr_attributes(grip_schema):

    sql = f"""SELECT feat_id, ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), ST_AsText(geom),
            (SELECT CASE 
                WHEN simple_traffic_direction = 9 THEN 'InBothDirections'
                WHEN simple_traffic_direction = 3 THEN 'InPositiveDirection'
                WHEN simple_traffic_direction = 2 THEN 'InNegativeDirection'
                ELSE NULL
                END simple_traffic_direction)
            , ST_AsText(ST_LineInterpolatePoint(geom, 0.5)) FROM {grip_schema}.ref_roads_staging
            WHERE simple_traffic_direction != 1"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    result = pre.fetchall()

    return result

# Get probe data of
def get_probe_data(grip_schema):
    sql = f"""SELECT direction, ST_AsText(geom) from {grip_schema}.probe"""

    pre = conn_guser.cursor()
    pre.execute(sql)

    result = pre.fetchall()

    return result

#Get Standard Road Edge or Ferry Edge has Maximum Total Height Allowed
# -------function--------
# def getMaximumAllowedHeigth(country_code):
#         try:
#             conn = psycopg2.connect(database=ydb,
#                                         user=yuser,
#                                         password=ypassw,
#                                         host=yhost,
#                                         port=yport
#                                         )
#             yard = conn.cursor()

#             schema = "yard_cpp_2"
#             sql = "SELECT "
#             sql += "s.ScopeAdminKey AS country_code"
#             sql += ",c1.valuestring AS unit_of_measurement"
#             sql += ",c2.valuelong AS lower_limit"
#             sql += ",c3.valuelong AS upper_limit"
#             sql += ",c4.valuelong AS lower_limit_motorways"
#             sql += ",c5.valuestring AS allowed_on_motorways"
#             sql += ",lt.typename"
#             sql += " FROM " + schema + ".REFSCOPE s"
#             sql += " LEFT JOIN " + schema + ".REFSCOPEMETADATA sm ON s.ScopeId = sm.ScopeId"
#             sql += " LEFT JOIN " + schema + ".REFMETADATA m       ON sm.MetaDataId = m.MetaDataId"
#             sql += " LEFT JOIN " + schema + ".REFENTITYTYPE lt    ON m.MetaDataTypeId = lt.TypeId"
#             sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c1   ON m.MetaDataId = c1.ListId AND c1.listcolumntypeid = 572"
#             sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c2   ON m.MetaDataId = c2.ListId AND c2.listcolumntypeid = 570"
#             sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c3   ON m.MetaDataId = c3.ListId AND c3.listcolumntypeid = 573"
#             sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c4   ON m.MetaDataId = c4.ListId AND c4.listcolumntypeid = 571"
#             sql += " LEFT JOIN " + schema + ".REFLISTELEMENT c5   ON m.MetaDataId = c5.ListId AND c5.listcolumntypeid = 569"
#             sql += " WHERE lt.typeid = 568"
#             sql += " AND m.versionminor = (SELECT Max(versionminor) FROM " + schema + ".REFMETADATA vm"
#             sql += " INNER JOIN " + schema + ".REFSCOPEMETADATA vsm ON vm.metadataid = vsm.metadataid"
#             sql += " WHERE vsm.ScopeId = s.scopeId AND vm.metadatatypeid = m.metadatatypeid)"
#             sql += " AND s.ScopeAdminKey ilike '" + country_code + "'"
#             yard.execute(sql)
#             rows = yard.fetchall()
#             conn.close()
#             return rows
#         except:
#             conn.close()
#             print('Getting allowed Maximum Total Height from YARD failed')

#close GRIP database connections
def close_db_connections():
    close_grip = conn_guser.close()
    print('Database connections closed')
