# # -*- coding: utf-8 -*-
import sys

sys.path.append(r'C:\Users\phatthie\OneDrive - TomTom\Desktop\Coding Project\sourcing-grip-development\sourcing-grip\source-code\api')
# use system path

# from matplotlib.font_manager import json_dump
from qualitychecks.database import getAllowedLanguageCode,evaluation_update_status, get_utm, mnr_server_info, get_solite_centroid, get_solite_convex_hull, get_job_details, mnr_network, mnr_nw_names, update_table, update_table_mnr, get_solite_km, create_ref_roads_staging,insert_ref_staging, get_mnr_km, solite_from_postgis, insert_meta

import geopandas as gpd
import pandas as pd
from pyproj import CRS
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from datetime import datetime
from shapely.ops import split, snap, unary_union
from qualitychecks.checks.geometry_toshort import too_short_geometry_wrapper
# from qualitychecks.checks.fuzzyNameMatch import fuzzyNameMatch,fuzzy_wrapper
from qualitychecks.checks.snapping import snapping_wrapper
# from qualitychecks.checks.frc import frcCheck,frc_wrapper
from qualitychecks.checks.water import water_intersecting, water_wrapper
# from qualitychecks.checks.sharp_angle import sharpAngle,sharpAngle2,sharp_angle_wrapper
from qualitychecks.checks.duplicates import duplicate_wrapper
# from qualitychecks.checks.yard import yard_check,yard_wrapper
from qualitychecks.checks.Isolated_roads import RAI,RAI_Wrapper
from qualitychecks.checks.ascii import check_ascii_features,ascii_wrapper
# from qualitychecks.checks.missing_junction import check_features_for_missing_junction, missing_junction,missing_junction_wrapper
# from qualitychecks.checks.preact_filter import preact_filter,preact_wrapper
from qualitychecks.checks.small_dangles import small_dangles_wrapper
from qualitychecks.checks.spikes import spikes_wrapper
# from qualitychecks.checks.solite_split import solite_split_wrapper
from qualitychecks.helpers import remove_single_quotes
from qualitychecks.checks.self_intersecting import self_intersecting_wrapper
from qualitychecks.checks.max_dimension import max_dimension_wrapper
from qualitychecks.checks.streetnames import streetname_wrapper
from loguru import logger

import json
import os
import sys
#from emailservices.feasibilityemailservice import feasibilityemailsucess,feasibilityemailfaild
from threading import Thread


# class EvaluationChecks():
      
def RunEvaluation(filename):
    
    
    logger.info("EvaluationChecks start")

    
    startTime = datetime.now()
    error_flag=False   #flag to check if evaluation run is successful
    total_checks=0
    successful_checks=0
    summary=""
    error_summary={}
    mapping_filename=""
    mnr_total_nw,source_total_nw,potential_km=-1,-1,-1
    checks_status={}


    # --------------------------Parameters--------------------------#
    
    #print("Start", datetime.now() - startTime)
    logger.info("get_job_details start")
    job_type = "evaluation"
    job = get_job_details(filename, job_type)
    country_iso =  job[0]
    grip_schema =  job[1]
    buffer = job[4]
    language = job[5]
    language2 = job[6]
    job_id = job[2]
    userid = job[3]
    run_type = job[9]
    logger.info("get_job_details" , country_iso ,grip_schema ,job_id , userid)
    logger.info("get_job_details End")
    # print(country_iso)
    # print(run_type)
    # print(language)
    # print(grip_schema)
    print(job_id)


    # --------------------------SOLite Data--------------------------#
    logger.info("Get SOLite Centroid" , datetime.now() - startTime)
    #print("Get SOLite Centroid", datetime.now() - startTime)
    # Get SOLite centroid (to get the utm zone for reprojection)
    try:
        pt = get_solite_centroid(grip_schema)

    except:
        error_summary["Get_SOLite_Centroid"]="Failed to get SOLite Centroid."
        error_flag=True
        logger.exception("Failed to get SOLite Centroid...!" , datetime.now() - startTime)


    logger.info("Create SOLite DataFrame" , datetime.now() - startTime)
    #print("Create SOLite DataFrame", datetime.now() - startTime)
    # Create SOLite dataframe
    try:
        solite = solite_from_postgis(grip_schema)

    except:
        error_summary["Create_SOLite_DataFrame"]="Failed to create SOLite DataFrame."
        error_flag=True
        logger.exception("Failed to create SOLite DataFrame...!")


    # Generate Convex Hull Polygon around Source to fetch reference data
    logger.info("Create Convex Hull Polygon" , datetime.now() - startTime)
    try:
        solite_polygon_wkt = get_solite_convex_hull(grip_schema)  # create convex hull

    except:
        error_summary["Create_SOLite_Hull"]="Failed to create SOLite Covex Hull Polygon."
        error_flag=True
        logger.exception("Failed to create SOLite Convex Hull Polygon...!")

    # --------------------------Get Reference Data--------------------------#

    # Get MN-R data
    logger.info("Get MNR global Schema", datetime.now() - startTime)
    try:
        if(error_flag==False):

            # Get mnr server/schema details
            mnr = mnr_server_info(country_iso)
            mnr_host = mnr[1]
            mnr_schema = mnr[0]

    except Exception as E:
        logger.exception(E)
        error_summary["ConnectionEngine/Buffer"]=str(E)
        error_flag=True

    logger.info("Fetch MNR based on source scope", datetime.now() - startTime)
    try:
        if(error_flag==False):
            # create staging table
            create_ref_roads_staging(grip_schema)
            result = mnr_network(mnr_schema, solite_polygon_wkt, mnr_host)
            #Process in batches of 1000
        while True:
            data = result.fetchmany(1000)
            if not data:
                break
            insert_ref_staging(grip_schema, data)
    except:
        error_flag=True 
        logger.exception("Error in creating MNR intersection")

    logger.info("Fetching of MNR data completed", datetime.now() - startTime)

    # --------------------------Get projected coordinate system (utm zone)--------------------------#
    logger.info("Get UTM Zone", datetime.now() - startTime)
    try:
        if(error_flag==False):

            # Create dataframe from intersection
            utm = get_utm(pt)

            z = (utm[:-1])
            if "S" in utm:
                s = True
            else:
                s = False

            # PYProj dict
            crs = CRS.from_dict({'proj': 'utm', 'zone': z, 'south': s})

            # Get all required details
            gbl_prj_crs = crs.to_authority()[1]

            solite_prj = solite.to_crs(epsg=gbl_prj_crs)  # World projection

    except Exception as E:
        print(E)
        error_flag=True 
        logger.exception("Error in getting projected coordinate system")

    """ try:
        fig, ax = plt.subplots(figsize=(15, 10))
        ax.set_aspect('equal')
        mnr_intersection_prj = mnr_intersection.to_crs(epsg=gbl_prj_crs)  # World projection
        mnr_line = mnr_intersection_prj.plot(ax=ax, color='blue',label='MNR')
        source_line = solite_prj.plot(ax=ax, color='red',label='Source')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
        plt.savefig(feasibility_output_path +'\\'+filename+'.png')
        print(">Source vs MN-R Map created and located in project folder.")

    except Exception as E:
        error_summary["Source_vs_MN-R"]=str(E)
        error_flag=True
        print("Error in plotting data for Source vs MN-R Map")
    
    print("----------Downloaded MN-R Network", datetime.now() - startTime)
"""
    #try:
    #	total_checks,successful_checks=RAI_Wrapper(solite,mnr_intersection,feasibility_output_path,sdp_id,sdp_del_id,source_name,results_to_push,checks_status,error_summary,total_checks,successful_checks, gbl_prj_crs)
    #except:
    #	pass
    #print("----------Ended RAI_Wrapper", datetime.now() - startTime)

    # try:
    #     total_checks,successful_checks=too_short_geometry_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks, job_id)
    # except:
    #     pass
    # logger.info("----------Ended too_short_geometry_wrapper", datetime.now() - startTime)

    # try:
    #     total_checks,successful_checks=water_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks, job_id, mnr_schema, mnr_host)
    # except:
    #     pass
    # logger.info("----------Ended water_wrapper", datetime.now() - startTime)

    # try:
    # 	total_checks,successful_checks=fuzzy_wrapper(solite_prj,schema,countrycode,language,host,sdp_id,sdp_del_id,source_name,results_to_push,checks_status,error_summary,total_checks,successful_checks, solite_polygon_wkt)
    # except:
    # 	pass
    # print("----------Ended fuzzy_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks,potential_km=solite_split_wrapper(solite_prj,mnr_intersection,buffer,feasibility_output_path,error_summary,checks_status,total_checks,successful_checks, gbl_prj_crs)
    #except:
    #	pass

    # try:
    #     total_checks,successful_checks=duplicate_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks, job_id)
    # except:
    #     pass
    # logger.info("----------Ended duplicate_wrapper", datetime.now() - startTime)

    # try:
    #     total_checks,successful_checks,pop=snapping_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks, job_id)
    # except:
    #     pass
    # logger.info("----------Ended snapping_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks=frc_wrapper(solite,sj_start, sj_end, gbl_prj_crs,sdp_id,sdp_del_id,source_name,results_to_push,checks_status,error_summary,total_checks,successful_checks)
    #except:
    #	pass
    #print("----------Ended frc_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks=sharp_angle_wrapper(solite,startdf,enddf,sdp_id,sdp_del_id,source_name,results_to_push,checks_status,error_summary,total_checks,successful_checks, gbl_prj_crs)
    #except:
    #	pass
    #print("----------Ended sharp_angle_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks=ascii_wrapper(gbl_prj_crs,solite,sdp_id,sdp_del_id,source_name,results_to_push,checks_status,error_summary,total_checks,successful_checks,'streetname1')
    #except:
    #	pass
    #print("----------Ended ascii_wrapper", datetime.now() - startTime)

    # try:
    #     total_checks,successful_checks=missing_junction_wrapper(grip_schema,solite,checks_status,error_summary,total_checks,successful_checks,job_id)
    # except:
    #     pass
    # logger.info("----------Ended missing_junction_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks=preact_wrapper(solite,sdp_id,sdp_del_id,source_name,results_to_push,checks_status,error_summary,total_checks,successful_checks, gbl_prj_crs)
    #except:
    #	pass
    #print("----------Ended preact_wrapper", datetime.now() - startTime)

    # try:
    #     total_checks,successful_checks=small_dangles_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks,job_id)
    # except Exception as e:
    #     print(e)
    #     pass
    # logger.info("----------Ended small_dangles_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks=spikes_wrapper(results_to_push,checks_status,error_summary,total_checks,successful_checks)
    #except:
    #	pass
    #print("----------Ended spikes_wrapper", datetime.now() - startTime)

    #try:
    #	total_checks,successful_checks=yard_wrapper(solite, language,checks_status,error_summary,total_checks,successful_checks)
    #except:
    #	pass
    #print("----------Ended yard_wrapper", datetime.now() - startTime)

    # try:
    #     total_checks,successful_checks=self_intersecting_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks,job_id)
    # except:
    #     pass
    # logger.info("----------Ended self_intersecting_wrapper", datetime.now() - startTime)

    # try:
        # total_checks,successful_checks=self_intersecting_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks,job_id)
    # except:
    #     pass
    # logger.info("----------Ended self_intersecting_wrapper", datetime.now() - startTime)    
    
    ## high demension quality 
    try:
        # total_checks,successful_checks= max_dimension_wrapper(country_iso, grip_schema)
        total_checks,successful_checks = max_dimension_wrapper(grip_schema, checks_status, error_summary, total_checks, successful_checks, job_id, country_iso)
        # put in one single script line. 
        # keep the logging and non-nessary to print out the function. Notify the start line function. 
    except:
        pass
    logger.info("----------Ended max_dimension", datetime.now() - startTime)

    # for street name check.
    # try:
    #     total_checks,successful_checks=streetname_wrapper(grip_schema,checks_status,error_summary,total_checks,successful_checks, job_id, country_iso,run_type)
    # except:
    #     pass
    # logger.info("----------Ended street_name_check", datetime.now() - startTime)


    try:
        source_total_nw = get_solite_km(grip_schema)
        mnr_total_nw = get_mnr_km(grip_schema)
    except:
        pass

    
    summary=json.dumps(checks_status)
    error_summary=json.dumps(error_summary)
    error_summary=remove_single_quotes(error_summary)

    #logger.info(successful_checks,total_checks)
    successful_checks_percentage=(successful_checks/total_checks)*100
    #if(successful_checks_percentage>=30 and error_flag==False):

    if(successful_checks_percentage>=30):
        #logger.info("Update sucess status function called")
        success='yes'
        evaluation_update_status(success,str(source_total_nw),str(mnr_total_nw),summary,filename,error_summary,str("{:.2f}".format(potential_km)))
        print("Evaluation Run Successful")
        #logger.info("Evaluation Run Successful")
    else:
        success='no'
        #logger.info("Update failed status function called")
        evaluation_update_status(success,str(source_total_nw),str(mnr_total_nw),str(summary),filename,error_summary,str("{:.2f}".format(potential_km)))
        logger.info("Evaluation Run Failed")
    

        
    logger.info("Evaluation Process DONE", datetime.now() - startTime)


# EvaluationChecks("414")

# Sweeden.
# RunEvaluation("gripsource_187dc8")
# NL
RunEvaluation("gripsource_91881b")
# RunEvaluation("gripsource_47ce4a")


    