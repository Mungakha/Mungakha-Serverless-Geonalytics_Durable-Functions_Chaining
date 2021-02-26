# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import time
#GENERAL
import http.client
import mimetypes
import json
from pandas import json_normalize
import sys, os
import pandas as pd
import datetime
from copy import deepcopy
import numpy as np
import requests
#ARCGIS
from arcgis.features.manage_data import dissolve_boundaries
from arcgis.features.find_locations import find_centroids
from arcgis.geometry import from_geo_coordinate_string
from arcgis.geocoding import geocode
from arcgis.geometry import lengths, areas_and_lengths, project
from arcgis.geometry import Point, Polyline, Polygon, Geometry
from arcgis.gis import GIS
import arcgis
from arcgis import geometry 
from arcgis import features
from arcgis.geoanalytics import manage_data
from arcgis.features.manage_data import overlay_layers
from arcgis.features import GeoAccessor, GeoSeriesAccessor, FeatureLayer
from arcgis.features import FeatureLayerCollection
import azure.functions as func

def main(num: int) -> int:
    logging.info(f"Triggered Function No: {num}")

    
    #test = os.environ["testers"]#Read keyvault ceredentials integrated in the function
    gis = GIS("ESRI PORTAL", "USERNAME", test)#Log onto the ESRI Portal
    test1="prod_gis_13ec984076494b7aa3908360b8341dde"#os.environ["test1"]
    test2="NBla5nmun5rRCPwmu0Aa-YPzvCh7Rv8vSFczXknHCUQ="#os.environ["test2"]
    conn = http.client.HTTPSConnection("API_URL")
    payload = 'client_id='+ test1 +'&client_secret='+test2 +'&grant_type=client_credentials'
    headers = {  'Content-Type': 'application/x-www-form-urlencoded'}
    conn.request("POST", "/API ENDPOINT URL/Web/PROD/oauth2/access_token", payload, headers)
    res = conn.getresponse()
    data = res.read()
    access_token=data.decode("utf-8")
    # Sign into Tech 1, download any opcodes with zeroes and put them into a seach list

    url = "API_URL/T1Prod/CiAnywhere/API ENDPOINT URL/Web/PROD/Api/RaaS/v1/TABLENAME?q=Active%20%3D1%20and%20LATITUDE%20%3D%20%20%220.000000%22%20and%20LONGITUDE%20%3D%20%220.000000%22&pageSize =300"

    payload = {}
    headers = {'Authorization': access_token.split('"')[3]}
    response = requests.request("GET", url, headers=headers, data = payload)
    #print(response.text.encode('utf8'))
    j=json.loads(response.text.encode('utf8'))
    tc = pd.json_normalize(j['DataSet'])
    tc=tc[tc['T_LATITUDE'].astype(float)==0]
    mc=tc.drop(columns=['T_ID',  'T_TITLE', 'T_AFSCERTIFIEDAREA','T_FSCCERTIFIEDAREA', 'T_STARTDATE', 'T_ENDDATE', 'T_LATITUDE', 'T_LONGITUDE', 'T_ACTIVE', 'T_CREATEDDATETIME', 'T_MODIFIEDDATETIME'])
    searchlist=str(list(mc.T_OPERATIONNO.values))[1:-1]

   
    try:
        item = gis.content.search("Centroid_FeatureName1",item_type="Feature Layer Collection")#Search and download PTN Centroids
        df= gis.content.get(item[0].id).layers[0].query(where = f"ID_FIELD in ({searchlist})",out_fields = "*", ).sdf#Extract Spatilla Enabled DataFrame
        #df=df[df['ID_FIELD'].isin(mc.T_OPERATIONNO)]
        df=df.assign(Long=df.SHAPE.astype(str).apply(lambda x: Point(x)            .coordinates()).str[0],Lat=df.SHAPE.astype(str).                apply(lambda x: Point(x).coordinates()).str[1]).                    drop(columns=['OBJECTID',  'Count_','ORIG_FID','PLANTATION' ,'AnalysisArea','SHAPE'])#Compute Lat Long and drop unwanted columns
    except:
        pass


    # Read Native Forest Centroids

    try:
        itemnf = gis.content.search("Centroid_FeatureName2",item_type="Feature Layer Collection")#Search and download NF Centroids
        ntfobject= gis.content.get(itemnf[0].id).layers[0]
        tableNF=gis.content.get(itemnf[0].id).layers[0].query(where = f"ID_FIELD in ({searchlist})",out_fields = "*", ).sdf
        
        
    except:
        pass
    #Project NF sdf to geographic coordinate system from 
    try:
        spr=ntfobject.query().spatial_reference['latestWkid']#Acquire the to be spatial reference
        tableNF['SHAPE'] =tableNF.SHAPE.apply(lambda Y: geometry.project(geometries =[Y],in_sr = spr, out_sr = 4326,gis = gis)).str[0]
    except:
        pass
    #Compute NF Lat Long and drop unwanted coordinates
    try:
        tableNF=tableNF.join(pd.read_json(tableNF['SHAPE'].to_json(orient='records'), orient='records').rename(columns={'x':'Long','y':'Lat'}))
        tableNF=tableNF.drop(columns=['OBJECTID','Count_', 'AnalysisArea', 'ORIG_FID', 'SHAPE'])
    except:
        pass
    #Drop any opcodes that are NaNs or empty
    try:
        tableNF['Ops_Code']=tableNF['Ops_Code'].replace({'':np.nan})
        tableNF=tableNF[tableNF['ID_FIELD'].notna()]
    except:
        pass
    # Create table with known schema, drop all rows and append df/df1 if they contain any data
    data12='[{"ID_FIELD":"AZZ8JF1","Long":117.338241695,"Lat":-32.401822711},{"ID_FIELD":"AZZ8JF1","Long":115.535504945,"Lat":-30.371467414},{"ID_FIELD":"AZZ2KF1","Long":115.870765563,"Lat":-30.87934509}]'
    table=pd.read_json(data12, orient='records')
    table=table.loc[:-1]
    try:
        table=table.append([df,tableNF], ignore_index=True)
    except:
        pass
    #Filter Tech 1 Opcodes with zeroes and drop unwanted columns and merge with table
    tc1=tc.drop(columns=[ 'T_AFSCERTIFIEDAREA','T_TITLE',
        'T_FSCCERTIFIEDAREA', 'T_STARTDATE', 'T_ENDDATE', 'T_LATITUDE',
        'T_LONGITUDE', 'T_ACTIVE', 'T_CREATEDDATETIME', 'T_MODIFIEDDATETIME'])
    try:
        missing13=pd.merge(tc1,table, how='right', left_on='T_OPERATIONNO', right_on='ID_FIELD')
        missing1=missing13[(missing13['T_ID'].notna())]
        missing1['SystemId']= "FPC"
    except:
        pass
    try:
        mis=pd.DataFrame({'a':[0],'b':['b'],'c':['c'],'d':[1.0],'e':[2.0],'f':['f']})
        mis=mis.assign(a=missing1.T_ID,b=missing1.T_OPERATIONNO,c=missing1.Ops_Code,d=missing1.Long.astype(float),e=missing1.Lat.astype(float),f=missing1.SystemId)
    except:
        pass
    
    #Rename columns to allign to TECH1
    try:
        mis.drop(columns=['c','b'],inplace=True)
        mis.columns=['EngagementId','DE_LOD_INF_LAND_LONG', 'DE_LOD_INF_LAND_LAT','SystemId']
    except:
        pass
    # Prepare payload

    try:
        result = mis.to_json(orient="records")
        parsed = json.loads(result)
        p=json.dumps(parsed)
    except Exception:
        pass
    try:
        url = "API_URL/T1Prod/CiAnywhere/API ENDPOINT URL/Web/PROD/Api/WS/v1/CRMEngagement/SaveBulk"

        payload = {'Items': json.loads(p)}

        print(payload['Items'][0]['SystemId'])
        headers = {'Authorization': access_token.split('"')[3],'Content-Type': 'application/json'}

        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        logging.info(response.text.encode('utf8'))
         
    except Exception:
        pass
    return num + 1