# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
import os
import azure.functions as func
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
import pandas

def main(num: int) -> int:
    logging.info(f"Triggered Function No: {num}")


    test = os.environ["testers"]#Read keyvault ceredentials integrated in the function
    gis = GIS("ESRI PORTAL", "USERNAME", test)#Log onto the ESRI Portal
    #Delete following feature Services if found.
    try:
        gis.content.search("Dnffindcentroids")[0].delete()
        gis.content.search("DnfHealthLyrPolygonToPoint")[0].delete()
    except:
        pass
    #Download fc with the following ID
    item=gis.content.get('FeatureID')
    l=item.layers[0]
    dissolve_fields=['Dissolvefields']
    try:
        g=dissolve_boundaries(l, dissolve_fields,output_name='FatureName1')#Dissolve the polygon feature service on these fields
        #Dissolve downloaded feature service into multipart polygon and save on portal as 'findcentroids' and get the new feature's ID
        c= gis.content.get(g.id).layers[0]
    except:
        pass
     #Calculate the new feature ID's Centroids an save resulting point feature servic on Portal as HealthLyrPolygonToPoint
    try:
        pp= find_centroids(c, output_name="Centroid_FeatureName1")
        tableNF= gis.content.get(pp.id).layers[0].query().sdf      
    except:
        pass
    return num + 1