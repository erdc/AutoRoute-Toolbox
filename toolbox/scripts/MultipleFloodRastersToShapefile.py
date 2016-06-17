'''-------------------------------------------------------------------------------
 Tool Name:   MultipleFloodRastersToShapefile.py
 Source Name: MultipleFloodRastersToShapefile.py.py
 Version:     ArcGIS 10.3
 License:     BSD 3-Clause
 Author:      Alan Snow & Andrew Dohmann
 Description: Convert AutoRoute flood extent rasters to shapefile and merge them together.
 History:     Initial coding 6/16/2016, based on AutoRoute_Raster_to_FloodPolygon.py by Mike Follum
 ------------------------------------------------------------------------------'''
import arcpy
import os
from arcpy.sa import *

class MultipleFloodRastersToShapefile(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Multiple Flood Rasters to Shapefile"
        self.description = ("Convert AutoRoute flood extent rasters to shapefile and merge them together.")
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        param0 = arcpy.Parameter(name="floodmap_directory",
                                 displayName="Flood Map Directory",
                                 direction="Input",
                                 parameterType="Required",
                                 datatype="DEFolder")

        param1 = arcpy.Parameter(name = 'working_directory',
                                 displayName = 'Working Directory',
                                 direction = 'Input',
                                 parameterType = 'Required',
                                 datatype = 'DEFolder')

        param2 = arcpy.Parameter(name = "out_shapefile",
                                 displayName = "Output Flood Extents Shapefile",
                                 direction = "Output",
                                 parameterType = "Required",
                                 datatype = "DEFeatureClass")

        params = [param0, param1, param2]

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        
        if arcpy.CheckExtension("Spatial") == "Available":
            arcpy.AddMessage("Checking out spatial license ...")
            arcpy.CheckOutExtension("Spatial")
        else:
            arcpy.ExecuteError("ERROR: The Spatial Analyst license is required to run this tool.")
            
        floodmap_directory = parameters[0].valueAsText
        working_directory = parameters[1].valueAsText
        out_shapefile = parameters[2].valueAsText
        
        # Overwrite pre-existing files
        arcpy.env.overwriteOutput = True
        
        out_shapefile_list = []
        arcpy.env.workspace = floodmap_directory
        flood_rasters = arcpy.ListRasters()
        number_of_rasters = len(flood_rasters)
        for index, flood_raster in enumerate(flood_rasters):
            arcpy.AddMessage("Processing raster: {0} ({1}/{2})".format(flood_raster, index+1, number_of_rasters))
            Agg_Val = 5
            Agg_Use = str(Agg_Val) + " Meters"

            # Process: Convert raster floating point to integer
            arcpy.AddMessage("  Convert raster floating point to integer ...")
            InRast=Int(flood_raster)

            # Process: Boundary Clean
            arcpy.AddMessage("  Performing boundary clean ...")
            BC_Raster = os.path.join("in_memory", "BC_Raster.img")
            OutBndCln = BoundaryClean(InRast, "NO_SORT", "TWO_WAY")
            OutBndCln.save(BC_Raster)
            arcpy.Delete_management(InRast)
            
            # Process: Con
            arcpy.AddMessage("  Turning all values to 0 or 3 ...")
            BC_3C_Raster = os.path.join("in_memory", "BC_3C_Raster.img")
            OutCon = Con((OutBndCln!=3),0,3)
            OutCon.save(BC_3C_Raster)
            arcpy.Delete_management(BC_Raster)

            # Process: Set Null
            arcpy.AddMessage("  Setting values of 0 to NULL ...")
            BC_3CNULL_Raster = os.path.join("in_memory", "BC_3CNULL_Raster.img")
            OutNULL = SetNull(OutCon==0,OutCon) 
            OutNULL.save(BC_3CNULL_Raster)
            arcpy.Delete_management(BC_3C_Raster)

            # Process: Raster to Polygon
            arcpy.AddMessage("  Converting raster to polygon ...")
            Poly_A = os.path.join("in_memory", "Poly_A.shp")
            arcpy.RasterToPolygon_conversion(BC_3CNULL_Raster, Poly_A, "SIMPLIFY", "Value")
            arcpy.Delete_management(BC_3CNULL_Raster)

            ############################################################
            #This is where we loop the aggregate polygons
            ############################################################
            Sign = "Even"
            Num_Iter = 21
            Min_Area = "0 SQUAREMETERS"
            Min_Hole_Size = "100000 SQUAREMETERS"			#Should be a large Number

            Poly_B = os.path.join("in_memory", "Poly_B.shp")
            arcpy.AddMessage("  Working through the aggregation of polygons ...")
            for x in xrange(1,Num_Iter):
                 if(Sign == "Even"):
                      Sign = "Odd"
                 else:
                      Sign = "Even"

                 Agg_Val = (5.0/(1000*111)) * x
                 Agg_Use = str(Agg_Val) + " METERS"

                 if(Sign == "Odd"):
                      arcpy.AggregatePolygons_cartography(Poly_A, Poly_B, Agg_Val, Min_Area, Min_Hole_Size, "NON_ORTHOGONAL", "","")
                 if(Sign == "Even"):
                      arcpy.AggregatePolygons_cartography(Poly_B, Poly_A, Agg_Val, Min_Area, Min_Hole_Size, "NON_ORTHOGONAL", "","")

            Poly_Final = os.path.join(working_directory, "Poly_{0}_{1}.shp".format(os.path.splitext(flood_raster)[0].replace('-','_'), index))
            if(Sign == "Odd"):
                 arcpy.CopyFeatures_management(Poly_B,Poly_Final)
            if(Sign == "Even"):
                 arcpy.CopyFeatures_management(Poly_A,Poly_Final)
            
            #CLEANUP
            arcpy.Delete_management("in_memory")
            
            out_shapefile_list.append(Poly_Final)
        
        
        #Merge Shapefiles
        arcpy.AddMessage("Merging all flood map shapefiles ...")
        arcpy.Merge_management(out_shapefile_list, out_shapefile)
        
        #CLEANUP
        for shapfile in out_shapefile_list:
            arcpy.Delete_management(shapfile)