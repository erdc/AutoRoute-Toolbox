'''-------------------------------------------------------------------------------
 Tool Name:   flood_raster_to_shapefile_multiprocess.py
 Source Name: flood_raster_to_shapefile_multiprocess.py
 Version:     ArcGIS 10.3
 License:     BSD 3-Clause
 Author:      Alan Snow
 Description: Convert AutoRoute flood extent rasters to shapefile and merge them together.
 History:     Initial coding 6/16/2016, based on AutoRoute_Raster_to_FloodPolygon.py by Mike Follum
 ------------------------------------------------------------------------------'''
import arcpy
import multiprocessing
import os
import sys
from time import sleep

def floodmap_to_shapefile(args):
    """
    Convert floodmap to shapefile
    """
    arcpy.env.overwriteOutput = True
    
    flood_raster = args[0]
    index = args[1]
    number_of_rasters = args[2]
    working_directory = args[3]
    mp_spatial_lock = args[4]
    
    arcpy.AddMessage("Processing raster: {0} ({1}/{2})".format(flood_raster, index+1, number_of_rasters))
    print("Processing raster: {0} ({1}/{2})".format(flood_raster, index+1, number_of_rasters))
    Agg_Val = 5
    Agg_Use = str(Agg_Val) + " Meters"
    
    mp_spatial_lock.acquire()
    spatial_checked_out = False
    while not spatial_checked_out:
        if arcpy.CheckExtension("Spatial") == "Available":
            print("Checking out spatial license ...")
            arcpy.CheckOutExtension("Spatial")
            spatial_checked_out = True
            break
        sleep(1)
        
    # Process: Convert raster floating point to integer
    print("  Convert raster floating point to integer ...")
    InRast=arcpy.sa.Int(flood_raster)

    # Process: Boundary Clean
    print("  Performing boundary clean ...")
    BC_Raster = os.path.join("in_memory", "BC_Raster_{0}".format(index))
    OutBndCln = arcpy.sa.BoundaryClean(InRast, "NO_SORT", "TWO_WAY")
    OutBndCln.save(BC_Raster)
    
    # Process: Con
    print("  Turning all values to 0 or 3 ...")
    BC_3C_Raster = os.path.join("in_memory", "BC_3C_Raster_{0}".format(index))
    OutCon = arcpy.sa.Con((OutBndCln!=3),0,3)
    OutCon.save(BC_3C_Raster)
    arcpy.Delete_management(BC_Raster)

    # Process: Set Null
    print("  Setting values of 0 to NULL ...")
    BC_3CNULL_Raster = os.path.join("in_memory", "BC_3CNULL_Raster_{0}".format(index))
    OutNULL = arcpy.sa.SetNull(OutCon==0,OutCon) 
    OutNULL.save(BC_3CNULL_Raster)
    arcpy.Delete_management(BC_3C_Raster)

    mp_spatial_lock.release()
    
    # Process: Raster to Polygon
    print("  Converting raster to polygon ...")
    Poly_A = os.path.join("in_memory", "Poly_A_{0}".format(index))
    arcpy.RasterToPolygon_conversion(BC_3CNULL_Raster, Poly_A, "SIMPLIFY", "Value")
    arcpy.Delete_management(BC_3CNULL_Raster)

    ############################################################
    #This is where we loop the aggregate polygons
    ############################################################
    Sign = "Even"
    Num_Iter = 21
    Min_Area = "0 SQUAREMETERS"
    Min_Hole_Size = "100000 SQUAREMETERS"			#Should be a large Number

    Poly_B = os.path.join("in_memory", "Poly_B_{0}".format(index))
    print("  Working through the aggregation of polygons ...")
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

    Poly_Final = os.path.join(working_directory, "Poly_Final_{0}.shp".format(index))
    if(Sign == "Odd"):
         arcpy.CopyFeatures_management(Poly_B,Poly_Final)
    if(Sign == "Even"):
         arcpy.CopyFeatures_management(Poly_A,Poly_Final)

    #CLEANUP
    arcpy.Delete_management(Poly_A)
    arcpy.Delete_management(Poly_B)
    
    return Poly_Final

def main_execute(floodmap_directory, working_directory, out_shapefile):

    multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
    
    # Overwrite pre-existing files
    arcpy.env.overwriteOutput = True
    
    arcpy.env.workspace = floodmap_directory
    flood_rasters = arcpy.ListRasters()
    number_of_rasters = len(flood_rasters)
    mp_spatial_lock = multiprocessing.Manager().Lock()
    job_combinations = []
    out_shapefile_list = []
    for index, flood_raster in enumerate(flood_rasters):
        job_combinations.append((os.path.join(floodmap_directory, flood_raster), index, number_of_rasters, working_directory, mp_spatial_lock))
        #out_shapefile_list.append(floodmap_to_shapefile((flood_raster, index, number_of_rasters, working_directory)))

    arcpy.env.workspace = ""
    
    NUM_CPUS = min(number_of_rasters, multiprocessing.cpu_count())
    pool = multiprocessing.Pool(NUM_CPUS)
    
    for output in pool.imap_unordered(floodmap_to_shapefile, job_combinations):
        out_shapefile_list.append(output)
        
    pool.close()
    pool.join()

    #CLEANUP
    arcpy.Delete_management("in_memory")

    #Merge Shapefiles
    arcpy.AddMessage("Merging all flood map shapefiles ...")
    arcpy.Merge_management(out_shapefile_list, out_shapefile)
    
    #CLEANUP
    for shapfile in out_shapefile_list:
        arcpy.Delete_management(shapfile)

if __name__=='__main__':
    import flood_raster_to_shapefile_multiprocess
    
    floodmap_directory = arcpy.GetParameterAsText(0)
    #floodmap_directory = 'E:\\RAPID\\AutoRoute\\flood_maps\\test'
    working_directory = arcpy.GetParameterAsText(1)
    #working_directory = 'E:\\RAPID\\AutoRoute\\tmp'
    out_shapefile = arcpy.GetParameterAsText(2)
    #out_shapefile = 'E:\\RAPID\\AutoRoute\\floodmap2.shp'
    
    flood_raster_to_shapefile_multiprocess.main_execute(floodmap_directory, working_directory, out_shapefile)