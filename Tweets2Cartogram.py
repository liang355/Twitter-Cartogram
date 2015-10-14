#Name: Tweets2Cartogram
#Author: Yiren Ding, Yingbin Liang
#Release Date: 5/15/2014
#Version:1.1
#Special thanks to Prof.Brent Hetch.
#Libraries required: GDAL,Tweepy,OGR,Arcpy

import os, sys, string, math

gPath = os.path.split(sys.argv[0])[0]
#os.chdir(gPath)
from osgeo import gdal
print gPath
sys.path.append('GDAL')

import ogr
import arcpy
from arcpy import env
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import traceback

#User Input Values#########################################
in_basemap = "repro5"
in_path = "D:/ArcGIS Workspace/Arc2_EX5/EX5.gdb"   # input feature class path
out_path = "D:/ArcGIS Workspace/Arc2_EX5/EX5.gdb"  # output feature class pathD:\Car
out_LocationsOfTweets = "TweetsLocations"
out_ChoroplethMap = "ChoroplethMap"                                 # output feature class name
TweetsAmount = 50                                  # Number of tweets you want to collect
KeyWord = "the"                                   # Keyword

sValue_field = "Count_"                           #tweets count field
sArea_field = "AREA"                              #Area field
iIterations = 10                                  #Iteration times

##Global Value#############################################
ckey = 'LW0n4aOcS1pwT5ZmIgJhQ'
csecret = 'ddOENJjo3PJoNhclj3CdACQmFaSLL0Q9taBRXFHkZA'
atoken = '1285070930-QJPj4yqREn3wLHu6xnOhew6bbnUZo011myNR5kp'
asecret = 'C5xmsDUWB8xj1vaHGJWP4opQ95yU95JTjMFpvxDdlaZKW'

geometry_type = "POINT"
has_m = "DISABLED"
has_z = "DISABLED"
sr = arcpy.SpatialReference("WGS 1984")

XY = []
Coords = dict()
row_values = []
num_tweets = 0

# Set workspace###########################################
env.workspace = out_path
arcpy.env.overwriteOutput = True

#Listener Class############################################
class listener(StreamListener):	

	def on_status(self, status):

        #print "Tweet_Text: ", status.text
		if status.coordinates != None:
			print "Time_Stamp", status.created_at
			Coords.update(status.coordinates)
			XY = (Coords.get('coordinates'))
			print "X: ", XY[0]
			print "Y: ", XY[1]
			global num_tweets
			global InsertRow
			num_tweets = num_tweets +1
			temprow = (XY[0],XY[1],status.created_at.year,status.created_at.month,
				status.created_at.day,status.created_at.hour,
				status.created_at.minute,(XY[0],XY[1]))
			row_values.append(temprow)

			if num_tweets < TweetsAmount:
				return True
			else:
				print num_tweets, "items have been recorded"
				return False

	def on_error(self, status):
		 print status






#############################################################################
# Feature stores various pre-calculated values about each feature
class Feature(object):
    count = 0
    def __init__(self):
        Feature.count = Feature.count + 1
        self.lFID = -1
        self.lGElemPos = -1
        self.ptCenter_x = -1
        self.ptCenter_y = -1
        self.dNew_area = -1
        self.dFactor = -1
        self.sName = ""
        self.dValue = -1
        self.dArea = -1
        self.dMass = -1
        self.dRadius = -1
        self.dVertices = -1
    
#############################################################################
#
# Finding the Centroid of a multi-part feature requires recursion
#
def RecurseCentroidMP(geom, area, sx, sy, polyc):
    pc = geom.GetPointCount()
    gt = geom.GetGeometryType()

    # As we recurse deeper into the geometries, we get away
    # from areas (lines, points)

    if pc == 0:
        # Still have multiple geometries - need to recurse deeper
        for i in range(geom.GetGeometryCount()):
            g2 = geom.GetGeometryRef(i)

            if (gt == 3):
                area = geom.GetArea()
                
            (sx, sy, polyc) = RecurseCentroidMP(g2, area, sx, sy, polyc)

                        
    else:
        # Calculate the centroid of this polygon
        polyc = polyc + 1
        cx = cy = 0
        xoff, xmax, yoff, ymax = geom.GetEnvelope()

        for i in range(0, pc):
            x0 = geom.GetX(i) - xoff
            y0 = geom.GetY(i) - yoff

            cx = cx + x0
            cy = cy + y0


        cx = cx / pc + xoff
        cy = cy / pc + yoff
        
##        for i in range(0, pc):
##            x0 = geom.GetX(i) - xoff
##            y0 = geom.GetY(i) - yoff
##            x1 = geom.GetX(i+1) - xoff
##            y1 = geom.GetY(i+1) - yoff
##
##            p = (x0 * y1 - x1 * y0)
##            cx = cx + (x0 + x1) * p
##            cy = cy + (y0 + y1) * p
##
##
##        cx = (cx / (-6.0 * area)) + xoff
##        cy = (cy / (-6.0 * area)) + yoff

        print "PolyCentroid: " + str(cx) + ", " + str(cy)
        sx.append(cx)
        sy.append(cy)


    return (sx, sy, polyc)


#############################################################################
#
# Returns the x,y centroid of a multipolygon
#
# Works perfectly for single-part polygons but is having "issues" with
# multi-part polygons. Right now, it averages the centroid of each part
# of the multi-part polygon to determine the overall centroid.
#
def GetCentroidMP( geom ):
    sx = []
    sy = []
    polyc = area = 0
    try: 
        (sx, sy, polyc) = RecurseCentroidMP(geom, area, sx, sy, polyc)
    
        if (polyc == 0):
            print "Error: Polygon Count Zero!!!"
            sys.exit(-1)

    except:            
        print "Error: Calculating Centroid!!!"
        raise("Error: Calculating Centroid!!!")
        return (0, 0)


    cx = cy = 0
    for i in range(0, polyc):
        cx = cx + sx[i]
        cy = cy + sy[i]
    cx = cx / polyc
    cy = cy / polyc    
        
    #print "Polyc: " + str(polyc) + " Centroid: "+str(sx) + ", " + str(sy)    

    # Return the average centroid            
    return (cx, cy)

#############################################################################
#
# Actually changes the x,y of each point
#
def TransformGeometry(aLocal, dForceReductionFactor, geom):
    global g_lFeature_count
        
    gc = geom.GetGeometryCount() 

    if (gc > 0):
        # If it's multi-part, we have to go recursive
        for g in range(gc):
            old_geom = geom.GetGeometryRef(g)
            new_geom = TransformGeometry(aLocal, dForceReductionFactor, old_geom)
            if new_geom is not old_geom:
                geom.SetGeometryDirectly(new_geom)
            
    else:
        pc = geom.GetPointCount()

        #print "    Which has " + str(pc) + " points"
        #print "Transforming Geometry"
        for p in range(pc):
            x = x0 = geom.GetX(p)
            y = y0 = geom.GetY(p)
            
            #print "  Vertex: " + str(p) + "  (" + str(x) + " , " + str(y) + " )"
                
            #
            # Compute the influence of all shapes on this point
            #
            for i in range(g_lFeature_count):
                lf = aLocal[i]
                cx = lf.ptCenter_x
                cy = lf.ptCenter_y

                # Pythagorean distance                
                distance = math.sqrt((x0 - cx) ** 2 + (y0 - cy) ** 2)
                
                if (distance > lf.dRadius):
                    # Calculate the force on verteces far away from the centroid of this feature
                    Fij = lf.dMass * lf.dRadius / distance
                else:
                    # Calculate the force on verteces far away from the centroid of this feature
                    xF = distance / lf.dRadius
                    Fij = lf.dMass * (xF ** 2) * (4 - (3 * xF))
                
                Fij = Fij * dForceReductionFactor / distance
                #print "    " + str(i) + "  " + str(distance) + "  " + str(Fij)
                x = (x0 - cx) * Fij + x
                y = (y0 - cy) * Fij + y
                    
            #print "  Update: " + str(p) + "  (" + str(x) + " , " + str(y) + " )"
            geom.SetPoint(p, x, y)
                
        # End: Loop through all the points

    # if..else...

    return geom


#############################################################################
#
# CreateRubberSheetCartogramFeature


def CreateRubberSheetCartogram(inFile, outFile):
    global g_lFeature_count
    global g_aFeatures
    global g_dTotal_value
    global g_iValue_field

    (iPath, iName) = os.path.split(inFile)
    (oPath, oName) = os.path.split(outFile)

    try:    
        # Open the input Shapefile
        inSHP = ogr.Open(inFile, update = 0)
        inLayer = inSHP.GetLayer(0)
        inDefn = inLayer.GetLayerDefn()

        # Delete the destination files
        if os.path.exists(outFile):
            os.remove(outFile)
            (base, ext) = os.path.splitext(outFile)
            os.remove(base+".shx")
            os.remove(base+".dbf")

        # Create the output Shapefile
        shpDriver = ogr.GetDriverByName('ESRI Shapefile')
        shpDriver.DeleteDataSource(outFile)
        outSHP = shpDriver.CreateDataSource(outFile)

        name = inDefn.GetName()
        geom_type = inDefn.GetGeomType()
        srs = inLayer.GetSpatialRef()
        outLayer = outSHP.CreateLayer(name, srs, geom_type)

        # copy the field definintions to the output Shapefile    
        for i in range(inDefn.GetFieldCount()):
            inField = inDefn.GetFieldDefn(i)
            
            field = ogr.FieldDefn(inField.GetName(), inField.GetType())
            field.SetWidth(inField.GetWidth())
            field.SetPrecision(inField.GetPrecision())
            outLayer.CreateField(field)
    except:
        print "Error creating temp shapefile "+outFile
        sys.exit(-1)



    try:    
        #
        # Most of this function follows Andy Agenda#s Avenue script as closely as possible
        # I even maintained variable names and comments where I could.
        #
        dAreaTotal = 0
        aLocal = []
        cx = 0
        cy = 0
        
        #
        # Read the polygons; accumulate total area;
        # cache the individual areas and centers.
        #
        for i in range(inLayer.GetFeatureCount()):
            inFeature = inLayer.GetNextFeature()

            mf = Feature()            
            mf.lFID = inFeature.GetFID()
            
            geom = inFeature.GetGeometryRef()
            mf.dArea = geom.GetArea()
            
            dAreaTotal = dAreaTotal + mf.dArea
            
            (cx, cy) = GetCentroidMP(geom)
            mf.ptCenter_x = cx
            mf.ptCenter_y = cy            
                    
            mf.dValue = inFeature.GetField(g_iValue_field)
            aLocal.append(mf)

        
        #
        # Set up the problem: masses, radii,
        # accumulated size error.
        #
        dFraction = dAreaTotal / g_dTotal_value    
        dSizeErrorTotal = 0    
        dSizeError = 0

        for i in range(g_lFeature_count):
            lf = aLocal[i]
            dPolygonValue = lf.dValue
            dPolygonArea = lf.dArea

            # The area should never be less than zero            
            if (dPolygonArea < 0):
                dPolygonArea = 0

            dDesired = dPolygonValue * dFraction
            
            dRadius = math.sqrt(dPolygonArea / math.pi)
            lf.dRadius = dRadius
            
            lf.dMass = math.sqrt(dDesired / math.pi) - dRadius

            dSizeError = max(dPolygonArea, dDesired) / min(dPolygonArea, dDesired)

            dSizeErrorTotal = dSizeErrorTotal + dSizeError
            
            print " Feature: " + str(lf.lFID)  # + " " + g_aFeatures[i].sName
            print "  Area:    " + str(dPolygonArea)
            print "  Radius:  " + str(lf.dRadius)
            print "  Desired: " + str(dDesired)
            print "  Mass:    " + str(lf.dMass)
            print "  Error:   " + str(dSizeError)
            print "  Centroid: (" + str(lf.ptCenter_x) + ", " + str(lf.ptCenter_y) + ")"                
        # end: for i in range(g_lFeature_count)
        
        dMean = dSizeErrorTotal / g_lFeature_count
        
        # This is interesting - I need to read up more on why this is done
        dForceReductionFactor = 1 / (dMean + 1)
        
        print "  SET: " + str(dSizeErrorTotal)
        print "  FRF: " + str(dForceReductionFactor)

    except:
        if inSHP:
            inSHP.Destroy()
        if outSHP:
            outSHP.Destroy()

        print "Error calculating forces"
        sys.exit(-1)

    try:
        
        c = 0
        
        inLayer.ResetReading()
        inFeature = inLayer.GetNextFeature()
        while inFeature is not None:
            geom = inFeature.GetGeometryRef().Clone()
            c=c+1
            print "Calling TransformGeometry (" +str(c) + ")..."

            geom = TransformGeometry(aLocal, dForceReductionFactor, geom)

            print "Geometry Transformed...(" +str(c) + ")"
            
            outFeature = ogr.Feature(inLayer.GetLayerDefn())
            outFeature.SetFrom(inFeature)
            outFeature.SetGeometryDirectly(geom)

            # Copy the contents of each field
            #for i in range(inDefn.GetFieldCount()):
            #    outFeature.SetField(i, inFeature.GetField(i))
            
            outLayer.CreateFeature(outFeature)            

            outLayer.SyncToDisk()

            outFeature.Destroy()
            inFeature.Destroy()
        
            # get next feature
            inFeature = inLayer.GetNextFeature()
            
        # Loop through polygon features in layer

        print "Cleaning up..."

        del aLocal

        #outSHP.SyncToDisk()

        inSHP.Destroy()

        outSHP.Destroy()

    except:
        if inSHP:
            inSHP.Destroy()
        if outSHP:
            outSHP.Destroy()

        print "Error moving vertices"
        sys.exit(-1)

            
# --- CreateRubberSheetCartogramFeature

#############################################################################
def Usage():
    print 'Usage: carto.py infile outfile areafield'
    print
    sys.exit(1)

#############################################################################




auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())
twitterStream.filter(track=[KeyWord])

	
try:
		#Create Feature class#######################################
	arcpy.CreateFeatureclass_management(out_path,out_LocationsOfTweets,
	"POINT","","DISABLED","DISABLED", arcpy.SpatialReference("WGS 1984"))
	arcpy.AddField_management(out_LocationsOfTweets,"Latitude","DOUBLE")
	arcpy.AddField_management(out_LocationsOfTweets,"Longitude","DOUBLE")	
	arcpy.AddField_management(out_LocationsOfTweets,"Year","SHORT")
	arcpy.AddField_management(out_LocationsOfTweets,"Month","SHORT")
	arcpy.AddField_management(out_LocationsOfTweets,"Day","SHORT")
	arcpy.AddField_management(out_LocationsOfTweets,"Hour","SHORT")
	arcpy.AddField_management(out_LocationsOfTweets,"Minute","SHORT")
		
       #Insert Data,use cursor########################################
	InsertFeature = out_path + "/" + out_LocationsOfTweets
	cursor = arcpy.da.InsertCursor(InsertFeature,("Latitude","Longitude","Year",
	"Month","Day","Hour","Minute","SHAPE@XY"))

	for row in row_values:
		cursor.insertRow(row)
	del cursor 
	print "Insert completed"
		#SpatialJoin
	target_features = in_path + "/" + in_basemap
	join_features = out_path + "/" + out_LocationsOfTweets
	out_feature_class = out_path + "/" + out_ChoroplethMap

	arcpy.SpatialJoin_analysis(target_features, join_features, "D:/Car/FinalRawMap.shp")
	print "SpatialJoin completed"
	#arcpy.CalculateField_management("D:/Car/FinalRawMap.shp","Join_Count","[Joint_Count]+1","VB")
		

except Exception, e:
		traceback.print_exc()




inFile = "D:/Car/FinalRawMap.shp"
outFile = "D:/Car/Cartogram"


g_iValue_index = -1
iArea_index = -1

bDelete_temp = 0

i = 1


while i < len(sys.argv):
    arg = sys.argv[i]

    if inFile is None:
        inFile = arg
    elif outFile is None:
        outFile = arg
    elif sValue_field is None:
        sValue_field = arg
    elif (iIterations == 0):
        iIterations = int(arg)

    else:
        Usage()

    i = i + 1

if outFile is None:
    Usage()




#############################################################################
# Open the datasource to operate on.

print "Input file: " + inFile

inSHP = ogr.Open( inFile, update = 0 )
inLayer = inSHP.GetLayer( 0 )
inDefn = inLayer.GetLayerDefn()

#fileBase = outFile[outFile.rfind('\\')+1:outFile.rfind('.')]

tempFiles = []

fcount = inDefn.GetFieldCount()
g_iValue_field = -1
for i in range(0, fcount):
    src_fd = inDefn.GetFieldDefn(i)
    
    #fd = ogr.FieldDefn( src_fd.GetName(), src_fd.GetType() )
    #fd.SetWidth( src_fd.GetWidth() )
    #fd.SetPrecision( src_fd.GetPrecision() )
    #shp_layer.CreateField( fd )

    if src_fd.GetName() == sValue_field:
        g_iValue_field = i
##    elif src_fd.GetName() == sArea_field:
##        iArea_field = i

print "New Area Field (" + sValue_field + ") = ", g_iValue_field       
##print "Old Area Field (" + sArea_field + ") = ", iArea_field        

#############################################################################
# Loop through the features and calculate the new areas
g_lFeature_count = inLayer.GetFeatureCount()

dTotal_area = 0.0
g_dTotal_value = 0.0
dTotal_points = 0
g_aFeatures = []

for i in range(g_lFeature_count):
    inFeat = inLayer.GetNextFeature()
    myFeature = Feature()
    
    geom = inFeat.GetGeometryRef()

    myFeature.lFID = inFeat.GetFID()
    
    myFeature.dArea = geom.GetArea()
    #g_aFeatures[i].adArea = inFeat.GetFieldAsDouble(iArea_field)
    myFeature.dValue = inFeat.GetFieldAsDouble(g_iValue_field)
    
    dTotal_area += myFeature.dArea
    g_dTotal_value += myFeature.dValue

    myFeature.dDensity = myFeature.dValue / myFeature.dArea
    myFeature.dMass = myFeature.dValue
       
    myFeature.dVertices = geom.GetPointCount()
    dTotal_points = dTotal_points + myFeature.dVertices
    g_aFeatures.append(myFeature)

dDensity = g_dTotal_value / dTotal_area

print "Total (",sValue_field,") = ", g_dTotal_value
print "Total (",sArea_field,") = ", dTotal_area

print "New density (",sValue_field,"/",sArea_field,") = ", dDensity


for f in g_aFeatures:
    f.dNew_area = f.dValue / dDensity
    f.dFactor = f.dNew_area / f.dArea
    f.dRadius = math.sqrt(f.dArea / math.pi)

    #print "Feature "+str(f.lFID)+" value="+str(f.dValue)+" area="+str(f.dArea)+" new area="+str(f.dNew_area)+" factor="+str(f.dFactor)


    # RESOLVE: For some reason, the Country shape file in the ROW data goes negative hereb
    dTemp = f.dNew_area / math.pi
    if (dTemp > 0):
       f.adMass = math.sqrt(f.dNew_area / math.pi) - f.dRadius
    else:
       f.adMass = 0

inSHP.Destroy()

aTempfiles = []

(fileBase, fileExt) = os.path.splitext(outFile)

#CreateRubberSheetCartogram
for i in range(1,iIterations+1):
    if (i < iIterations):
        outFile = fileBase + "_" +str(i) + ".shp"
    else:
        outFile = fileBase + ".shp"

    print "Iteration: " + str(i) + " output: " + outFile
    
    aTempfiles.append(outFile)

    CreateRubberSheetCartogram(inFile, outFile)
    
    inFile = outFile        

print "Cartogram Complete!"

if (bDelete_temp):
    for temp in aTempfiles:
        shp_driver.DeleteDataSource(temp)
