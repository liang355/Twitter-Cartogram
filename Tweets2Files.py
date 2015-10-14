import arcpy
from arcpy import env
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import traceback


#User Input Values#########################################
in_basemap = "States"
in_path = "C:\Users\Liang\Dropbox\UniversityOfMinnesota\Courses\CSCI5980\Project\ProjectWorkspace.gdb"   # input feature class path
out_path = "C:\Users\Liang\Dropbox\UniversityOfMinnesota\Courses\CSCI5980\Project\ProjectWorkspace.gdb"  # output feature class path
out_LocationsOfTweets = "TweetsLocations"
out_ChoroplethMap = "ChoroplethMap"                                 # output feature class name
TweetsAmount = 50                                  # Number of tweets you want to collect
KeyWord = "the"                                   # Keyword

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


#Main################################################################
def main():
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

		arcpy.SpatialJoin_analysis(target_features, join_features, out_feature_class)
		print "SpatialJoin completed"

	except Exception, e:
		traceback.print_exc()

if __name__ == '__main__':
	main()
