#Script and functions to pull Strava Data down using stravalib
# Activities are saved as a csv
# Streams are saved as a compressed h5
from stravalib.client import Client
import pandas as pd
import os
import sys

#Routine for finding the setdiff of two lists
def find_setdiff(first, second):
        second = set(second)
        return [item for item in first if item not in second]
            
class strava_data_cache:

    #Some Constants
    Run         = 1
    Walk        = 2
    Hike        = 3
    Ride        = 4
    Swim        = 5
    Workout     = 6
    VirtualRide = 7
    AlpineSki   = 8

    stream_types = ['time', 'latlng', 'distance', 'altitude', 'velocity_smooth', \
                        'heartrate', 'cadence', 'watts', 'temp', 'moving', 'grade_smooth']
    
    stream_cols = ['time', 'distance', 'altitude', 'velocity_smooth', \
                        'heartrate', 'cadence', 'watts', 'temp', 'moving', 'grade_smooth', 'lat', 'lon']    
    def __init__(self):
        self.token_file = 'token.txt'
        self.activities_file = 'activities.csv'
        self.streams_file = 'streams.h5'
        self.activites_df = pd.DataFrame()
        self.client = None       

    #Setup Strava Client        
    def setup_client(self):
        self.client = Client()
        if os.path.exists(self.token_file):
           f = open(self.token_file, "r")
           token = f.readline()
           f.close()
        else:
           token = input('Enter access token: ')
        self.client = Client(access_token=token)
        self.client.get_athlete() # Get current athlete details

    #Pull Activities into specified csv file
    def get_activities(self):
        if not os.path.exists(self.activities_file):
            date = "1990-01-01T00:00:00Z"
            old_df = pd.DataFrame()
        else:
            old_df = pd.read_csv(self.activities_file)
            date = pd.to_datetime(old_df.iloc[-1]['start_date']).strftime('%Y-%m-%dT%H:%M:%SZ')
        activities = []
        for act in self.client.get_activities(after = date):
            activities.append(act)
        if len(activities) > 0:
            #package up these activites into a Dataframe from list of dictionary entries
            attributes = ['achievement_count',	'athlete_count',	'average_cadence',	'average_heartrate',	'average_speed',	
                          'average_temp',	'average_watts',	'best_efforts',	'calories',	'comment_count',	'commute',	'description',	
                          'device_name',	'device_watts',	'distance',	'elapsed_time',	'elev_high',	'elev_low',	'embed_token',	
                          'external_id',	'flagged',	'gear',	'gear_id',	'guid',	'has_heartrate',	'has_kudoed',	'id',	
                          'instagram_primary_photo',	'kilojoules',	'kudos_count',	'location_city',	'location_country',	
                          'location_state',	'manual',	'max_heartrate',	'max_speed',	'max_watts',	'moving_time',	'name',	
                          'partner_brand_tag',	'partner_logo_url',	'photo_count',	'photos',	'pr_count',	'private',	'resource_state',	
                          'segment_efforts',	'splits_metric',	'splits_standard',	'start_date',	'start_date_local',	'start_latitude',	
                          'start_longitude',	'suffer_score',	'timezone',	'total_elevation_gain',	'total_photo_count',	'trainer',	'type',	
                          'upload_id',	'weighted_average_watts',	'workout_type']
            new_df = pd.DataFrame([{fn: getattr(act, fn) for fn in attributes} for act in activities])
            new_df.distance = new_df.distance/1609 #convert distances from meters to miles  
            if len(old_df) == 0:
                self.activites_df = new_df
            else:
                self.activites_df = pd.concat([old_df,new_df])
            self.activites_df.to_csv(self.activities_file,encoding ='utf-8')  
        else:
            self.activites_df = old_df
        print("Number of activities: ", len(self.activites_df))

    #Pull new streams into h5 file
    def get_streams(self,maxNum = float("inf")):
        #Check if we have the activities if not then try and grab them
        if len(self.activites_df) == 0:
            self.get_activities()         

        type_map = pd.Series([self.Run,self.Ride,self.Swim,self.Walk,self.Workout,self.VirtualRide,self.Hike,self.AlpineSki],
                             index = ['Run', 'Ride', 'Swim', 'Walk',
                                        'Workout', 'VirtualRide', 'Hike', 'AlpineSki'])
                             
        with pd.HDFStore(self.streams_file,'a') as store:
            try:
                stream_ids = store['streams']['id'].unique()
            except:
                stream_ids = []
            for idx,row in self.activites_df.iterrows():  
                if idx > maxNum:
                    break;
                if row.id in stream_ids:
                    continue
                else:
                    print("Processing ID: {} Name: {}".format(row.id,row.name))
                try:
                    stream = self.client.get_activity_streams(row.id, types=self.stream_types)
                except:
                    print ("Unexpected error getting:", sys.exc_info()[0])
                    continue
                try:
                    temp_df = pd.DataFrame({i : stream[i].data for i in stream}, index=stream['time'].data)
                    if len(temp_df) == 0:
                        print ("Skipping empty stream: %s"%row.name)
                        continue
                    if 'latlng' in temp_df.keys():
                        temp_df['lat'], temp_df['lon'] = list(zip(*temp_df["latlng"]))
                        temp_df = temp_df.drop('latlng',axis=1)
                    if 'distance' in temp_df.keys():
                        temp_df['distance'] = temp_df['distance']/1609
                    temp_df['type'] = row.type
                    temp_df['id'] = row.id
                    
                    #common formatting to get it in the same format for uniform writes
                    for col in find_setdiff(self.stream_cols,temp_df.keys()):
                        temp_df[col] = pd.np.nan
                    temp_df['heartrate'] = temp_df['heartrate'].astype('float')
                    temp_df['cadence'] = temp_df['cadence'].astype('float')
                    temp_df['velocity_smooth'] = temp_df['velocity_smooth'].astype('float')  
                    temp_df['watts'] = temp_df['watts'].astype('float')  
                    temp_df['type'] = temp_df['type'].map(type_map)
                    store.append('streams', temp_df, data_columns=True, format='table',complib='blosc')
                except:
                    print ("Unexpected error post get:", sys.exc_info()[0])
    
    def simple_stream_test(self):
        print(self.client)
        print(self.activites_df.iloc[0].id)
        stream = self.client.get_activity_streams(self.activites_df.iloc[0].id, types=self.stream_types)
        print(stream)
        
if __name__ == '__main__':
    strava_cache = strava_data_cache()
    strava_cache.setup_client()
    strava_cache.get_activities()
    #strava_cache.simple_stream_test()
    strava_cache.get_streams()



