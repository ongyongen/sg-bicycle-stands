import json
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

class Scraper:
    def __init__(self, lta_account_key, sg_geojson_file):
        self.key = lta_account_key
        self.sg_raw = json.loads(open(sg_geojson_file).read())

    # Generate points 500m apart from each other, covering the entirety of SG
    def generate_equidistant_points(self):
        df_points = pd.DataFrame(columns=['lat','lon','coords','point_in_sg'])
        
        lon = [103.6081 + (x * 0.005) for x in range(100)]
        lat = [1.1607 + (x * 0.005) for x in range(70)]
        lst = [(x,y) for x in lat for y in lon]
        df_points['lat'] = list(map(lambda x: x[0], lst))
        df_points['lon'] = list(map(lambda x: x[1], lst))
        df_points['coords'] = list(map(lambda lon, lat: Point(lon, lat), list(df_points['lon']), list(df_points['lat'])))

        sg_raw_coords = self.sg_raw['features'][0]['geometry']['coordinates'][0][0]
        sg = Polygon(sg_raw_coords)

        df_points['point_in_sg'] = list(map(lambda x: sg.contains(x), list(df_points['coords'])))

        df_points = df_points[df_points['point_in_sg'] == True].reset_index().drop(columns=['index'])
        
        return df_points
    
    # Use generated points to extract bicycle racks data from LTA API
    def extract_bicycle_racks_data(self, df_points):
        all_data = []
        for i in range(len(df_points)):
            lat = df_points.loc[i,'lat']
            lon = df_points.loc[i,'lon']
            url = f"http://datamall2.mytransport.sg/ltaodataservice/BicycleParkingv2?Lat={lat}&Long={lon}"
            payload={}
            headers = {
              'AccountKey': self.key
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            data = response.json()['value']
            all_data.append(data)
            print(response)
            
        all_data = list(filter(lambda x: x != [], all_data))

        return all_data
        

    # Prepare cleaned dataset for extracted bicycle racks
    def clean_bicycle_racks_data(self, all_data):
        
        # Helper function to extract fields of interest from data
        def extract_field(field, all_data):
            return list(map(lambda x: x[0][field], all_data))
        
        df_final = pd.DataFrame(columns=["desc","lat", "lon", "rack_type", "rack_count", "shelter"])
        df_final['desc'] = extract_field('Description', all_data)
        df_final['lat'] = extract_field('Latitude', all_data)
        df_final['lon'] = extract_field('Longitude', all_data)
        df_final['rack_type'] = extract_field('RackType', all_data)
        df_final['rack_count'] = extract_field('RackCount', all_data)
        df_final['shelter'] = extract_field('ShelterIndicator', all_data)
        df_final = df_final.drop_duplicates(subset=['lat', 'lon'])
        return df_final

scraper = Scraper('A92ZBNTrRiiupgAXIckQkA==', 'singapore.geojson')
df_points = scraper.generate_equidistant_points()
all_data = scraper.extract_bicycle_racks_data(df_points)
df_final = scraper.clean_bicycle_racks_data(all_data)
