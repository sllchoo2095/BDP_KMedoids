

import pandas as pd
import plotly.express as px
import geopandas as gpd

import numpy as np

df = pd.read_fwf('/Users/stephaniechoo/Desktop/BDPTask3Analysis/Data/K6/K6_Depth10.txt', header=None, delimiter=',')

df[0] = df[0].str.strip('Medoid [center=[')
df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[":",":"], regex=True, inplace=True)
df[['Medoid_Latitude','DataPoint']] = df[1].str.split(":",expand=True,)
df[['Longitude_Data','Latitude_Data']] = df['DataPoint'].str.split(",",expand=True,)
df=df.rename(columns={0: "Medoid_Longitude"})
df=df.drop(columns=[1,"DataPoint"])
df["Medoid_Latitude"] = df["Medoid_Latitude"].str.strip(']]')
df["Longitude_Data"] = df["Longitude_Data"].str.strip('[')
df["Latitude_Data"] = df["Latitude_Data"].str.strip(']')
df['Medoid_Coordinates'] = df[["Medoid_Longitude","Medoid_Latitude"]].agg(','.join, axis=1)

print(df)


column_values = df[["Medoid_Coordinates"]].values

unique_values =  np.unique(column_values)
#print(unique_values)

unique_values_list= unique_values.tolist()
medoid_nums= []
for i in unique_values: 
    medoid_nums.append(unique_values_list.index(i))

zip_iterator = zip(unique_values, medoid_nums )

medoid_dict = dict(zip_iterator)

#print(medoid_dict)

df['Cluster'] = df['Medoid_Coordinates'].map(medoid_dict)

#print(df.dtypes)
cols = df.columns.drop('Medoid_Coordinates', 'Medoid')
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
df['Cluster']= df.Cluster.astype(str)

print(df)

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude_Data, df.Latitude_Data ))

#print(gdf)
import plotly.graph_objects as go
px.set_mapbox_access_token(open(".mapbox_token").read())
fig = px.scatter_mapbox(df,lat=df.Latitude_Data,lon=df.Longitude_Data, color= df.Cluster, zoom=10, 
category_orders={"Cluster": ["0","1","2","3","4","5"]}, color_discrete_map={
                '0': "#636EFA",
                "1": "#EF553B",
                "2": "#00CC96",
                "3": "#19D3F3",
                "4": "#FF97FF", 
                "5": "#FECB52"})


fig.add_trace(go.Scattermapbox(
        lat=df.Medoid_Latitude,
        lon=df.Medoid_Longitude,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=5,
            color='rgb(0, 0, 0)',
            opacity=0.7
        ),
        name="Medoid"
    ))
             
fig.show()