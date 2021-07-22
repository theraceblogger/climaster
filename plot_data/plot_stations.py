import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon

map = gpd.read_file('/Users/chuckschultz/climaster/plot_data/gadm36_shp/gadm36.shp')
df = pd.read_csv('/Users/chuckschultz/climaster/CSVs/stations.csv')

lats = df.latitude.to_list()
lons = df.longitude.to_list()
coords = list(zip(lons, lats))

geometry = [Point(xy) for xy in coords]
geo_df = gpd.GeoDataFrame(df, 
                          crs='EPSG:4326', 
                          geometry = geometry)

fig, ax = plt.subplots(figsize=(20,10))
map.to_crs(epsg=4326).plot(ax=ax, color='lightgrey')
geo_df.plot(ax=ax, alpha=0.5, markersize=0.3)
ax.set_title('Weather Stations')
fig.savefig("stations.png")
plt.show()

# path_to_data = gpd.datasets.get_path("nybb")
# gdf = gpd.read_file(path_to_data)

# # print(gdf)

# gdf = gdf.set_index("BoroName")
# gdf["area"] = gdf.area
# # print(gdf["area"])

# gdf['boundary'] = gdf.boundary
# # print(gdf['boundary'])

# gdf['centroid'] = gdf.centroid
# # print(gdf['centroid'])

# first_point = gdf['centroid'].iloc[0]
# gdf['distance'] = gdf['centroid'].distance(first_point)
# # print(gdf['distance'])

# # gdf.plot("area", legend=True)

# # gdf = gdf.set_geometry("centroid")
# # gdfc.plot("area", legend=True)

# # ax = gdf["geometry"].plot()
# # gdf["centroid"].plot(ax=ax, color="black")

# gdf = gdf.set_geometry("geometry")
# # gdf.plot("area", legend=True)

# gdf.crs
# boroughs_4326 = gdf.to_crs("EPSG:4326")
# boroughs_4326.plot()
