import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon

map = gpd.read_file('/Users/chuckschultz/climaster/plot_data/us_shapefiles/cb_2018_us_nation_20m.shp')
df = pd.read_csv('/Users/chuckschultz/climaster/stations_limit_us.csv')

lats = df.latitude.to_list()
lons = df.longitude.to_list()
coords = list(zip(lons, lats))

geometry = [Point(xy) for xy in coords]
geo_df = gpd.GeoDataFrame(df, 
                          crs='EPSG:4326', 
                          geometry = geometry)

fig, ax = plt.subplots(figsize=(20,10))
map.to_crs(epsg=4326).plot(ax=ax, color='lightgrey')
geo_df.plot(ax=ax, alpha=0.7, markersize=0.7)
ax.set_title('Weather Stations\nus 45 center')
fig.savefig("stations_us_45_center.png")