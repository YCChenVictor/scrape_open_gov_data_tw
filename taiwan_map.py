import matplotlib.pyplot as plt
import geopandas as gp
import numpy as np
from docs.identity import map_data_location

# obtain the map of taiwan
map_file_path = map_data_location + "VILLAGE_MOI_121_1081121.shp"
villages_shp = gp.read_file(map_file_path)
print(villages_shp.head())
