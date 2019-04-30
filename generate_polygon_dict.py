from shapely.geometry import Polygon, MultiPolygon, Point
import pandas as pd
import json
from datetime import datetime #for testing time of script execution
import pickle #to dump resulting dictionary into directory as a binary file

STARTTIME = datetime.now()

MAP_FILE = 'geojson/TwitterAnalysisMap.json'
OUTPUT_FILE = 'data/merged_polygon_dict'

gj = open(MAP_FILE, mode='r').read()
twitter_geojson = json.loads(gj)

polygons = {}
multi_polygons = {}

polygon_objects = {}
multi_polygon_objects = {}

def generate():

    for f in twitter_geojson['features']:
        if f['geometry']['type'] == 'Polygon':
            polygons[f"{f['properties']['name']}"] = f['geometry']['coordinates']

        elif f['geometry']['type'] == 'MultiPolygon':
            multi_polygons[f"{f['properties']['name']}"] = f['geometry']['coordinates']

    for name, shape in polygons.items():
        shell_parameter = []
        holes_parameter = []
        shell_layer = shape[0]
        holes_layer = shape[1:]

        for pair in shell_layer:
            shell_parameter.append(tuple(pair))
        for pair in holes_layer:
            holes_parameter.append(tuple(pair))

        polygon = Polygon(shell_parameter, holes_parameter)
        polygon_objects[f'{name}'] = polygon

    for name, shape in multi_polygons.items():

        mp_input = []

        for pg in shape:
            shell_parameter = []
            holes_parameter = []

            shell_layer= pg[0]
            holes_layer = pg[1:]

            for pair in shell_layer:
                shell_parameter.append(tuple(pair))

            for pair in holes_parameter:
                holes_parameter.append(tuple(pair))

            polygon = (shell_parameter, holes_parameter)
            mp_input.append(polygon)

        multi_polygon = MultiPolygon(mp_input)
        multi_polygon_objects[f'{name}'] = multi_polygon

    merged_polygon_dict = {**polygon_objects, **multi_polygon_objects}

    with open(OUTPUT_FILE, 'wb') as file:
        pickle.dump(merged_polygon_dict, file)


if __name__ == '__main__':

    print(f"\n\nExtracting info from '{MAP_FILE}'...\n")
    generate()
    print(f"\nSuccessfully converted all lists of coordinates into Shapely Objects...\n")
    print(f"\nSuccessfully dumped output file to '{OUTPUT_FILE}'.\n")
    print(f"\nTime to completion: {datetime.now() - STARTTIME}\n")
