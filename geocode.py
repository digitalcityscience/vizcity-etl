import pandas as pd

from shapely.wkt import loads as wkt_loads
from shapely.ops import shared_paths
from shapely.geometry import LineString

from models import AddressInfo, LocationLineEPSG
from utils import do_geocode, has_numbers, to_md5
import os



def load_csv_to_df():
    loc_df = pd.read_csv(LOCATION_INFO_PATH)
    loc_df["geometry"] = loc_df["geometry"].apply(wkt_loads)
    loc_df.set_index("geom_id", drop=False, inplace = True)

    return loc_df


LOCATION_INFO_PATH= os.getcwd() + "/fixtures/verkehrslage_streets.csv"
LOCATION_INFO_DF = load_csv_to_df()



def hausdorff_distance(geom1, geom2):
    '''
    Hausdorff Distance calculator, somewhat based on https://github.com/anitagraser/QGIS-Processing-tools/blob/master/1.1/scripts/find_similar_line_feature.py
    Uses pure pyqgis instead of numpy
    Inputs geom1 and geom2 should be QgsGeometry of type 'QGis.Line'
    '''
    # dist = lambda x1, y1, x2, y2: float((x2-x1)**2+(y2-y1)**2)**(0.5)  #Euclidean distance between two coordinates
    def dist(x1, y1, x2, y2):
        return float((x2-x1)**2+(y2-y1)**2)**(0.5)

    def combine(coords1, coords2):
        from itertools import product
        combinations = list(product([tup for tup in coords1], [tup for tup in coords2]))

        return combinations


    ##Get all possible combinations between coordinates on the first line and second line
    combins = [dist(comb[0][0], comb[0][1], comb[1][0], comb[1][1]) for comb in combine(list(geom1.coords), list(geom2.coords))]  # might be like segments?

    ##Find array dimensions
    combinSz = len(list(combins))
    xArrSize = len([tup for tup in list(geom1.coords)])
    yArrSize = int(combinSz/xArrSize)

    ##Turn 1-dimensional list of distances into 2-dimensional list/array
    distAryOne = [[0]*yArrSize for i in range(xArrSize)]  #initialize empty 2-dimensional distance array first
    for x in range(xArrSize):
        for y in range(yArrSize):
            distAryOne[x][y]=combins[(x*yArrSize)+y]
    distAryTwo = [[0]*xArrSize for i in range(yArrSize)]  #flipped order of distAryOne
    for y in range(yArrSize):
        for x in range(xArrSize):
            #print y, x, (y*xArrSize)+x
            distAryTwo[y][x]=combins[(y*xArrSize)+x]

    ##Finally calculates Hausdorff Distance
    #Calculate distances between origin and target feature
    H1 = max([min([distAryOne[i][j] for i in range(xArrSize)]) for j in range(yArrSize)])  #get the highest minimum (supremum infimum) travelling along axis 1 (y-axis)
    H2 = max([min([distAryOne[i][j] for j in range(yArrSize)]) for i in range(xArrSize)])  #get the highest minimum (supremum infimum) travelling along axis 0 (x-axis)
    #print H1, H2
    #Repeat the calculation in reverse order
    H3 = max([min([distAryTwo[j][i] for i in range(xArrSize)]) for j in range(yArrSize)])  #get the highest minimum (supremum infimum) travelling along axis 1 (y-axis)
    H4 = max([min([distAryTwo[j][i] for j in range(yArrSize)]) for i in range(xArrSize)])  #get the highest minimum (supremum infimum) travelling along axis 0 (x-axis)
    #print H3, H4

    hausdorff = max([H1, H2]+[H3, H4])
    #print hausdorff

    return hausdorff

# gets location info for a street segment
def get_address_info(street: LocationLineEPSG) -> AddressInfo:
    
    df = LOCATION_INFO_DF.copy()

    def find_by_geom_hash(street: LocationLineEPSG):
        geom_hash = to_md5(street.shapely_linestring.wkt)
        match = df[df["geom_id"] == geom_hash]

        if len(match.index) == 1:
            return AddressInfo.from_dict(match.iloc[0].to_dict())

    
    def find_by_proximity(street: LocationLineEPSG):
        """
        find locaton info in pandas dataframe by street_id index
        """
        df = LOCATION_INFO_DF.copy()
        
        df.loc[:, "close_by"] = LOCATION_INFO_DF['geometry'].apply(lambda x: x.distance(street.shapely_linestring.centroid) < 250)
        close_by = df.loc[df["close_by"] == True, :].copy()
        close_by = close_by.reset_index(drop=True)
        
        if(len(close_by.index) > 0):
            # rank by hausdorff distance
            close_by.loc[:, "hausdorff"] = close_by['geometry'].apply(lambda x: hausdorff_distance(x, street.shapely_linestring))

            return AddressInfo.from_dict(close_by.iloc[close_by["hausdorff"].idxmin()].to_dict())

        return None    

    # try getting address info from local csv with prev. used streets
    address_info = find_by_geom_hash(street)
    
    if not address_info:
        address_info = find_by_proximity(street)

    if not address_info:
        # if location info not saved locally
        # get the street name, neighborhood and district by reverse geocoding
        street_segment = street.shapely_linestring
        address_info = get_location_info_from_geocoding(street_segment)
        # and save it to local csv
        add_address_info_to_local_csv(address_info, street_segment.wkt)

    return address_info
            
        
# gets the street name, neighborhood and district by reverse geocoding
def get_location_info_from_geocoding(street_segment: LineString) -> AddressInfo:
        
    location_info = do_geocode(street_segment.centroid.x,street_segment.centroid.y)

    # the order of the address array is not always the same
    if has_numbers(location_info[0]):
        # usually street number is first item , also needs to work for "29b"
        return AddressInfo(
            street_number=location_info[0],
            street=location_info[1],
            neighborhood=location_info[2],
            district=location_info[3],
            geom_id = to_md5(street_segment.wkt)            
        )

    if not has_numbers(location_info[0]) and has_numbers(location_info[1]):
        # first element is a place name, like a shop
        return AddressInfo(
            street_number=location_info[1],
            street=location_info[2],
            neighborhood=location_info[3],
            district=location_info[4],
            geom_id = to_md5(street_segment.wkt)            
        )
    
    if not has_numbers(location_info[0]) and not has_numbers(location_info[1]):
        # does not contain a street number at all
        return AddressInfo(
            street_number="null",
            street=location_info[0],
            neighborhood=location_info[1],
            district=location_info[2],
            geom_id = to_md5(street_segment.wkt)            
        )
    
    
def add_address_info_to_local_csv(address_info: AddressInfo, wkt_str: str):
            row = address_info.to_dict()
            row["geometry"] = wkt_str
            
            # add address info to local dataframe
            global LOCATION_INFO_DF
            LOCATION_INFO_DF.loc[len(LOCATION_INFO_DF.index)] = row
            
            # update local csv
            LOCATION_INFO_DF.to_csv(LOCATION_INFO_PATH, index=False)
            # reload after exporting
            LOCATION_INFO_DF = load_csv_to_df()
            
            print("added new adress at ", len(LOCATION_INFO_DF.index), " ",  address_info)

            return address_info
