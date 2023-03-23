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
    loc_df.set_index("id", drop=False, inplace = True)

    return loc_df


LOCATION_INFO_PATH= os.getcwd() + "/fixtures/verkehrslage_streets.csv"
LOCATION_INFO_DF = load_csv_to_df()


# gets location info for a street segment
def get_address_info(street: LocationLineEPSG) -> AddressInfo:
    
    df = LOCATION_INFO_DF.copy()

    def find_by_geom_hash(street: LocationLineEPSG):
        geom_hash = to_md5(street.shapely_linestring.wkt)
        match = df[df["id"] == geom_hash]

        if len(match.index) == 1:
            return AddressInfo.from_dict(match.iloc[0].to_dict())

    
    def find_by_proximity(street: LocationLineEPSG):
        """
        find locaton info in pandas dataframe by street_id index
        """
        
        # subset of intersecting streets
        df = LOCATION_INFO_DF.copy()
        
        df.loc[:, "close_by"] = LOCATION_INFO_DF['geometry'].apply(lambda x: x.distance(street.shapely_linestring) < 50)
        close_by = df.loc[df["close_by"] == True, :].copy()
        
        if(len(close_by.index) > 0):
            # rank by shared path length in direction of street
            close_by.loc[:, "shared_path"] = close_by['geometry'].apply(lambda x: shared_paths(x, street.shapely_linestring).geoms[0].length)
            address_info = close_by.loc[close_by['shared_path'].idxmax(), :].to_dict()
            
            if address_info["shared_path"] > 1:
                return AddressInfo.from_dict(address_info)
               
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
            id = to_md5(street_segment.wkt)            
        )

    if not has_numbers(location_info[0]) and has_numbers(location_info[1]):
        # first element is a place name, like a shop
        return AddressInfo(
            street_number=location_info[1],
            street=location_info[2],
            neighborhood=location_info[3],
            district=location_info[4],
            id = to_md5(street_segment.wkt)            
        )
    
    if not has_numbers(location_info[0]) and not has_numbers(location_info[1]):
        # does not contain a street number at all
        return AddressInfo(
            street_number="null",
            street=location_info[0],
            neighborhood=location_info[1],
            district=location_info[2],
            id = to_md5(street_segment.wkt)            
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
