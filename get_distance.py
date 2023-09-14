import requests
from haversine import haversine
import pickle
import json
from google.cloud import storage


def load_distance_cache():
    '''with open('distance_cache.pkl', 'rb') as f:
            Hmap = pickle.load(f)'''
    storage_client = storage.Client()
    bucket = storage_client.bucket('eco-project-bucket-processed')
    blob = bucket.blob('distance_cache.pkl')               #path to the storage bucket
    pickle_in = blob.download_as_string()
    Hmap = pickle.loads(pickle_in)

    return Hmap


def write_data_to_distance_cache(new_Hmap):
    '''with open('distance_cache.pkl', 'wb') as f:
        pickle.dump(new_Hmap, f)
'''
    storage_client = storage.Client()
    bucket = storage_client.bucket('eco-project-bucket-processed')
    blob = bucket.blob('distance_cache.pkl')               #path to the storage bucket
    pickle_out = pickle.dumps(new_Hmap)
    blob.upload_from_string(pickle_out)


def get_distance(origin, destination, mode, Api_key, Hmap):
    """
    Calculates the distance and duration of travel between two locations using the Google Maps Distance Matrix API.

    Args:
        origin (tuple): The latitude and longitude of the origin location.
        destination (tuple): The latitude and longitude of the destination location.
        mode (str): The mode of travel (e.g., 'driving', 'walking', 'transit').
        Api_key (str): The API key for accessing the Google Maps Distance Matrix API.
        Hmap (dict): A dictionary to store the distance and duration data for different origin-destination-mode combinations.

    Returns:
        tuple: The distance and duration of travel between the origin and destination locations. The distance is in meters and the duration is in seconds.
    """

    origin_final = ','.join([str(i) for i in origin])
    dest_final = ','.join([str(i) for i in destination])
    unique_key = origin_final + dest_final + mode
    unique_key = unique_key.replace(',', '')

    dist = []
    duration = []
    if unique_key in Hmap:
        value = Hmap.get(unique_key)
        status = value["rows"][0]["elements"][0]["status"]
        if status == "OK":
            for row in value["rows"]:
                print(row["elements"])
                for distance in row["elements"]:
                    dist.append(distance["distance"]['value'])
                    print(distance)
                    for distance in row["elements"]:
                        duration.append(distance["duration"]['value'])
            distance = dist[0], duration[0]  # ,duration_in_traffic[0]

            # distance =  0.0,0.0

            return distance
        else:
            distance = distance_matrix_api_call(origin_final, dest_final, mode, Api_key, unique_key, Hmap)

    else:
        distance = distance_matrix_api_call(origin_final, dest_final, mode, Api_key, unique_key, Hmap)
        return distance





def distance_matrix_api_call(origin_final, dest_final, mode, Api_key, unique_key, Hmap):
    """
    Retrieves the distance and duration between two locations using the Google Distance Matrix API.

    Args:
        origin_final (string): The origin coordinates in the format "latitude,longitude".
        dest_final (string): The destination coordinates in the format "latitude,longitude".
        mode (string): The travel mode (e.g., "Train", "Car").
        Api_key (string): The API key for accessing the Google Distance Matrix API.
        unique_key (string): A unique key generated based on the origin, destination, and mode.
        Hmap (dictionary): A cache of previously calculated distances.

    Returns:
        tuple: The distance and duration between the origin and destination.

    """
    DISTANCE_MATRIX_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"
    api_key = Api_key
    params = {
        "destinations": dest_final,
        "origins": origin_final,
        "mode": mode,
        "key": Api_key,
    }
    response = requests.get(DISTANCE_MATRIX_URL, params=params)
    response_data = response.json()

    dist = []
    duration = []
    try:
        status = response_data["rows"][0]["elements"][0]["status"]
        if status == "OK":
            for row in response_data["rows"]:
                for distance in row["elements"]:
                    dist.append(distance["distance"]["value"])
                for distance in row["elements"]:
                    duration.append(distance["duration"]["value"])
        distance = dist[0], duration[0]

    except:
        distance = 0.0, 0.0

    a = dict(zip([unique_key], [response_data]))
    new_Hmap = {**a, **Hmap}
    # write_data_to_distance_cache(new_Hmap)

    return distance


def get_distance_flight(origin, destination):
    """
    Calculates the distance and time taken for a flight between two locations using the haversine formula.
    
    Args:
        origin (tuple): The latitude and longitude of the origin location.
        destination (tuple): The latitude and longitude of the destination location.
    
    Returns:
        distance (float): The distance between the origin and destination locations in meters.
        time (float): The time taken by an average plane to cover the distance in seconds.
    """
    try:
        if origin and destination:
            x = haversine(origin, destination, unit='mi')
            return x*1000, ((x/880)*60)*60 #return distance and time taken by the avg plane to cover that distance 880 km/h (according to wikipedia)
        else:
            return 0.0,0.0
    except:
        return 0.0,0.0






