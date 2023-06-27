from geopy.geocoders import Nominatim
import numpy as np
import pickle 
from google.cloud import storage


def mutually_exclusive(list1,list2):
    return list(set(list1)^set(list2))


def geopy_co_ordinates(unique_elements, Hmap):
    geolocator = Nominatim(user_agent="MyApp")
    new_Hmap = Hmap
    keys = list(Hmap.keys())
    origins_geopy = mutually_exclusive(keys,unique_elements)
    for i in origins_geopy:
        location = geolocator.geocode(i, timeout=None)
        if location:
            a = [(location.latitude, location.longitude)]
        else:
            a = [('53.4808, 2.2426')]
        b = dict(zip([i],a))
        new_Hmap = {**b, **new_Hmap}
    
    return new_Hmap


def check_local_cache(unique_elements):
    storage_client = storage.Client()
    bucket = storage_client.bucket('eco-project-bucket-processed')
    blob = bucket.blob('saved_dictionary.pkl')               #path to the storage bucket
    pickle_in = blob.download_as_string()
    Hmap = pickle.loads(pickle_in)



    '''with open('saved_dictionary.pkl', 'rb') as f:
            Hmap = pickle.load(f)'''

    if all(name in Hmap for name in unique_elements):
            #df['destination_co_ordinates'] = df['destination'].map(Hmap)
            return Hmap
    else:
        new_Hmap = geopy_co_ordinates(unique_elements, Hmap)
        '''with open('saved_dictionary.pkl', 'wb') as f:
            pickle.dump(new_Hmap, f)'''

        pickle_out = pickle.dumps(new_Hmap)
        blob.upload_from_string(pickle_out)
        #df['destination_co_ordinates'] = df['destination'].map(new_Hmap)
        return new_Hmap
