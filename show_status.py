import pickle
import pprint

# read

with open('./docs/record.pickle', 'rb') as handle:
    dict_data = pickle.load(handle)

pp = pprint.PrettyPrinter(indent=4)

pp.pprint(dict_data)
