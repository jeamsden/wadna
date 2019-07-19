import pandas as pd
import requests
from functools import partial

class Dataset():
    #class variables
    samples = {
        'waterloo': {
            'df': None,
            'load_type': 'feature_json',
            'url': 'https://services.arcgis.com/ZpeBVw5o1kjit7LT/arcgis/rest/services/CyclingInfrastructure/FeatureServer/0/query',
            'parameters': { 'where': '1=1','f': 'json', 'outFields': '*'},
            'depth': ['features'],
            'etl': [
            {
                'function': partial(pd.DataFrame.rename),
                'payload': {'columns': {'ASSET_ID': 'ASS_ID'}}
            },
            {
                'function': partial(pd.DataFrame.reset_index),
                'payload': None
            },
        ]
        },
        'basic': {
            'df': pd.DataFrame(data={'name': ['Jim', 'Jane', 'Jill'], 'sex': ['male', 'female', 'female']}),
            'load_type': 'simple',
            'etl': []
        }
    }

    datasource = samples

    def __init__(self, name='basic'):
        self.published_df = None,
        self.queued_df = None,
        self.url = None,
        self.type = None,
        self.dataset_name = name
    
    def load(self):
        load_type = self.datasource[self.dataset_name]['load_type']
        self.queued_df = self.loads[load_type](self, self.dataset_name)

    def simple_load(self, sample_name):
        return self.samples[sample_name]['df']

    def feature_json_load(self, sample_name):
        r = requests.get(url=self.samples[sample_name]['url'], params=self.samples[sample_name]['parameters'])
        data = []
        for feature in r.json()['features']:
            data.append(feature['attributes'])
        self.samples[sample_name]['df'] = pd.DataFrame(data)
        return self.samples[sample_name]['df']

    def etl(self):
        for step in range(len(self.datasource[self.dataset_name]['etl'])):
            try:
                self.queued_df = self.datasource[self.dataset_name]['etl'][step]['function'](self.queued_df, **self.datasource[self.dataset_name]['etl'][step]['payload'])
            except:
                self.queued_df = self.datasource[self.dataset_name]['etl'][step]['function'](self.queued_df)
        self.published_df = self.queued_df

    loads = {
        'simple': partial(simple_load),
        'feature_json': partial(feature_json_load)
    }