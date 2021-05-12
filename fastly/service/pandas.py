import re

import pandas as pd
import numpy as np
import datetime


class ETLPandasService:
    def __init__(self):
        self.df = ''

    def getdf(self):
        return self.df

    def regex_substring_count(self, regex, x):
        if re.search(regex, x):
            return 1
        return 0

    def match(self, regex, x, group=1):
        if re.search(regex, x):
            return re.search(regex, x).group(group)
        return ''

    def mutiple_regex_condition(self, lst, x):
        for condition in lst:
            if self.regex_substring_count(condition, x) == 0:
                return False
        return True

    def client_req(self, x):
        if x > 0:
            return 0
        elif x == 0:
            return 1
        else:
            return x

    def etl(self, jsonObj):
        print("Creating Dataframe...")

        try:
            # convert a dict of data to pandas dataframe
            self.df = pd.DataFrame.from_dict(jsonObj)

            print("Cleaning up the data...")
            # update all empty string to NaN
            self.df = self.df.replace(r'^\s*$', np.nan, regex=True)

            # update column type in dataframe
            updateArr = ['initial_status', 'final_status',
                         'response_header_size', 'response_body_size']

            for column in self.df.columns:
                if column in updateArr:
                    self.df[column] = self.df[column].astype('int')

            # Extract useful information from raw data
            self.df['timestamp'] = pd.to_datetime(
                self.df['timestamp'], format='%m/%d/%Y %H:%M:%S.00 %Z', utc=True)
            self.df['timestamp'] = self.df['timestamp'].dt.strftime(
                '%Y-%m-%d %H:%M:00')
            self.df['DNT'] = self.df['DNT'].fillna(0).astype('int')
            self.df['client_request'] = self.df['client_request'].fillna(
                -1).astype('int')

            print("Creating additional columns...")
            self.df['status'] = self.df['initial_status']
            self.df['size'] = self.df['response_header_size'] + \
                self.df['response_body_size']

            self.df['channel_id'] = self.df['url'].apply(
                lambda x: self.match(r"\/(\d+)\/", x))
            self.df['distributor'] = self.df['url'].apply(
                lambda x: (self.match(r"\/(dist|mt)\/((\w+|\d+|\-*)+)", x, group=1) + '-' + self.match(r"\/(dist|mt)\/((\w+|\d+|\-*)+)", x, group=2).title().replace("-", "_")))
            self.df['minutes_watched'] = self.df['url'].apply(
                lambda x: self.regex_substring_count(r"(playlist.+\.m3u8)|(chunklist.*\.m3u8)|(\d+.m3u8)", x)).astype('int')*6/60
            self.df['channel_start'] = self.df['url'].apply(
                lambda x: self.regex_substring_count(r"(?![chunklist])(\w|\d)+\.m3u8", x)).astype('int')
            self.df['count'] = 1
            self.df['count_720p'] = self.df['url'].apply(
                lambda x: 1 if self.mutiple_regex_condition([r"(chunklist\.m3u8)", r"720p"], x) else 0).astype('int')
            self.df['count_1080p'] = self.df['url'].apply(
                lambda x: 1 if self.mutiple_regex_condition([r"(chunklist\.m3u8)", r"1080p"], x) else 0).astype('int')
            self.df['between_720p_and_1080p_count'] = self.df['url'].apply(
                lambda x: 1 if self.mutiple_regex_condition([r"(chunklist\.m3u8)", r"\d{3,4}p"], x) and int(re.search(r"(\d{3,4})p", x).group(1)) > 720 and int(re.search(r"(\d{3,4})p", x).group(1)) < 1080 else 0).astype('int')
            self.df['under_720p_count'] = self.df['url'].apply(
                lambda x: 1 if self.mutiple_regex_condition([r"(chunklist\.m3u8)", r"\d{3,4}p"], x) and int(re.search(r"(\d{3,4})p", x).group(1)) < 720 else 0).astype('int')
            self.df['over_1080p_count'] = self.df['url'].apply(
                lambda x: 1 if self.mutiple_regex_condition([r"(chunklist\.m3u8)", r"\d{3,4}p"], x) and int(re.search(r"(\d{3,4})p", x).group(1)) > 1080 else 0).astype('int')
            self.df['city'] = self.df['city'].apply(lambda x: str(x).title())
            self.df['debug_url'] = np.where(np.logical_or(np.logical_or(
                self.df['channel_id'].isnull(), self.df['distributor'] == '-'), np.logical_and(self.df['status'] >= 400, self.df['status'] < 500)), self.df['url'], '')
            self.df['client_request'] = self.df['client_request'].apply(self.client_req).astype(
                'int')

            self.df = self.df.drop(columns=['response_header_size', 'response_body_size',
                                            'url', 'initial_status', 'final_status'])

            print("Performing ETL...")
            # create aggregated dataframe
            self.df = self.df.groupby(by=['timestamp', 'status', 'channel_id',
                                          'distributor', 'city', 'country', 'region', 'continent', 'debug_url', 'client_request']).sum().reset_index()

            # update the dataframe before converting it to numpy array
            self.df = self.df.rename(columns={'timestamp': 'timestamps',
                                              'size': 'request_size_bytes', 'count': 'request_count'})

            # rearranging the column order
            cols = self.df.columns.tolist()
            cols = ["timestamps", "status", "channel_id", "distributor", "city", "country", "region", "continent", "minutes_watched", "channel_start",
                    "request_size_bytes", "request_count", "count_720p", "count_1080p", "between_720p_and_1080p_count", "under_720p_count", "over_1080p_count", "debug_url", "client_request"]
            # cols = cols[0:8] + cols[11:13] + \
            #     cols[10:11] + cols[13:] + cols[8:9]

            self.df = self.df[cols]

            print("ETL completed")
        except pd.errors as err:
            print("Panda errors! Possible data corruption")
            raise err
