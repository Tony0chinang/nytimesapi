import argparse
import logging
from requests import Session

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""


log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        self.apiurl = 'https://api.nytimes.com'
        self.headers = {'Accepts': 'application/json',}
        self.session = Session()
        self.session.headers.update(self.headers)
        print("connecting to: ", self.apiurl)

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

        #connect and pull the result nytime's API
        #the returned data is add into the class object[r]
        url = self.apiurl + '/svc/search/v2/articlesearch.json'
        parameters = {'q': 'Silicon Valley', 'api-key': 'Gdcu3g88cGJsE7jqPca2v6OE0op4aduW'}
        self.r = self.session.get(url, params=parameters)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def flat_dict(self, d, parent_key: str = '', sep: str ='.'):
            #this method will restructure the data from multi-level structure 
            # into a single level structure (flat structure).
            
            fitems = []
            
            for k, v in d.items():
                new_key = parent_key + sep + k if parent_key else k

                if (type(v) is dict):
                    fitems.extend(source.flat_dict(v, new_key, sep=sep).items())

                elif (type(v) is list):               
                    for i in range(len(v)):
                        fitems.extend(source.flat_dict(v[i], new_key, sep=sep).items())

                else:
                    fitems.append((new_key, v))
    
            return dict(fitems)

    def getDataBatch(self, batch_size):
        
        self.connect()
        data = self.r.json()['response']['docs']
        
        ny = 0
        while ny < batch_size:
            newdata = {}
            
            newdata = source.flat_dict(data[ny])
            #print(newdata)
            ny = ny+1

            yield [newdata]

        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        # TODO: implement - this dummy implementation returns one batch of data
        yield [
            {
                "headline.main": "The main headline",
                "_id": "1234",
            }
        ]

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema


if __name__ == "__main__":
    config = {
        "api_key": "NYTIMES_API_KEY",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f"  - {item['_id']} - {item['headline.main']}")
