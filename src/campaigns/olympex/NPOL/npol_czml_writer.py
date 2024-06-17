import numpy as np
from copy import deepcopy
import json
from helper.sample_data import sample_czml

czml_head = sample_czml[0]
model = sample_czml[1]

# class declaration

class NpolCzmlWriter:
    """
     Description.
     one czml will be created for a day.
     And the czml will track all the 3d tiles available throughout the day.
     It will also set the availability of those 3d tiles.
     So not all 3d tiles will be visible at once.
    
    Further details:
    The czml generation needs some variables where as some static properties:
    varying things:
      - id: to identify each npol 3dtiles uniquely.
      - availability: time span for the visibility of the 3dtile.
              i.e. the 3d tile collected with frequency of 20 mins each date.
            - availability is range. Determines when the 3d tile is shown and till when.
            - start point is easy to find. i.e. the start date time is embedded in the filename
            - but the end date time is not available.
            - will have to depend on the next filename, to determine the end date time.
              This class won't calculate it. So, do it outside of this class and pass in the values
      - tileset.uri: the url of the 3dtile to be shown.
    """

    def __init__(self):
      self.model = deepcopy(model)
      self.czml_data = [czml_head]
    
    def add3dTiles(self, id, tileset_url, start_date_time, end_date_time):
      """add 3d tileset url, that will be only available for a certain time.

      Args:
          id (number): unique identifier
          tileset_url (string): complete url for the 3dtile data (public s3 url)
          start_date_time (string): The time when the 3dtile is available/loaded/visible. Should be in the format "YYYY-MM-DDTHH:MM:00Z"
          end_date_time (string): The time when the 3dtile ceases to exist/is_removed. Should be in the format "YYYY-MM-DDTHH:MM:00Z"
      """
      new_node = deepcopy(self.model)
      new_node['id'] = f"npol-3dtile-{id}"
      new_node['availability'] = f"{start_date_time}/{end_date_time}"
      new_node["tileset"]["uri"] = tileset_url
      self.czml_data.append(new_node)

    def get_string(self):
      """get the final czml

      Returns:
          string: czml data in string, that can be stored elsewhere.
      """
      return json.dumps(self.czml_data)
