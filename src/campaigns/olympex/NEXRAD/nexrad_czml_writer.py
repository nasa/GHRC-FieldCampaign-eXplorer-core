import numpy as np
from copy import deepcopy
import json
from helper.sample_data import sample_czml

czml_head = sample_czml[0]
model = sample_czml[1]

# class declaration

class NexradCzmlWriter:  
    """
    The czml generation needs some variables where as some static properties:
    varying things:
      - id: to identify each nexrad png image uniquely.
      - availability: time span for the visibility of the rectangle. rectangle used to show the image
            - availability is range. Determines when the rectangle is shown till when.
            - start point is easy to find. i.e. the start date time is embedded in the filename
            - but the end date time is not available.
            - will have to depend on the next filename, to determine the end date time.
      - coordinates: span for the rectangle to be shown.
            - is actually a constant for a single ground station.
      - image.uri: the url of the image to be shown in top of the rectangle.
    """

    def __init__(self, location, image_height=0, background_rgba_color=[255, 255, 255, 128]):
      """ during object initalization

      Args:
          location (array): degree cordinates in the form "[west south east north]"
          image_height (int, optional): The height from surface, where the image is to be shown. Defaults to 0.
          background_rgba_color (list, optional): The rgba color for the background of the image shown. Defaults to [255, 255, 255, 128].
      """
      self.model = deepcopy(model)

      #some generic properties
      self.model["rectangle"]["coordinates"]["wsenDegrees"] = location
      self.model["rectangle"]["height"] = image_height
      self.model["rectangle"]["material"]["image"]["color"]["rgba"] = background_rgba_color

      self.czml_data = [czml_head]
      self.location = location
      self.image_height = image_height
      self.background_rgba_color = background_rgba_color
    
    def addTemporalImagery(self, id, imagery_url, start_date_time, end_date_time):
      """add imagery url, that is only available for a certain time.

      Args:
          id (number): unique identifier
          imagery_url (string): complete url for the nexrad imagery data (public s3 url)
          start_date_time (string): The time when the imagery is available/visible. Should be in the format "YYYY-MM-DDTHH:MM:00Z"
          end_date_time (string): The time when the imagery ceases to exist. Should be in the format "YYYY-MM-DDTHH:MM:00Z"
      """
      new_node = deepcopy(self.model)
      new_node['id'] = f"nexrad-imagery-{id}"
      new_node['availability'] = f"{start_date_time}/{end_date_time}"
      new_node["rectangle"]["material"]["image"]["image"] = imagery_url
      self.czml_data.append(new_node)

    def get_string(self):
      """get the final czml

      Returns:
          string: czml data in string, that can be stored elsewhere.
      """
      return json.dumps(self.czml_data)
