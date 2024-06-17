
# A sample czml that will display the nexrad time dynamic imagery data in cesium
sample_czml = [
    {
      "id": "document",
      "name": "CZML NEXRAD",
      "version": "1.0",
      # "clock": {
      #   "interval": "2015-09-22T22:28:00Z/2015-09-22T23:58:00Z",
      #   "currentTime": "2015-09-22T22:28:00Z",
      #   "multiplier": 20,
      # },
    },
    {
      "id": "textureRectangle1",
      "name": "Rectangle area with nexrad image, above surface",
      "availability": "2015-09-22T22:29:00Z/2015-09-22T22:38:00Z",
      "rectangle": {
        "coordinates": {
          "wsenDegrees": [-123.197, 48.735, -121.812, 49.653],
        },
        "height": 0,
        "fill": True,
        "material": {
          "image": {
            "image": { "uri": "https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/Olympex/instrument-raw-data/nexrad/katx/2015-09-22/olympex_Level2_KATX_20150922_2229_ELEV_01.png" },
            "color": {
              "rgba": [255, 255, 255, 128],
            },
          },
        },
      },
    },
    {
      "id": "textureRectangle2",
      "name": "Rectangle area with nexrad image, above surface",
      "availability": "2015-09-22T22:38:00Z/2015-09-22T22:48:00Z",
      "rectangle": {
        "coordinates": {
          "wsenDegrees": [-123.197, 48.735, -121.812, 49.653],
        },
        "height": 0,
        "fill": True,
        "material": {
          "image": {
            "image": { "uri": "https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/Olympex/instrument-raw-data/nexrad/katx/2015-09-22/olympex_Level2_KATX_20150922_2238_ELEV_01.png", },
            "color": {
              "rgba": [255, 255, 255, 128],
            },
          },
        },
      },
    },
  ]