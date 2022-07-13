from WMOMessage import WMOUpperAirMessage
import pandas as pd
import numpy as np

class WMOSounding():
    def __init__(self, **kwargs):
        self.time_str = kwargs.get("time_str", None)
        self.wmo_id = kwargs.get("wmo_id", None)
        self.messages = kwargs.get("messages", {})
        return


