import pandas as pd
import numpy as np

class WMOUpperAirMessage():
    def __init__(self, **kwargs):
        self.type = None
        self.message = None
        self.header = None
        self.time_str = None
        self.id = None
        self.transmission_code = None
        self.MISSING = -9999.0
        self.lvl_top = None
        stations_file = "/home/ldm/SHARP-api/snstns.tbl"
        table_names = ["Site ID", "WMO ID", "Site Name", "State", "Country", "Latitude", "Longitude", "Elevation", "Flag"]
        self.stations = kwargs.get("stations_df", None)
        if self.stations is None:
            self.stations = pd.read_fwf(stations_file, comment="!", names=table_names, dtype=str)

        ## These messages are hard stops - exit the loop.
        self.MSG_STOP = ["51515", "41414", "31313"]

        ## These messages are to be ignored - continue the loop
        self.MSG_PASS = ["88999","77999"]


    def set_message(self, message):
        self.type = message[0]
        message = message[1:]
        self.id = message[1]
        self.message = message

    def set_header(self, header):
        self.header = header
        self.time_str = header[2]
        if len(header) == 4: self.transmission_code = header[-1]

    def decode(self):
        if self.type in ["TTAA", "TTCC"]:
            levels = self._decode_mand()
            #print(self.time_str, self.id, self.type)
            #for lev in levels: print(lev)
            return

        if self.type in ["TTBB", "TTDD"]:
            levels = self._decode_sigt()
            #print(self.time_str, self.id, self.type)
            #for lev in levels: print(lev)
            return

        if self.type in ["PPBB", "PPDD"]:
            levels = self._decode_sigw()
            #print(self.time_str, self.id, self.type)
            #for lev in levels: print(lev)
            return


    def _decode_mand(self):
        """
        Decode the TTAA block (mandatory levels) and store the data.
        """
        res_dicts = []

        datestr = self.message[0] ## Date block is always the first item
        wmo_id  = self.message[1] ## WMO ID is always the second item

        ## Parse the date string to get the day, hour,
        ## and top wind report level
        day, hour, self.lvl_top, wind_in_kts = self._get_date_and_top_from_rpt(datestr)
        if self.lvl_top == "/": return res_dicts

        ## index 0 is the time string,
        ## index 1 is the WMO ID
        idx = 2
        while idx < len(self.message):
            rpt = self.message[idx]
            if rpt in self.MSG_STOP: break
            if rpt in self.MSG_PASS: 
                idx += 1
                continue

            res, idx = self._lvl_mand(self.message, idx)
            res_dicts.append(res)
            idx += 1

        return res_dicts

    def _decode_sigt(self):
        """
        Decode the TTBB and TTDD blocks (significant levels) and store the data.
        """

        res_dicts = []

        datestr = self.message[0] ## Date block is always the first item
        wmo_id  = self.message[1] ## WMO ID is always the second item

        ## Parse the date string to get the day, hour,
        ## and in this case, the equipment code (rather than max level)
        day, hour, equipment_code, wind_in_kts = self._get_date_and_top_from_rpt(datestr)

        ## index 0 is the time string,
        ## index 1 is the WMO ID
        idx = 2
        additional_winds = False
        while idx < len(self.message):
            rpt = self.message[idx]
            if rpt in self.MSG_STOP: break
            if rpt == "21212": 
                idx += 1
                additional_winds = True
                continue
            res = self._lvl_sigt(self.message, idx, additional_winds=additional_winds)
            res_dicts.append(res)
            idx += 2

        return res_dicts

    def _decode_sigw(self):
        """
        Decode the PPBB and PPDD (significant lelels) wind reports and 
        return the data.
        """
        res_dicts = []

        datestr = self.message[0] ## Date block is always the first item
        wmo_id  = self.message[1] ## WMO ID is always the second item

        ## Parse the date string to get the day, hour,
        ## and in this case, the equipment code (rather than max level)
        day, hour, equipment_code, wind_in_kts = self._get_date_and_top_from_rpt(datestr)

        idx = 2
        last_altitude_group = None
        winds_on_pressure_levs = False
        while idx < len(self.message):
            above_100kft = False
            rpt = self.message[idx]
            inc = 1
            ## These message codes represent the end of data we care 
            ## about here. 
            if rpt in self.MSG_STOP: break
            if rpt == "21212": 
                winds_on_pressure_levs = True
                idx += 1
                continue
            elif rpt[0] != "9" and not winds_on_pressure_levs: 
                ## Apparently if data is over 100kft it
                ## just wraps over?
                last_alt_code = last_altitude_group[:2]
                if (rpt[:2] == "10" or rpt[:2] == "11") and (last_alt_code == "99" or last_alt_code == "10"):
                    above_100kft = True
                else:
                    break

            if not winds_on_pressure_levs:
                ## The first digit following the 9 is the ten thousands
                ## digit of the height of the report
                if not above_100kft:
                    hght_mod = float(rpt[1]) * 10000 
                else: hght_mod = float(int(rpt[:2])) * 10000 

                ## The next 3 digits contain the height in thousands of feet. 
                ## Add the ten thousands modifier to it to get the true height
                if rpt[2] == "/": h1 = -1
                else: h1 = (float(rpt[2]) * 1000) + hght_mod

                if rpt[3] == "/": h2 = -1
                else: h2 = (float(rpt[3]) * 1000) + hght_mod

                if rpt[4] == "/": h3 = -1
                else: h3 = (float(rpt[4]) * 1000) + hght_mod

                if h1 != -1 and idx+1 < len(self.message):
                    res = self._lvl_sigw(self.message, idx+1)
                    res["hght"] = 0 if h1 == 0 else h1 / 3.281
                    res_dicts.append(res)
                    inc += 1

                if h2 != -1 and idx+2 < len(self.message):
                    res = self._lvl_sigw(self.message, idx+2)
                    res["hght"] = h2 / 3.281
                    res_dicts.append(res)
                    inc += 1

                if h3 != -1 and idx+3 < len(self.message):
                    res = self._lvl_sigw(self.message, idx+3)
                    res["hght"] = h3 / 3.281
                    res_dicts.append(res)
                    inc += 1

            else:
                if self.type == "PPBB": lvl = int(self.message[idx][2:])
                elif self.type == "PPDD": lvl = int(self.message[idx][2:]) / 10.0
                idx += 1

                res = self._lvl_sigw(self.message, idx)
                res["lvl"] = lvl
                res_dicts.append(res)

            idx += inc
            last_altitude_group = rpt

        return res_dicts


    def _decode_ppaa(self):
        pass

    def _decode_ppcc(self):
        pass

    def _lvl_mand(self, rpt_list, idx):
        """
        Decodes the mandatory level messages. The variable idx is used
        to access the temperature and wind entries immediately following the 
        mandatory level info, and idx is incremented within this function 
        so that in its next iteration, it hits the next mandatory level. 

        This function returns a dictionary with the returned pressure, height, temperature,
        and wind data (as well as some intermediate indices needed while computing) along
        with the appropriately incremented idx value. 
        """
        code = rpt_list[idx]

        ## Get the level in mb
        l1 = code[:2]
        if "/" in l1 or "\\" in l1: l1 = self.MISSING 
        else: l1 = int(l1)

        ## Get the height in meters
        misg = 0
        h1 = code[2:]
        if "/" in h1 or "\\" in h1: h1 = self.MISSING
        else: h1 = int(h1)

        if (h1 == self.MISSING): misg = 1
        ## p1 and p2 are pointers to messages
        ## trop is a flag (1 or 0) if tropopause is found
        p1 = 1
        p2 = 2
        trop = 0

        ## Python doesn't do switch/case control flow.
        ## This is my best attempt to reproduce this
        ## type of control flow from the original NSHARP
        ## code.
        switch_ttaa = \
        {
            0: 
            {
                "lvl": 1000,
                "hght": h1,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            99: 
            {
                "lvl": h1 + 1000 if h1 < 300 else h1,
                "hght": self._get_stn_elev(self.id), 
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            92: 
            {
                "lvl": 925, 
                "hght": h1,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            85: 
            {
                "lvl": 850, 
                "hght": h1 + 1000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            70: 
            {
                "lvl": 700, 
                "hght": h1 + 3000 if h1 < 500 else h1 + 2000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            50: 
            {
                "lvl": 500, 
                "hght": h1 * 10,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            40: 
            {
                "lvl": 400, 
                "hght": h1 * 10,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            30: 
            {
                "lvl": 300, 
                "hght": (h1 * 10) + 10000 if h1 < 300 else h1 * 10,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            25: 
            {
                "lvl": 250, 
                "hght": (h1 * 10) + 10000 if h1 < 600 else h1 * 10,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            20: 
            {
                "lvl": 200, 
                "hght": (h1 * 10) + 10000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            15: 
            {
                "lvl": 150, 
                "hght": (h1 * 10) + 10000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            10: 
            {
                "lvl": 100, 
                "hght": (h1 * 10) + 10000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            88: 
            {
                "lvl": h1, 
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": 1,
            },
            77: 
            {
                "lvl": h1, 
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": -1,
                "p2": 1,
                "trop": trop,
            },
            66: 
            {
                "lvl": h1, 
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": -1,
                "p2": 1,
                "trop": trop,
            },
            "default": 
            {
                "lvl": self.MISSING,
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
        }
        switch_ttcc = \
        {
            88: 
            {
                "lvl": h1 / 10.0, 
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": 1,
            },
            77: 
            {
                "lvl": h1 / 10.0, 
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": -1,
                "p2": 1,
                "trop": trop,
            },
            66: 
            {
                "lvl": h1 / 10.0, 
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": -1,
                "p2": 1,
                "trop": trop,
            },
            70: 
            {
                "lvl": 70,
                "hght": (h1 * 10) + 10000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            50: 
            {
                "lvl": 50, 
                "hght": (h1 * 10) + 10000 if h1 > 800 else (h1 * 10) + 20000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            30: 
            {
                "lvl": 30, 
                "hght": (h1 * 10) + 20000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            20: 
            {
                "lvl": 20, 
                "hght": (h1 * 10) + 20000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            10: 
            {
                "lvl": 10, 
                "hght": (h1 * 10) + 30000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            7: 
            {
                "lvl": 7, 
                "hght": (h1 * 10) + 30000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            5: 
            {
                "lvl": 5, 
                "hght": (h1 * 10) + 30000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            3: 
            {
                "lvl": 3, 
                "hght": (h1 * 10) + 30000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            2: 
            {
                "lvl": 2, 
                "hght": (h1 * 10) + 40000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            1: 
            {
                "lvl": 1, 
                "hght": (h1 * 10) + 40000,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
            "default": 
            {
                "lvl": self.MISSING,
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
                "p1": p1,
                "p2": p2,
                "trop": trop,
            },
        }

        inc = 0
        ## Now we get the result of the switch
        ## by calling the dictionary. If the key isn't found,
        ## use the default case.
        if self.type == "TTAA":
            switch = switch_ttaa
        ## The only other time this function
        ## should be called is if self.type == "TTCC"
        else:
            switch = switch_ttcc

        try:
            res = switch[l1]
        except:
            res = switch["default"]

        if (misg): res["hght"] = self.MISSING 

        if res["lvl"] == self.MISSING and res["hght"] == self.MISSING:
            return res, idx

        ## Decode the temperature and dewpoint data.
        ## p1 points to the next message which should contain temperature and dewpoint data
        ## p2 points to the message which should contain wind data
        if res["p1"] > 0:
            loc = idx + res["p1"]
            if loc >= len(rpt_list):
                return res, loc
            rpt = rpt_list[loc]

            tmpc, dwpc = self._get_t_and_td_from_rpt(rpt)

            ## Assign the values to the output dictionary
            res["tmpc"] = tmpc
            res["dwpc"] = dwpc
            inc += 1

        ## Decode the wind speed and direction data.
        if res["lvl"] >= self.lvl_top or res["p2"] == 1 or res["trop"] == 1:
            loc = idx + res["p2"]
            if loc >= len(rpt_list):
                return res, loc
            rpt = rpt_list[loc]

            wdir, wspd = self._get_spd_and_dir_from_rpt(rpt)

            ## Assign the values to the output dictionary
            res["wspd"] = wspd
            res["wdir"] = wdir
            inc += 1

        elif res["lvl"] < self.lvl_top and res["hght"] != self.MISSING:
            wdir, wspd = self._get_spd_and_dir_from_rpt(rpt)

            ## Assign the values to the output dictionary
            res["wspd"] = wspd
            res["wdir"] = wdir
            inc += 1

        ## When dealing with the max wind group, we need
        ## to check for the presence of a wind shear code
        ## in order to increment properly
        if l1 == 77 or l1 == 66:
            if idx + 2 < len( rpt_list ): 
                rpt = rpt_list[idx + 2]
                if rpt.startswith("4"): inc += 1


        ## Increment our index based on the number
        ## of reports decoded thus far
        idx += inc 
        return res, idx 

    def _lvl_sigt(self, rpt_list, idx, additional_winds=False):
        """
        Decodes the significant level messages. The variable idx is used
        to access the temperature and wind entries immediately following the 
        significant level info. Unlike in self.lvl_man, the idx variable is not
        incremented by this function. 

        This function returns a dictionary with the returned pressure, height, temperature,
        and wind data. Height and wind are technically not part of the significant level 
        data. Height needs to be interpolated later and wind comes from the PPBB message. 
        """

        ## Return the result in a similarly formatted dictionary
        res = \
            {
                "lvl": self.MISSING,
                "hght": self.MISSING,
                "tmpc": self.MISSING,
                "dwpc": self.MISSING,
                "wdir": self.MISSING,
                "wspd": self.MISSING,
            }

        ## level mb
        data = rpt_list[idx]
        sigidx = data[:2]

        if "/" in data[2:]: return res
        elif data == "NIL": return res


        if self.type == "TTBB": lvl = int(rpt_list[idx][2:])
        elif self.type == "TTDD": lvl = int(rpt_list[idx][2:]) / 10.0

        ## 00 designates the surface block - everything else is in 
        ## the range of 11, 22, 33 ... 99, folding back to 11 and so on.
        ## The surface pressure may need modification so take care of that
        if sigidx == "00":
            lvl = lvl + 1000 if lvl < 300 else lvl
        ## Significant levels when pressure > 1000 hPa
        elif self.type == "TTBB" and lvl < 100:
            lvl = lvl + 1000

        data = rpt_list[idx+1]
        ## The TTBB/TTDD blocks can have wind info 
        ## on pressure surface in them because reasons
        if not additional_winds:
            tmpc, dwpc = self._get_t_and_td_from_rpt(data)
            res["lvl"] = lvl
            res["tmpc"] = tmpc
            res["dwpc"] = dwpc
        else:
            wdir, wspd = self._get_spd_and_dir_from_rpt(data)
            res["lvl"] = lvl
            res["wdir"] = wdir
            res["wspd"] = wspd

        return res

    def _lvl_sigw(self, messages, idx):
        """
        Parse a PPBB or PPDD wind report.
        """

        rpt = messages[idx]

        wdir, wspd = self._get_spd_and_dir_from_rpt(rpt)

        res = \
            {
                "lvl": -999,
                "hght": -999,
                "tmpc": -999,
                "dwpc": -999,
                "wdir": wdir,
                "wspd": wspd,
            }
        return res
    
    def _get_stn_elev(self, wmo_id):
        try:
            elev = float(self.stations[self.stations["WMO ID"] ==  str(wmo_id)]["Elevation"])
            return elev
        except:
            print(self.stations[self.stations["WMO ID"] == str(wmo_id)])
            print("Unable to find station: ", wmo_id)
            return 0
            

    def _get_t_and_td_from_rpt(self, rpt):
        """
        Parse a text string containing temperature
        and dewpoint depression information and return
        the decoded values.
        """

        ## decode the temperature data. Fill with
        ## a missing value if necessary.
        if "/" in rpt[:3] or "\\" in rpt[:3]: tmpc = self.MISSING 
        else: 
            tmpc = float(rpt[:3] ) / 10.0
            if int(rpt[:3]) % 2: tmpc *= -1

        ## Decode the dewpoint data. Fill with
        ## a missing value if necessary
        if "/" in rpt[3:] or "\\" in rpt[3:]: 
            dewpoint_depression = self.MISSING 
        else: 
            dewpoint_depression = float(rpt[3:])
            if (dewpoint_depression <= 55.0): dewpoint_depression *= .1
            else: dewpoint_depression -= 50.0

        if dewpoint_depression == self.MISSING: dwpc = self.MISSING
        else: dwpc = tmpc - dewpoint_depression
        return tmpc, dwpc

    def _get_spd_and_dir_from_rpt(self, rpt):
        """
        Parse a text string containing wind speed
        and direction information and return the
        decoded values.
        """
        if "/" in rpt or "\\" in rpt: 
            wspd = self.MISSING
            wdir = self.MISSING
            return wdir, wspd
        ## Wind direction. 
        wdir = float(rpt[:3])
        
        ## Wind speed. 
        wspd = float(int(rpt[3:]) + (wdir % 5) * 100.0)

        return wdir, wspd

    def _get_date_and_top_from_rpt(self, rpt):
        """
        Parse the date report string. Also contains info
        for whether or not wind reports are in knots, and
        the last pressure level with wind data.
        """
        ## Day/time group - subtract 50 from the first
        ## 2 digits to obtain day, the next 2 digits are the
        ## UTC hour, and the last digit denotes wind data level
        if int(rpt[:2]) > 50:
            day = str( int(rpt[:2]) - 50 )
            wind_in_kts = True
        else: 
            day = str( int(rpt[:2]) )
            wind_in_kts = False
        hour = rpt[2:4]
        if rpt[-1] != "/" and rpt[-1] != "\\": 
            if self.type == "TTAA":
                rpt_top = int(rpt[-1]) * 100
            else:
                rpt_top = int(rpt[-1]) * 10
        else: rpt_top = "/"

        return day, hour, rpt_top, wind_in_kts

