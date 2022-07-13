from WMOMessage import WMOUpperAirMessage
#from WMOData import WMOSounding
import pandas as pd
import numpy as np
import sys, os
import glob

class WMOReader():
    def __init__(self, filename):
        self.filename = filename
        self.headers = ["TTAA", "TTBB", "PPBB", "PPDD", "TTCC", "TTDD", "PPAA", "PPCC"]
        self.ignore = ["", "\n\n\n", "\n\n", [""], [], "\n"]
        self.transmissions = []
        self.records = {}
        self.text = None
        stations_file = "/home/ldm/SHARP-api/snstns.tbl"
        table_names = ["Site ID", "WMO ID", "Site Name", "State", "Country", "Latitude", "Longitude", "Elevation", "Flag"]
        self.stations = pd.read_fwf(stations_file, comment="!", names=table_names, dtype=str)
        with open(self.filename, "r", newline="") as snfile:
            print("FILE: ", self.filename)
            self._parse(snfile.read())

        for tid in list(self.records.keys()):
            for sid in list(self.records[tid].keys()):
                #snd = self.create_sounding(tid, sid)
                #if snd is None: continue
                for msg in self.records[tid][sid].keys():
                    wmo_msg = self.records[tid][sid][msg]
                    print(self.filename, tid, sid, msg) 
                    wmo_msg.decode()
                #print("Time: {time}\tSite ID: {sid}".format(time=tid, sid=sid))

    def create_sounding(self, record_time, site_id):
        """
        For a given record time and site id, construct 
        and return a WMOSounding object. If no TTAA message
        is found, which is required to construct a full profile,
        then print a warning and return None. 
        """
        record = self.records[record_time][site_id]
        if "TTAA" not in list(record.keys()):
            warning = "WARNING: Skipping {time}/{id} because no TTAA message was found."
            warning += "\nMessages in the record: {msgs}"
            warning += "\nFilename: {fname}"
            warning = warning.format(time=record_time, id=site_id, msgs=list(record.keys()), fname=self.filename)
            print(warning)
            return None
        sounding = WMOSounding()
        sounding.messages = record
        sounding.time_str = record_time
        sounding.wmo_id = site_id
        return sounding

    def _add_time_to_record(self, time_str):
        """
        Add a new time entry to the record. When corrected broadcasts
        are sent, sometimes the headers list the time as a few minutes 
        past the hour. This handles grouping the times to the synoptic 
        hour if the time is past (not before) and returns the proper
        time_str if corrected. 
        """

        ## If there are times in the record, compare the
        ## record and the current time to determine which
        ## one corresponds to the synoptic time.
        times = list(self.records.keys())
        for time in times:
            ## Check to see if the times are from the same day/hour
            if time.startswith(time_str[:4]):
                ## Which one is the synoptic time?
                if time[-2:] == "00": 
                    synop = time
                    other = time_str
                else: 
                    synop = time_str
                    other = time
                remainder = abs(int(synop[-2:]) - int(other[-2:]))
                if remainder <= 10: time_str = synop
        try:
            data = self.records[time_str] 
        except:
            self.records[time_str] = {}
        return time_str

    def _add_stn_to_record(self, time_str, wmo_num):
        try: 
            data = self.records[time_str]
            stn = data[wmo_num]
        except:
            data = self.records[time_str]
            data[wmo_num] = {}

    def _parse(self, text):
        """
        Parses the supplied raw text and creates instances of WMOUpperAirMessage
        and appending the messages to self.messages
        """
        self._parse_for_transmissions(text)

        for tidx in range(len(self.transmissions)):
            header, messages = self._format_messages(self.transmissions[tidx])
            
            ## Iterate over the formatted messages
            for message in messages:
                ## These messages usually are NIL transmissions
                if len(message) <= 2: continue
                ## Check a little more explicitly for NIL transmissions
                ## just in case...
                missing = False
                if message[0].upper() in ["/////", "MISDA", "SUSPENDED", "NIL", "NILL", "NNNN", "XMTD", "@"]: continue
                if message[1].upper() in ["/////", "MISDA", "SUSPENDED", "NIL", "NILL", "NNNN", "XMTD", "@"]: continue


                ## Construct a WMO Message and set the attributes
                ## while passing through an already opened pandas
                ## dataframe of stations. Not doing this takes a massive
                ## performance hit
                wmo_msg = WMOUpperAirMessage(stations_df=self.stations)
                ## Set the WMO message header
                wmo_msg.set_header(header)
                wmo_msg.set_message(message)

                ## Create dictionary record entries if they 
                ## do not already exist
                time_str = self._add_time_to_record(wmo_msg.time_str)
                ## When adding the time to the record, it groups observations
                ## within 10 minutes of the synoptic time. Re-set the time_str
                ## in case this grouping happens. 
                wmo_msg.time_str = time_str
                self._add_stn_to_record(wmo_msg.time_str, wmo_msg.id)
                ## Check for retransmissions. If there is a previous entry
                ## for this record, compare the headers to determine
                ## what to do or which one to keeo
                try:
                    old_record = self.records[wmo_msg.time_str][wmo_msg.id][wmo_msg.type]
                    retr_code_1 = old_record.transmission_code
                    retr_code_2 = wmo_msg.transmission_code
                    ## Sometimes there's no rebroadcast header but still two entries
                    ## in the file. If so, take the longer of the two entries
                    if retr_code_2 is None and retr_code_1 is None:
                        if len(old_record.message) > len(wmo_msg.message): wmo_msg = old_record
                        else: pass 
                    elif retr_code_1 > retr_code_2: wmo_msg = old_record
                    else: pass 
                except: pass
                self.records[wmo_msg.time_str][wmo_msg.id][wmo_msg.type] = wmo_msg

    def _parse_for_transmissions(self, text):
        """
        Parses the raw text string and splits the string into it's
        individual LDM transmissions. This is done by splitting the
        string on the End Of Transmission ('\x03') character. The 
        '\r' and Start Of Transmission ('\x01') characters are removed.

        Each transmission is split into it's individual messages by breaking
        on the '=' character. 

        Sets the class attribute self.transmissions, which is a list of lists.
        Each entry is a list of the messages in each transmission.
        """
        all_transmissions = text.replace("\r", "").replace("\x01", "").split("\x03")

        for tidx in range(len(all_transmissions)):
            transmission = all_transmissions[tidx]
            if transmission in self.ignore: continue

            ## The "=" character marks the end of a message,
            ## so use that to divide up the transmission
            messages = transmission.split("=")
            messages = [msg for msg in messages if msg not in self.ignore]
            self.transmissions.append(messages)

    def _format_messages(self, messages):
        """
        Called after _parse_for_transmissions or after self.transmissions
        manually set. Iterates over the transmissions available assuming they
        have already been spit on the '=' character.
        """
        header = None
        messages_out = []
        for midx in range(len(messages)):
            ## Split our message on the newline first and then
            ## filter out the junk
            message = messages[midx].split("\n")
            message = [msg for msg in message if msg not in self.ignore]

            if len(message) == 1:
                message = message[0].split(" ")

            ## If we are the first message in the 
            ## transmission, get the header info
            if midx == 0:
                header = message[1].split(" ")
                message = message[2:]

            ## Find where our data starts by searching for a matching
            ## header string in the list
            start = [n for n in range(len(message)) for head in self.headers if head in message[n]]
            if len(start) > 0: start = start[0]
            else: start = 0
            message = message[start:]

            ## Take our message segments that were split on the newline character
            ## and then split those on the whitespace character. Append all of the
            ## data to a new ist that should contain nicely separated message data
            split_message = []
            for n in range(len(message)):
                msg = message[n].split(" ")
                [split_message.append(m) for m in msg if m not in self.ignore]
                #split_message += msg
            #split_message = [m for m in split_message if m not in self.ignore]
            ## Find where our data header is in this new list
            start = [n for n in range(len(split_message)) for head in self.headers if head in split_message[n]]
            if len(start) > 0:
                start = start[0]
            else:
                start = 0

            if len(split_message[start:]) != 0:
                out = split_message[start:]
                for ignore in ["NIL", "NILL", "XMTD"]:
                    if ignore in out: out.pop(out.index(ignore))
                    elif ignore.lower() in out: out.pop(out.index(ignore.lower()))
                messages_out.append(out)
        return header, messages_out


def main():
    filepath = sys.argv[1]
    mydata = WMOReader(filepath)
    print(mydata)
    print(mydata.records)
    #files = glob.glob("/ldm/data/upperair/sonde/20220104*.uair")
    #for file in files:
    #    WMOReader(file)

if __name__ == "__main__":
    main()

