import pandas as pd 
import re
import datetime
import argparse
import sys
import os.path as path
import time

# TODO 
# - validate it's actually a perfmon file and terminate gracefully if not
# - validate datetime inputs are valid

def debugPrint(text):
    print("[DEBUG]: " + text)

def infoPrint(text):
    print("[INFO]: " + text)


# override argparse error method to display full help message on error
class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('ERROR: %s\n' % message)
        self.print_help()
        sys.stderr.write('EXAMPLE: \n\t$ python3 parse_perfmon_csv.py /Users/davidbudac/Downloads/DISCS_PROD_DataCollector_2.csv /Users/davidbudac/Downloads/DISCS_PROD_DataCollector_2_output.csv -f "2019-01-14 00:00:00" -t "2019-01-14 01:00:00" | tee ./log.txt')
        sys.exit(1)

ap = DefaultHelpParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

ap.add_argument("inputCSV", help="the CSV file to parse", type=str)
ap.add_argument("outputCSV", help="the output CSV filename", type=str)
ap.add_argument("-f", "--from", help="read values starting with specified timestamp, example: --from='2019-01-14 13:20:00'", type=str)
ap.add_argument("-t", "--to", help="read values until the specified timestamp", type=str)
ap.add_argument("-df", "--dateformat", help="custom date format, default = yyyy-mm-dd hh24:mi:ss", default="%Y-%m-%d %H:%M:%S", type=str)

args = ap.parse_args()

getattr(args, "inputCSV")

# TODO
# - accept as input parameters:
metricNameMustContainAll = ['physical']
metricNameMustContainOneOF = ['Disk Reads/sec', 'Disk Writes/sec', 'Disk Read Bytes/sec', 'Disk Write Bytes/sec']

params = dict()
params["from_time"] = getattr(args, "from") # = '01/14/2019 00:00:00'
params["to_time"] = getattr(args, "to") # = '01/14/2019 00:00:00'
params["dateformat"] = getattr(args, "dateformat")

# convert time range to datetime
# datetime_from = datetime.datetime.strptime(from_time, '%m/%d/%Y %H:%M:%S')
# datetime_to = datetime.datetime.strptime(to_time, '%m/%d/%Y %H:%M:%S')
params["datetime_from"] = datetime.datetime.strptime(params["from_time"], params["dateformat"]) if params["from_time"] else None
params["datetime_to"] = datetime.datetime.strptime(params["to_time"], params["dateformat"]) if params["to_time"] else None

params["fileName"] = getattr(args, "inputCSV")
params["fileNameOutput"] = getattr(args, "outputCSV")

debugPrint (f"Parameters= {params}")

# READ THE CSV HEADER FIRST

# validate the file exists
for e in [params["fileName"]]:
    if not path.exists(e):
        sys.stderr.write(f'ERROR: File "{e}" cannot be found.\n')
        sys.exit(2)

# start with reading just the header
# so we can only select metrics we want to process
header = pd.read_csv(params["fileName"], 
                 delim_whitespace=False, 
                 error_bad_lines=True,
                 nrows = 0
                )

debugPrint("File contains the following metrics:")
for e in header.columns:
    debugPrint("\t"+e)

# only keep the metrics we want to read + time dimension
metricsToRead = [
    e for i, e in enumerate(header.columns)
    if any (
        re.search (el, e, re.IGNORECASE) 
        for el in metricNameMustContainOneOF
    ) and all (
        re.search (el, e, re.IGNORECASE) 
        for el in metricNameMustContainAll
    )
]

# also add the time dimension column (we don't know the name, we can only be sure it's the first column)
metricsToRead.append(header.columns[0])

debugPrint("only the following metrics will be kept:")
for e in metricsToRead:
    debugPrint("\t"+e)

infoPrint (f"Reading file: {params['fileName']} ...")

startTime = time.perf_counter()
# read the csv file
perfmonCSV = pd.read_csv(params["fileName"],
                          delim_whitespace=False, 
                          error_bad_lines=False,
                          low_memory=False,
                          usecols=metricsToRead
                          )

infoPrint (f"...done. Time taken = {time.perf_counter()- startTime} seconds. ")



# filter the dataset for the given time interval
# (this can be rewritten to use iterator so we dont have to store everything in memory)
if all ([ params["datetime_from"], params["datetime_to"] ]):

    startTime = time.perf_counter()
    infoPrint("Removing items outside of specified interval: ")

    perfmonCSV = perfmonCSV[perfmonCSV[header.columns[0]]
                            .apply(lambda t: 
                                        True if params["datetime_from"] <= datetime.datetime.strptime(t, '%m/%d/%Y %H:%M:%S.%f') <= params["datetime_to"]
                                        else False
                            )]
    infoPrint(f"...done. Time taken = {time.perf_counter() - startTime}")


startTime = time.perf_counter()
infoPrint ("Transforming the dataset ...")
# rename the first (time dimension) column
perfmonCSV = perfmonCSV.rename(columns={header.columns[0] : 'timestamp'})

# rotate the dataset to create a metric dimension and value measure
perfmonCSV = perfmonCSV.melt(id_vars=['timestamp'], var_name='originalMetricName')

# add column "volume" for the disk metrics
# and fill in the volume name from the metric name
perfmonCSV['volume'] = perfmonCSV['originalMetricName'].apply(lambda metricName:
                                                    None if not re.search('[A-Z]:', metricName)
                                                    else re.search('[A-Z]:', metricName).group(0)                                    
                                               )

# strip the namespaces from metric names
perfmonCSV['metric'] = perfmonCSV['originalMetricName'].apply(lambda metricName:
                                                    metricName[metricName.rfind('\\')+1:]
                                               )


# convert the timestamp to datetime
perfmonCSV['timestamp']  = perfmonCSV['timestamp'].apply(
                                lambda t: datetime.datetime.strptime(t, '%m/%d/%Y %H:%M:%S.%f')
                            )

# convert value to float, convert empty values to None
perfmonCSV['value'] = perfmonCSV['value'].apply (
                                            lambda v: 
                                                    float(v) if v != ' ' else None
                                                )
infoPrint (f"...done. Time taken={time.perf_counter() - startTime}")

startTime = time.perf_counter()
infoPrint (f"Writing the output as CSV to file: {params['fileNameOutput']}")

perfmonCSV.to_csv (params["fileNameOutput"])

infoPrint (f"...done. Time taken={time.perf_counter() - startTime}")
