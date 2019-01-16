# Perfmon CSV Parser

Transforms the CSV output of Perfmon collector into a dimension/measure model.

### Installation

```bash
pip3 install -r requirements.txt
```


### Transformations

**Filters**
- `time_from` and `time_to`: only keeps data for the given period, keeps everything if not specified
- `metricsNameMustContain`: only keeps metrics where their name contains the given keywords
- `metricNameMustContainOneOF`: only keeps metrics where their name contains one of the specified keywords

**Transformations**
- the column containing timestamp is renamed to `timestamp`, so the resulting dataset can be combined with datasets from other sources (i.e. other servers with different settings)
- hostname is stripped from the metric names
- if a metric's name contains a drive letter ("C:"), it is stripped from the metric name and added to a new column `volume`
- the dataset is rotated by 90 degrees: new column `value` contains all the metric values


### Example Usage

```bash
python3 parse_perfmon_csv.py ./DISCS_PROD_DataCollector.csv ./Output_DataCollector.csv -f "2019-01-15 00:00:00" -t "2019-01-16 00:00:00"
```

**Parameters**
```
usage: parse_perfmon_csv.py [-h] [-f FROM] [-t TO] [-df DATEFORMAT]
                            inputCSV outputCSV

positional arguments:
  inputCSV              the CSV file to parse
  outputCSV             the output CSV filename

optional arguments:
  -h, --help            show this help message and exit
  -f FROM, --from FROM  read values starting with specified timestamp,
                        example: --from='2019-01-14 13:20:00' (default: None)
  -t TO, --to TO        read values until the specified timestamp (default:
                        None)
  -df DATEFORMAT, --dateformat DATEFORMAT
                        custom date format, default = yyyy-mm-dd hh24:mi:ss
                        (default: %Y-%m-%d %H:%M:%S)
```