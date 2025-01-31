# XML to CSV Converter for eBusiness Register Open Data

## Overview
This R script is designed to convert the eBusiness Register open data from XML to CSV format using a memory-efficient approach. The script processes the XML data line by line, allowing it to work within the memory constraints of a standard laptop with 16GB of RAM.

## Motivation
Processing large XML files in R can be challenging due to memory limitations. The XML files provided by the eBusiness Register can reach multiple gigabytes in size, making it difficult to load them entirely into memory. This script solves that problem by reading the XML data line by line and processing each company entry individually. While this approach is inefficient in terms of processing time, it significantly reduces memory usage, requiring only about 1.5GB of RAM.

## Performance
- The conversion of the largest file takes approximately **3.5 hours** on an older laptop with **16GB of RAM** (scripts use about ~1.5GB of RAM).
- Smaller files should take proportionally less time.
- The processing speed is primarily limited by the **line-by-line reading method**, rather than batch size.

## Requirements
Ensure that you have R installed along with the following R libraries:

```r
library(data.table)
library(xml2)
library(readr)
library(dplyr)
```

## Usage
1. **Download the XML file** from the [eBusiness Register open data portal](https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine).
2. **Place the XML file** in the same directory as the corresponding R script.
3. **Run the script** using your preferred IDE or environment, such as RStudio or the R command line.

## Customization
The script includes several customizable parameters:

- `total_records <- 347622` – Used to track script progress. This number will vary as the dataset changes (feel free to keep it up to date in your script).
- `batch_amount <- 1000` – Defines how many records are processed at a time. Adjust based on your available system resources.

> **Note:** Increasing `batch_amount` may speed up processing but requires more RAM. The impact of this setting is not precisely known, as the primary bottleneck is reading the XML line by line.

## File Conversion Example
To convert **ettevotja_rekvisiidid__yldandmed.xml**:
1. Download the file.
2. Run `ettevotja_rekvisiidid__yldandmed.R` in the same directory.
3. ettevotja_rekvisiidid__yldandmed.csv will be created in the same directory and appended to in every batch.

## Limitations
- The script is **slow** due to its line-by-line reading approach.
- The batch size setting's effect on performance is **not fully tested**.
- **Memory-efficient but not optimized for speed**.
