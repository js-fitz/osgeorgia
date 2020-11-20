# Open-Sourcing Georgia Elections Data
### AKA "ElViz" (Elections Visualization Tools)

This repository contains cleaned data on recent elections in Georgia collected from multiple official sources. The sources for specific files can be found in subdirectory readme files.


- **clean_data** contains all non-geometric data from recent elections in Georgia, mostly state-wide but also with some smaller races.
    - **2020_november** contains the results from the Nov. 3 elections, delimited by county and/or precinct 
        - **joined** subfolders contain cleaned results data, with a file for each race (ie. US Senate (Loeffler) or DA - Alcovy cty.)
        - **cleaning code** contains the full source code used for parsing and cleaning the data
        - **county_results_raw** contains the .txt files downloaded directly from the GA Secretary of State's website (pre-cleaned and pre-joined).
    - **recent_runoffs** contains voter participation data from the past three state-wide elections that resulted in runoffs (The 2018 Primary + General and the 2016 primary). Note that for primary elections, ballots are split by county so participation rates are calculated as DEM participation divided by TOTAL registered voters. These data include voter numbers & participation across race/ethnicity and sex/gender groups. 
        - **raw_turnout_by_precinct** contains the raw source data. Cleaned & joined data is stored in a folder named for the election (2018_november or 2016_may)
- **map_data** contains the data currently in use on the ElViz prototype
- **precinct_decoder** Precinct names vary slightly between datasets. They also change over time. The precinct decoder contains a reference table of all known precincts in every county, with columns representing the known aliases for the given precinct across all datasets. Additional data can be added using the supplied python script. Matches are decided through a jaro-based fuzzy string matching algorithm (source within).
- **shapes** contains the precinct and county shape geometry files used to generate maps. Although there are additional parameters included in some of these datasets, we are using them purely for the geographic data and appending our own cleaned data to these results. The precinct name decoder is run over the participation data from recent runoffs as well as the 2020 November voting margin data, then over the shape files, to match precinct names across datasets. 
- **voters** — Miscellanous demographic data not related to a specific election (currently includes 2019 voter registration data and population over time (2010-2019) by county).
