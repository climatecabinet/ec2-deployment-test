# Tiger/Shapefile Data
This is an incomplete README for the tiger/shapefile dataset. Still left to add:

1. How to clean this data

## Background

TIGER (Topologically Integrated Geographic Encoding and Referencing system) is a format used by the US Census Bureau to describe land attributes such as roads, buildings, rivers, and lakes, as well as areas such as census tracts.

## Fetching Instructions

1. Go to https://www.census.gov/cgi-bin/geo/shapefiles/index.php
2. Select the desired year and layer type.
3. If there's only one option, download the .zip file. For layers that have multiple options, like State Legislative Districts which have one .zip file for each state and chamber, download each .zip file.
4. Unzip the files and add the resulting folders to their respective year folder, or create a new year folder if needed. For layers with multiple .zip files, unzipe each, and add all the resulting folders to a folder in the respective year folder. For an example, see `raw-data/2019/sldl`, which contains State Legislative District Upper Chamber files for each state.


