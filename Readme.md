# Earthquake Analytics Project

**Project Description:**

* Analyze and visualize earthquake data using PySpark.
* Explore geographical distribution, depth, magnitude, and distance.
* Categorize earthquakes based on magnitude levels.
* 
**Accepted file format:** CSV

**Data:**

* Scans to a dataframe with columns:
   * `Date` (string, YYYY-MM-DD)
   * `Time` (string, HH:MM:SS)
   * `Latitude` (float)
   * `Longitude` (float)
   * `Type` (string)
   * `Depth` (float)
   * `Magnitude` (float)

**Dependencies:**

* Apache Spark: [https://spark.apache.org/](https://spark.apache.org/)
* Folium: [https://python-visualization.github.io/folium/](https://python-visualization.github.io/folium/)

**Code Structure:**

* `earthquake_analysis.py`: Main script for data processing, analysis, and visualization.
* `utils.py`: (Optional) Utility functions.

**Execution:**

1. Install Spark and Folium.
2. Set `file_path` in `earthquake_analysis.py` to your CSV file.
3. run src/scripts.py

**Output:**

* CSV files:
   * Average depth and magnitude statistics per earthquake type.
   * Final processed data with additional columns. 

    

**Further details and explanations available within code comments and documentation.**
