import folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    avg,
    udf,
    to_timestamp,
)
from pyspark.sql.types import StringType, FloatType

# Create a SparkSession
spark = SparkSession.builder.appName("EarthQuake Analytics").getOrCreate()

# Define paths for clarity
file_path = "../src/resources/database.csv"
average_stats_output = "../src/resources/average_depth_and_magnitude_stats.csv"
final_output = "../src/resources/final_output.csv"

# Read the CSV file
input_df = spark.read.csv(file_path, header=True)
df = input_df.select(
    col("Date").cast(StringType()),
    col("Time").cast(StringType()),
    col("Latitude").cast(FloatType()),
    col("Longitude").cast(FloatType()),
    col("Type").cast(StringType()),
    col("Depth").cast(FloatType()),
    col("Magnitude").cast(StringType()),
)

# Combine Date and Time columns into a Timestamp column
timestamped_df = df.withColumn(
    "timestamp",
    to_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss"),
).drop("Date", "Time")

# Filter for earthquakes with magnitude > 5.0
filtered_df = timestamped_df.where(col("Magnitude") > 5.0)

# Calculate average depth and magnitude for each earthquake type
average_depth_and_magnitude_stats = (
    filtered_df.groupBy("Type")
    .agg(avg(col("Depth")).alias("Average Depth"), avg(col("Magnitude")).alias("Average Magnitude"))
    .write.format("csv")
    .option("header", True)
    .option("mode", "overwrite")
    .save(average_stats_output)
)


# Define UDFs for magnitude categorization and distance calculation
def categorize_magnitude(magnitude):
    """Categorizes earthquake magnitude into levels."""
    if float(magnitude) <= 5.0:
        return "Low"
    elif float(magnitude) <= 7.0:
        return "Moderate"
    else:
        return "High"


udf_categorize_magnitude = udf(categorize_magnitude, StringType())


def euclidean_distance(x1, y1, x2, y2):
    """Calculates the Euclidean distance between two points."""
    return ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5


udf_euclidean_distance = udf(euclidean_distance, FloatType())  # Use FloatType for distance

# Reference location
reference_lat = 0.0
reference_lon = 0.0

# Categorize magnitudes and calculate distances
categorized_df = filtered_df.withColumn("Level1", udf_categorize_magnitude(col("Magnitude")))
final_df = categorized_df.withColumn(
    "Distance (euclidean_distance)",
    udf_euclidean_distance(col("Latitude"), col("Longitude"), lit(reference_lat), lit(reference_lon))
)

# Display and save the final DataFrame
final_df.show()
final_df.write.format("csv").option("header", True).option("mode", "overwrite").save(final_output)

# Visualize earthquake locations on a Folium map
earthquake_locations = df.select("Latitude", "Longitude").distinct()
m = folium.Map(location=[0, 0], zoom_start=2)

for row in earthquake_locations.collect():
    latitude = row["Latitude"]
    longitude = row["Longitude"]
    folium.Marker([latitude, longitude]).add_to(m)

# Optional: Stop the SparkSession
spark.stop()
