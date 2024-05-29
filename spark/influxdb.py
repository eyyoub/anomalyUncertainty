from datetime import datetime
from influxDB_Writer import InfluxDBWriter
from influxdb_client import WritePrecision, InfluxDBClient, Point
from spark_processor import SparkDataProcessor
from pyspark.sql.functions import *
from pyspark.sql.functions import col, regexp_replace, to_timestamp
import pandas as pd
# Create an instance of the SparkDataProcessor class
data_processor = SparkDataProcessor()

# Process the data
df = data_processor.process_data()

# Create an instance of InfluxDBWriter
influx_writer = InfluxDBWriter()


print('=================>hoio')

#df = df.withColumn('Timestamp', datetime.strptime(df["Timestamp"], "%d/%m/%Y %H:%M:%S.%f %p"))
df = df.withColumn("Normal/Attack", when(df["Normal/Attack"] == "Attack", 1).otherwise(0))
def save_to_influxdb(batch_df):
    points = []

    # Convert each row in the DataFrame to an InfluxDB point
    for row in batch_df.collect():
        timestamp_str = row["Timestamp"].strip()
        try:
            
            date_obj = datetime.strptime(timestamp_str, '%d/%m/%Y %I:%M:%S %p')
            timestamp = date_obj.strftime('%Y-%m-%d %H:%M:%S.%f')
            print(timestamp)
            point = Point("swat9").time(datetime.utcnow(), WritePrecision.MS)
        except ValueError as e:
            print("Error parsing timestamp:", e)

        # Iterate through DataFrame columns and create InfluxDB fields dynamically
        for col_name in batch_df.columns:
            # Exclude the timestamp column, if present
            if col_name != "Timestamp" and col_name != "Normal/Attack":
                # Add fields to the InfluxDB point
                point.field(col_name, float(row[col_name]))
            else :
                if col_name == "Normal/Attack":
                     point.field(col_name, (row[col_name]))
                if col_name == "Timestamp":
                    point.time(col_name, (timestamp))
            
        # Print the point before saving to InfluxDB
        print(f"Saving point: {point.to_line_protocol()}")

        # Save the point to InfluxDB (use your actual method to save)
        influx_writer.write_to_influxdb(point)
        print("All data saved successfully to InfluxDB.")

    # Write points to InfluxDB
    print("Data saved successfully to InfluxDB.")

# Start streaming query
query = (df.writeStream
         .foreachBatch(lambda batch_df, epoch_id: save_to_influxdb(batch_df))
         .outputMode("append")
         .option("checkpointLocation", "checkpoints")
         .start())

query.awaitTermination()


#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /sparkScripts/influxdb.py