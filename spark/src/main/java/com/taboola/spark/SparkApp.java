package com.taboola.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SparkApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(SparkApp.class);

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().master("local[4]").getOrCreate();

        // generate events
        // each event has an id (eventId) and a timestamp
        // an eventId is a number between 0 an 99
        Dataset<Row> events = getEvents(spark);
        events.printSchema();

        // REPLACE THIS CODE
        // The spark stream continuously receives messages. Each message has 2 fields:
        // * timestamp
        // * event id (valid values: from 0 to 99)
        //
        // The spark stream should collect, in the database, for each time bucket and event id, a counter of all the messages received.
        // The time bucket has a granularity of 1 minute.
        events
                // Add a new column for the minute, formatted as "yyyy-MM-dd HH:mm"
                //.withWatermark("timestamp", "1 minutes")
                .withColumn("time_bucket", functions.date_format(functions.col("timestamp"), "yyyyMMddHHmm"))
//                .groupBy("time_bucket", "eventId")
                // Group by minute and eventId and count the occurrences
//                .groupBy(functions.window(functions.col("timestamp"),"2 minutes"),functions.col("time_bucket"),functions.col("eventId"))
                .groupBy("time_bucket", "eventId")
                .agg(functions.count("*").alias("count"))
                //.select("eventId", "event_count")
                //.count()
                .withColumnRenamed("eventId", "event_id")

                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                .option("truncate", "false")
                .option("numRows", 5000)
                .outputMode(OutputMode.Update())
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.sort("time_bucket", "event_id")
                            .foreachPartition(SparkApp::writeIntoDb);

                })
                .start();

        // the stream will run forever
        spark.streams().awaitAnyTermination();
    }

    private static void writeIntoDb(Iterator<Row> partition) {
        Connection connection = getConnection();
        PreparedStatement statement = null;

        try {
            // Prepare the SQL upsert statement
            //String upsertQuery = "INSERT INTO events (time_bucket, event_id, count) VALUES (?, ?, ?)";
            String upsertQuery = "MERGE INTO events AS target\n" +
                                 "USING (values (?, ?, ?)) AS source (time_bucket, event_id, \"COUNT\")\n" +
                                 "ON target.time_bucket = source.time_bucket AND target.event_id = source.event_id\n" +
                                 "WHEN MATCHED THEN\n" +
                                 "    UPDATE SET target.count = source.count\n" +
                                 "WHEN NOT MATCHED THEN\n" +
                                 "    INSERT (time_bucket, event_id, count) VALUES (source.time_bucket, source.event_id, source.count);";
            statement = connection.prepareStatement(upsertQuery);
            while (partition.hasNext()) {
                Row row = partition.next();
                LOGGER.info("==>{}", row);
                statement.setString(1, row.getString(0));  // time_bucket
                statement.setLong(2, row.getLong(1));    // event_id
                statement.setLong(3, row.getLong(2));    // count
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (Exception e) {
            LOGGER.error("Write data into database failed.", e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Error occurred during statement or connection close.", sqlException);
            }
        }


    }

    private static Connection getConnection() {
        final String jdbdUrl = "jdbc:hsqldb:hsql://localhost/xdb";
        final String user = "sa";
        final String password = "";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("allow_empty_batch", "true");

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            return DriverManager.getConnection(jdbdUrl, connectionProperties);
        } catch (SQLException | ClassNotFoundException exception) {
            LOGGER.error("Unable to establish connection to database", exception);
        }
        return null;

    }

    private static Dataset<Row> getEvents(SparkSession spark) {
        return spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", "10000")
                .load()
                .withColumn("eventId", functions.rand(System.currentTimeMillis()).multiply(functions.lit(100)).cast(DataTypes.LongType))
                .select("eventId", "timestamp");
    }

}
