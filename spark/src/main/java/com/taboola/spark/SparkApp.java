package com.taboola.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


import org.apache.commons.lang3.time.StopWatch;
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

public class SparkApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(SparkApp.class);

    private final static String UPSERT_SQL = "MERGE INTO events AS target " +
            "USING (values (?, ?, ?)) AS source (time_bucket, event_id, \"COUNT\") " +
            "ON target.time_bucket = source.time_bucket AND target.event_id = source.event_id " +
            "WHEN MATCHED THEN UPDATE SET target.count = source.count " +
            "WHEN NOT MATCHED THEN INSERT (time_bucket, event_id, \"COUNT\") VALUES (source.time_bucket, source.event_id, source.count);";

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().master("local[4]").config("spark.sql.session.timeZone", "UTC").getOrCreate();

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
                .withWatermark("timestamp", "60 minutes")  // Watermark to handle late data
                // check for valid event Id
                .filter((col("eventId").$greater$eq(0).and(col("eventId").$less$eq(99))))
                //format the time to minute for further grouping and write into DB
                .withColumn("time_bucket", functions.date_format(col("timestamp"), "yyyyMMddHHmm"))
                .groupBy("time_bucket","eventId")
                //count the event
                .agg(functions.count("*").alias("count"))
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                //output only update event count
                .outputMode(OutputMode.Update())
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.show(1000,false);
                    //collet the batch to a list and insert into DB at once.
                    List<Row> list = batchDF.collectAsList();
                    writeIntoDb(batchId, list);
                })
                .start();

        // the stream will run forever
        spark.streams().awaitAnyTermination();
    }

    private static void writeIntoDb(long batchId, List<Row> rows) {
        final StopWatch stopWatch = StopWatch.createStarted();
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = DbInstance.INSTANCE.getDatasource().getConnection();
            statement = connection.prepareStatement(UPSERT_SQL);
            for (Row row : rows) {
                statement.setString(1, row.getString(0));  // time_bucket
                statement.setLong(2, row.getLong(1));    // event_id
                statement.setLong(3, row.getLong(2));    // count
                statement.addBatch();
                LOGGER.debug("==>{}", row);
            }

            if (rows.size() > 0) {
                statement.executeBatch();
            }
            LOGGER.info("=======================>Batch {}, Batch Size {}, spend {}", batchId, rows.size(),stopWatch.getTime(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            LOGGER.error("Write data into database failed.", e);
        } finally {
            stopWatch.stop();
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
