package com.taboola.spark;

import org.apache.spark.sql.*;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SparkApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(SparkApp.class);

    private final static String UPSERT_SQL = "MERGE INTO events AS target " +
                                             "USING (values (?, ?, ?)) AS source (time_bucket, event_id, \"COUNT\") " +
                                             "ON target.time_bucket = source.time_bucket AND target.event_id = source.event_id " +
                                             "WHEN MATCHED THEN UPDATE SET target.count = source.count " +
                                             "WHEN NOT MATCHED THEN INSERT (time_bucket, event_id, \"COUNT\") VALUES (source.time_bucket, source.event_id, source.count);";
    private static final String JDBC_URL = "jdbc:hsqldb:hsql://localhost/xdb";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";
    private static final String JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    private static final String PROP_DB_USER = "user";
    private static final String PROP_DB_PASSWORD = "password";;

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
        events.withColumn("time_bucket", functions.date_format(functions.col("timestamp"), "yyyyMMddHHmm") )
                .groupBy("time_bucket", "eventId")
                .agg(functions.count("*").alias("count"))
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .option("truncate", "false")
                .option("numRows", 5000)
                .outputMode(OutputMode.Update())
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.sort("time_bucket", "eventId")
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
//            String upsertQuery = new StringBuilder("MERGE INTO events AS target")
//                    .append("USING (values (?, ?, ?)) AS source (time_bucket, event_id, \"COUNT\")")
//                    .append("ON target.time_bucket = source.time_bucket AND target.event_id = source.event_id")
//                    .append("WHEN MATCHED THEN UPDATE SET target.count = source.count")
//                    .append("WHEN NOT MATCHED THEN INSERT (time_bucket, event_id, \"COUNT\") VALUES (source.time_bucket, source.event_id, source.count);")
//                    .toString();

            statement = connection.prepareStatement(UPSERT_SQL);
            int batchSize = 0;
            while (partition.hasNext()) {
                Row row = partition.next();
                statement.setString(1, row.getString(0));  // time_bucket
                statement.setLong(2, row.getLong(1));    // event_id
                statement.setLong(3, row.getLong(2));    // count
                statement.addBatch();
                batchSize++;
                LOGGER.info("==>{}", row);
            }
            if(batchSize>0) {
                statement.executeBatch();
            }
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
        Properties connectionProperties = new Properties();
        connectionProperties.put(PROP_DB_USER, DB_USER);
        connectionProperties.put(PROP_DB_PASSWORD, DB_PASSWORD);
//        connectionProperties.put("allow_empty_batch", "true");

        try {
            Class.forName(JDBC_DRIVER);
            return DriverManager.getConnection(JDBC_URL, connectionProperties);
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
