package com.taboola.api.services;

import com.taboola.api.domains.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;

@Component
public class DbService {
    private final static Logger LOGGER = LoggerFactory.getLogger(DbService.class);

    public List<Event> queryEvent(final String sql, final Map<Integer, Object> statementParam)  {

        final List<Event> events = new LinkedList<>();
        PreparedStatement statement = null;
        Connection connection = getConnection();

        try {
            statement = this.createPreparedStatement(sql);
            for(Map.Entry<Integer, Object> entry:statementParam.entrySet()) {
                statement.setObject(entry.getKey(),entry.getValue());
            }
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()){
                Event event = new Event();
                event.setTimeBucket(resultSet.getString("time_bucket"));
                event.setEventId(resultSet.getString("event_id"));
                event.setCount(resultSet.getLong("count"));
                events.add(event);
            }
            resultSet.close();


        } catch (SQLException e) {
            LOGGER.error("Query failed", e);
        } finally {
            try {
                if(statement!=null){
                    statement.close();
                }
                if(connection!=null && !connection.isClosed()){
                    connection.close();
                }
            } catch (SQLException sqlException) {
                LOGGER.error("Error occurred during statement or connection close.", sqlException);
            }
        }
        return events;
    }

    private PreparedStatement createPreparedStatement(final String sql){
        PreparedStatement statement = null;
        Connection connection = this.getConnection();
        try{
            if(connection!=null){
                statement = connection.prepareStatement(sql);
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to prepare statement for sql", e);
        }
        return statement;
    }

    private Connection getConnection(){

        final String jdbcUrl = "jdbc:hsqldb:hsql://localhost/xdb";
        final String user = "sa";
        final String password = "";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("allow_empty_batch","true");

        try{
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            return DriverManager.getConnection(jdbcUrl, connectionProperties);
        } catch (SQLException | ClassNotFoundException exception) {
            LOGGER.error("Unable to establish connection to database", exception);
        }
        return null;

    }
}
