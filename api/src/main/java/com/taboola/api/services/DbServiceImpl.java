package com.taboola.api.services;

import com.taboola.api.domains.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Service that response for establishing connection and doing query.
 */
@Component
public class DbServiceImpl implements DbService{
    private final static Logger LOGGER = LoggerFactory.getLogger(DbServiceImpl.class);
    private static final String COL_TIME_BUCKET = "time_bucket";
    private static final String COL_EVENT_ID = "event_id";
    private static final String COL_COUNT = "count";
    private static final String PROP_USER = "user";
    private static final String PROP_PASSWORD = "password";

    @Value("${hql.jdbc.driver}")
    private String jdbcDriver;
    @Value("${hql.jdbc.url}")
    private String jdbcUrl;
    @Value("${hql.jdbc.user}")
    private String user;
    @Value("${hql.jdbc.password}")
    private String password;

    public List<Event> queryEvent(final String sql, final Map<Integer, Object> statementParam) {

        final List<Event> events = new LinkedList<>();
        PreparedStatement statement = null;
        Connection connection = getConnection();

        try {
            statement = this.createPreparedStatement(sql);
            for (Map.Entry<Integer, Object> entry : statementParam.entrySet()) {
                statement.setObject(entry.getKey(), entry.getValue());
            }
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Event event = new Event();
                event.setTimeBucket(resultSet.getString(COL_TIME_BUCKET));
                event.setEventId(resultSet.getString(COL_EVENT_ID));
                event.setCount(resultSet.getLong(COL_COUNT));
                events.add(event);
            }
            resultSet.close();


        } catch (SQLException e) {
            LOGGER.error("Query failed", e);
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
        return events;
    }

    private PreparedStatement createPreparedStatement(final String sql) {
        PreparedStatement statement = null;
        Connection connection = this.getConnection();
        try {
            if (connection != null) {
                statement = connection.prepareStatement(sql);
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to prepare statement for sql", e);
        }
        return statement;
    }

    private Connection getConnection() {
        Properties connectionProperties = new Properties();
        connectionProperties.put(PROP_USER, user);
        connectionProperties.put(PROP_PASSWORD, password);

        try {
            Class.forName(this.jdbcDriver);
            return DriverManager.getConnection(jdbcUrl, connectionProperties);
        } catch (SQLException | ClassNotFoundException exception) {
            LOGGER.error("Unable to establish connection to database", exception);
        }
        return null;

    }
}
