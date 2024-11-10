package com.taboola.api.services;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Service that response for establishing connection and doing query.
 */
@Service
public class DbServiceImpl implements DbService {
    private static final String COL_EVENT_ID = "event_id";
    private static final String COL_COUNT = "count";
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public DbServiceImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Query the eventId and count by timeBucket
     * @param timeBucket - the timeBucket to query the DB
     * @return the map contains eventId and count.
     */
    @Override
    public Map<String, Long> queryEventByTimeBucket(final String timeBucket) {
        final Map<String, Long> map = new LinkedHashMap<>();
        this.jdbcTemplate.query(SqlString.EVENTS_BY_TIME_BUCKET.getSql(), new Object[]{timeBucket}, (resultSet) -> {
            map.put(resultSet.getString(COL_EVENT_ID), resultSet.getLong(COL_COUNT));
        });
        return map;
    }

    /**
     * Query count by timeBucket and eventId
     * @param timeBucket - the timeBucket to query the DB
     * @param eventId - the eventId to query the DB
     * @return the count in DB. Return null if there is no data in DB.
     */
    @Override
    public Long queryCountByTimeBucketAndEventId(final String timeBucket, final int eventId) {
        try {
            return this.jdbcTemplate.queryForObject(SqlString.COUNT_BY_TIME_BUCKET_AND_EVENT_ID.getSql(), new Object[]{timeBucket, eventId}, Long.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }

    }
}
