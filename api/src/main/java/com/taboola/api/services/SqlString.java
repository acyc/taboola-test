package com.taboola.api.services;

/**
 * Manages SQL string.
 */
public enum SqlString {
    /**
     * Query SQL for retrieving all events by time bucket.
     */
    EVENTS_BY_TIME_BUCKET("select time_bucket, event_id, count from events where time_bucket = ? order by event_id"),
    /**
     * Query SQL for getting count by time bucket and event id.
     */
    COUNT_BY_TIME_BUCKET_AND_EVENT_ID("select \"COUNT\" from events where time_bucket = ? and event_id = ?");

    private final String sql;
    SqlString(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }
}
