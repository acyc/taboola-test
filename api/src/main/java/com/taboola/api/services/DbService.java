package com.taboola.api.services;

import java.util.Map;

public interface DbService {
    Map<String, Long> queryEventByTimeBucket(final String timeBucket);

    Long queryCountByTimeBucketAndEventId(final String timeBucket, final int eventId);
}
