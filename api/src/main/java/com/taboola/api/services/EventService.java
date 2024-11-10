package com.taboola.api.services;

import com.taboola.api.domains.Event;

import java.util.Map;

public interface EventService {
    Map<String, Long> getEventsByTimeBucket(final String timeBucket);

    Long getEventByTimeBucketAndEventId(final String timeBucket, final int eventId);
}
