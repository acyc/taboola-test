package com.taboola.api.services;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.taboola.api.exceptions.NotFoundException;


@Component
public class EventServiceImpl implements EventService {
    private final DbService dbService;

    @Autowired
    public EventServiceImpl(DbService dbService) {
        this.dbService = dbService;
    }

    /**
     * Get event ID and count map by timeBucket
     * @param timeBucket - the timeBucket to get the map.
     * @return the map that contains eventID and count.
     * @throws NotFoundException if the map is empty.
     */
    public Map<String, Long> getEventsByTimeBucket(final String timeBucket) {
        Map<String, Long> result = this.dbService.queryEventByTimeBucket(timeBucket);

        if (result.isEmpty()) {
            throw new NotFoundException(timeBucket);
        }

        return result;
    }

    /**
     * Get count by timeBucket and eventId
     * @param timeBucket - the timeBucket to get the count
     * @param eventId - the eventId to get the count.
     * @return the count of events for the timeBucket and eventId.
     * @throws NotFoundException if the count is null
     */
    public Long getEventByTimeBucketAndEventId(final String timeBucket, final int eventId) {
        Long count = this.dbService.queryCountByTimeBucketAndEventId(timeBucket, eventId);
        if (count == null) {
            throw new NotFoundException(timeBucket, eventId);
        }
        return count;
    }


}
