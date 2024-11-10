package com.taboola.api.services;

import com.taboola.api.domains.Event;
import com.taboola.api.exceptions.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Component
public class EventServiceImpl implements EventService {
    private final DbService dbService;

    @Autowired
    public EventServiceImpl(DbService dbService) {
        this.dbService = dbService;
    }

    public Map<String, Long> getEventsByTimeBucket(final String timeBucket) {

        final String sql = "select time_bucket, event_id, \"COUNT\" from events where time_bucket = ? order by event_id";
        Map<Integer, Object> map = new HashMap<>(1);
        map.put(1, timeBucket);

        Map<String, Long> result = this.dbService.queryEvent(sql, map)
                .stream()
                .collect(Collectors.toMap(Event::getEventId, Event::getCount));

        if(result.isEmpty()){
            throw new NotFoundException(timeBucket);
        }

        return result;
    }

    public Event getEventByTimeBucketAndEventId(final String timeBucket, final int eventId) {

        final String sql = "select time_bucket, event_id, \"COUNT\" from events where time_bucket = ? and event_id = ?";
        Map<Integer, Object> map = new HashMap<>(2);
        map.put(1, timeBucket);
        map.put(2, eventId);
        List<Event> events = this.dbService.queryEvent(sql, map);
        if(events.isEmpty()){
            throw new NotFoundException(timeBucket, eventId);
        }
        return events.get(0);
    }


}
