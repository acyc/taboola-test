package com.taboola.api;

import com.taboola.api.domains.Event;
import com.taboola.api.services.EventService;

import com.taboola.api.validators.BasicValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController()
@RequestMapping("/api/counters")
public class CounterController {

    private final EventService eventService;
    private final BasicValidator validator;

    @Autowired
    public CounterController(EventService eventService, BasicValidator validator) {
        this.eventService = eventService;
        this.validator = validator;
    }

    @GetMapping(value = "/time/{timeBucket}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Long> getEventsByTime(@PathVariable final String timeBucket) {
        validator.validateTimeBucket(timeBucket);

        return eventService.getEventsByTimeBucket(timeBucket);
    }


    @GetMapping(value = "/time/{timeBucket}/eventId/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public long getEventsById(@PathVariable final String timeBucket, @PathVariable final int eventId) {
        validator.validateTimeBucket(timeBucket);
        validator.validateEventId(eventId);

        Event event = eventService.getEventByTimeBucketAndEventId(timeBucket, eventId);
        return event.getCount();
    }
}
