package com.taboola.api;

import com.taboola.api.domains.Event;
import com.taboola.api.services.EventService;
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

    @Autowired
    public CounterController(EventService eventService) {
        this.eventService = eventService;
    }

    @GetMapping(value = "/time/{timeBucket}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Long> getEventsByTime(@PathVariable final String timeBucket) {
        return eventService.getEventsByTime(timeBucket);
    }


    @GetMapping(value = "/time/{timeBucket}/eventId/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public long getEventsById(@PathVariable final String timeBucket, @PathVariable final String eventId) {
        Event event = eventService.getEventByTimeAndId(timeBucket, eventId);
        return event.getCount();
    }
}
