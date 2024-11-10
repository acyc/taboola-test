package com.taboola.api;

import java.util.Map;

import org.hibernate.validator.constraints.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.taboola.api.services.EventService;
import com.taboola.api.validators.TimeBucket;

@RestController()
@RequestMapping("/api/counters")
@Validated
public class CounterController {

    private final EventService eventService;

    @Autowired
    public CounterController(EventService eventService) {
        this.eventService = eventService;
    }

    @GetMapping(value = "/time/{timeBucket}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Long> getEventsByTimeBucket(@TimeBucket @PathVariable final String timeBucket) {
        return eventService.getEventsByTimeBucket(timeBucket);
    }


    @GetMapping(value = "/time/{timeBucket}/eventId/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public long getCountByTimeBucketAnEventId(@TimeBucket @PathVariable final String timeBucket,
                              @Range(min = 0, max = 99, message = "Invalid Event ID value") @PathVariable final String eventId) {
        return eventService.getEventByTimeBucketAndEventId(timeBucket, Integer.parseInt(eventId));
    }
}
