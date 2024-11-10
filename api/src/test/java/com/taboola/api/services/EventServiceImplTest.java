package com.taboola.api.services;

import com.taboola.api.exceptions.NotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class EventServiceImplTest {
    @Mock
    private DbService dbService;

    private EventServiceImpl eventService;

    @BeforeEach
    void setUp() {
        eventService = new EventServiceImpl(dbService);
    }

    @Test
    void testGetEventsByTimeBucket_Has3() {
        String timeBucket = "202411100101";
        Map<String, Long> expectResult = new HashMap<>();
        expectResult.put("5", 56L);
        expectResult.put("33", 546L);
        expectResult.put("77", 756L);
        when(dbService.queryEventByTimeBucket(timeBucket)).thenReturn(expectResult);

        Map<String, Long> result = eventService.getEventsByTimeBucket(timeBucket);
        assertEquals(3, result.size());
        expectResult.forEach((expectEventId, expectCount) -> {
            assertEquals(expectCount, result.get(expectEventId));
        });
    }

    @Test
    void testGetEventsByTimeBucket_NotFound() {
        String timeBucket = "202411100101";
        Map<String, Long> expectResult = new HashMap<>();
        when(dbService.queryEventByTimeBucket(timeBucket)).thenReturn(expectResult);
        Exception exception = assertThrows(NotFoundException.class, () -> {
            eventService.getEventsByTimeBucket(timeBucket);
        });

        assertNotNull(exception);
    }

    @Test
    void testCountByTimeBucketAndEventId_HasResult() {
        String timeBucket = "202411100101";
        int eventId = 77;
        Long expectResult = 856L;
        when(dbService.queryCountByTimeBucketAndEventId(timeBucket, eventId)).thenReturn(expectResult);

        Long countResult = eventService.getEventByTimeBucketAndEventId(timeBucket, eventId);
        assertEquals(expectResult, countResult);
    }

    @Test
    void testCountByTimeBucketAndEventId_HasResult0() {
        String timeBucket = "202411100101";
        int eventId = 77;
        Long expectResult = 0L;
        when(dbService.queryCountByTimeBucketAndEventId(timeBucket, eventId)).thenReturn(expectResult);

        Long countResult = eventService.getEventByTimeBucketAndEventId(timeBucket, eventId);
        assertEquals(expectResult, countResult);
    }

    @Test
    void testCountByTimeBucketAndEventId_NotFound() {
        String timeBucket = "202411100101";
        int eventId = 77;
        Long expectResult = null;
        when(dbService.queryCountByTimeBucketAndEventId(timeBucket, eventId)).thenReturn(expectResult);

        Exception exception = assertThrows(NotFoundException.class, () -> {
            eventService.getEventByTimeBucketAndEventId(timeBucket, eventId);
        });

        assertNotNull(exception);
    }


}