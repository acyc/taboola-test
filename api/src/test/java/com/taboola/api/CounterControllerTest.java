package com.taboola.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taboola.api.exceptions.NotFoundException;
import com.taboola.api.services.EventService;

@WebMvcTest(CounterController.class)
class CounterControllerTest {

    private static final String EVENTS_ENDPOINT = "/api/counters/time/{timeBucket}";
    private static final String COUNTS_ENDPOINT = "/api/counters/time/{timeBucket}/eventId/{eventId}";
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EventService service;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

    }

    @Test
    void testGetEventsByTimeBucket() throws Exception {

        String timeBucket = "202411100101";
        Map<String, Long> expectResult = new HashMap<>();
        expectResult.put("5", 56L);
        expectResult.put("33", 546L);
        expectResult.put("77", 756L);

        when(service.getEventsByTimeBucket(timeBucket)).thenReturn(expectResult);
        MvcResult mvcResult = this.mockMvc.perform(get(EVENTS_ENDPOINT, timeBucket)).andDo(print()).andExpect(status().isOk())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(3, result.size());
        expectResult.forEach((expectEventId, expectCount) -> {
            assertEquals(expectCount, result.get(expectEventId).asLong());
        });

    }

    @Test
    void testGetEventsByTimeBucket_NotFound() throws Exception {

        String timeBucket = "202411100101";
        NotFoundException notFoundException = new NotFoundException(timeBucket);
        when(service.getEventsByTimeBucket(timeBucket)).thenThrow(notFoundException);
        MvcResult mvcResult = this.mockMvc.perform(get(EVENTS_ENDPOINT, timeBucket)).andDo(print()).andExpect(status().isNotFound())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(notFoundException.getMessage(), result.get("error").asText());

    }

    @Test
    void testGetEventsByTimeBucket_InvalidTimeBucket() throws Exception {
        String timeBucket = "202411100199";

        MvcResult mvcResult = this.mockMvc.perform(get(EVENTS_ENDPOINT, timeBucket)).andDo(print()).andExpect(status().isBadRequest())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals("Invalid time value", result.get("error").asText());

    }

    @Test
    void testGetCountByTimeBucketAnEventId() throws Exception {
        String timeBucket = "202411100101";
        int eventId = 77;
        Long expectResult = 985L;

        when(service.getEventByTimeBucketAndEventId(timeBucket, eventId)).thenReturn(expectResult);
        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isOk())
                .andReturn();

        Long result = Long.valueOf(mvcResult.getResponse().getContentAsString());

        assertEquals(expectResult, result);
    }

    @Test
    void testGetCountByTimeBucketAnEventId_NotFound() throws Exception {
        String timeBucket = "202411100101";
        int eventId = 77;
        Long expectResult = 985L;
        NotFoundException notFoundException = new NotFoundException(timeBucket, eventId);
        when(service.getEventByTimeBucketAndEventId(timeBucket, eventId)).thenThrow(notFoundException);

        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isNotFound())
                .andReturn();

        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals(notFoundException.getMessage(), result.get("error").asText());
    }

    @Test
    void testGetCountByTimeBucketAnEventId_InvalidTimeBucket() throws Exception {

        String timeBucket = "202411100199";
        int eventId = 77;

        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isBadRequest())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals("Invalid time value", result.get("error").asText());

    }

    @Test
    void testGetCountByTimeBucketAnEventId_InvalidEventId100() throws Exception {

        String timeBucket = "202411100101";
        int eventId = 100;

        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isBadRequest())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals("Invalid Event ID value", result.get("error").asText());

    }

    @Test
    void testGetCountByTimeBucketAnEventId_InvalidEventIdNegative() throws Exception {

        String timeBucket = "202411100101";
        int eventId = -1;

        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isBadRequest())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals("Invalid Event ID value", result.get("error").asText());

    }

    @Test
    void testGetCountByTimeBucketAnEventId_InvalidEventIdString() throws Exception {

        String timeBucket = "202411100101";
        String eventId = "abc";

        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isBadRequest())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals("Invalid Event ID value", result.get("error").asText());

    }

    @Test
    void testGetCountByTimeBucketAnEventId_InvalidTimeBucketAndEventId() throws Exception {

        String timeBucket = "202411100199";
        int eventId = 777;

        MvcResult mvcResult = this.mockMvc.perform(get(COUNTS_ENDPOINT, timeBucket, eventId)).andDo(print()).andExpect(status().isBadRequest())
                .andReturn();
        JsonNode result = this.objectMapper.readTree(mvcResult.getResponse().getContentAsString());

        assertEquals(2, result.size());
        assertEquals("Invalid time value, Invalid Event ID value", result.get("error").asText());

    }
}