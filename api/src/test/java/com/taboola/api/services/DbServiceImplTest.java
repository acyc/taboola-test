package com.taboola.api.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

@ExtendWith(MockitoExtension.class)
class DbServiceImplTest {
    private static final String COL_EVENT_ID = "event_id";
    private static final String COL_COUNT = "count";
    @Mock
    private JdbcTemplate jdbcTemplate;

    private DbService dbService;


    @BeforeEach
    void setUp() {
        this.dbService = new DbServiceImpl(jdbcTemplate);
    }

    @Test
    void testQueryEventByTimeBucket() {
        String timeBucket = "202411100101";
        Object[] queryParams = new Object[]{timeBucket};
        doAnswer(invocation -> {

            RowCallbackHandler handler = invocation.getArgument(2);
            for (int i = 1; i < 4; i++) {
                ResultSet rs1 = Mockito.mock(ResultSet.class);
                when(rs1.getString(COL_EVENT_ID)).thenReturn(String.valueOf(5 * i));
                when(rs1.getLong(COL_COUNT)).thenReturn(Long.valueOf(10 * i));
                handler.processRow(rs1);
            }

            return null;
        }).when(jdbcTemplate).query(eq(SqlString.EVENTS_BY_TIME_BUCKET.getSql()), eq(queryParams), any(RowCallbackHandler.class));

        Map<String, Long> result = dbService.queryEventByTimeBucket(timeBucket);
        assertEquals(3, result.size());
        for (int i = 1; i < 4; i++) {
            assertEquals(Long.valueOf(10 * i), result.get(String.valueOf(5 * i)));
        }
    }

    @Test
    void testQueryCountByTimeBucketAndEventId_HasResult() {
        String timeBucket = "202411100101";
        int eventId = 77;
        Object[] queryParams = new Object[]{timeBucket, eventId};
        Long expectResult = 10L;
        when(jdbcTemplate.queryForObject(SqlString.COUNT_BY_TIME_BUCKET_AND_EVENT_ID.getSql(), queryParams, Long.class)).
                thenReturn(expectResult);

        Long result = dbService.queryCountByTimeBucketAndEventId(timeBucket, eventId);
        assertEquals(expectResult, result);
    }

    @Test
    void testQueryCountByTimeBucketAndEventId_NotFound() {
        String timeBucket = "202411100101";
        int eventId = 77;
        Object[] queryParams = new Object[]{timeBucket, eventId};

        when(jdbcTemplate.queryForObject(SqlString.COUNT_BY_TIME_BUCKET_AND_EVENT_ID.getSql(), queryParams, Long.class))
                .thenThrow(new EmptyResultDataAccessException(1));

        Long count = dbService.queryCountByTimeBucketAndEventId(timeBucket, eventId);

        Assertions.assertNull(count);
    }
}