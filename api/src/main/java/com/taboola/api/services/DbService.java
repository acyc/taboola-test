package com.taboola.api.services;

import com.taboola.api.domains.Event;

import java.util.List;
import java.util.Map;

public interface DbService {
    List<Event> queryEvent(final String sql, final Map<Integer, Object> statementParam) ;
}
