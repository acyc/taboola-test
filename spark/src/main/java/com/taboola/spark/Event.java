package com.taboola.spark;

public class Event {
    private final String timeBucket;
    private final long eventId;
    private final long count;

    public Event(String timeBucket, long eventId, long count) {
        this.timeBucket = timeBucket;
        this.eventId = eventId;
        this.count = count;
    }


    public String getTimeBucket() {
        return timeBucket;
    }


    public long getEventId() {
        return eventId;
    }


    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "Event{" +
               "timeBucket='" + timeBucket + '\'' +
               ", eventId=" + eventId +
               ", count=" + count +
               '}';
    }
}
