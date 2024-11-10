package com.taboola.api.exceptions;

public class NotFoundException extends RuntimeException {
    public NotFoundException(final String timeBucket){
        super(String.format("Unable to find events in this time bucket: %s",timeBucket));
    }

    public NotFoundException(final String timeBucket, final int eventId){
        super(String.format("Unable to find Event ID: %s, in this time bucket: %s",eventId, timeBucket));
    }
}
