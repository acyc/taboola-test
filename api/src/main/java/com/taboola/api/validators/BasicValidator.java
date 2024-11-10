package com.taboola.api.validators;

import com.taboola.api.exceptions.BadRequestException;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;

@Component
public class BasicValidator {

    private static final String PATTERN = "yyyyMMddHHmm";

    public void validateTimeBucket(final String timeBucket){
        final SimpleDateFormat df = new SimpleDateFormat(PATTERN);
        try {
            df.setLenient(false);
            df.parse(timeBucket);
        } catch (ParseException e) {
            throw new BadRequestException(String.format("Invalid time bucket value: %s",timeBucket));
        }
    }

    public void validateEventId(final int eventId){
        if (eventId<0 || eventId>99){
            throw new BadRequestException(String.format("Invalid event ID: %s",eventId));
        }
    }
}
