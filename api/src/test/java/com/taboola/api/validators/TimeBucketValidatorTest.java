package com.taboola.api.validators;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimeBucketValidatorTest {
    @Mock
    private TimeBucket timeBucket;
    private TimeBucketValidator validator;

    @BeforeEach
    void setUp() {
        when(timeBucket.pattern()).thenReturn("yyyyMMddHHmm");
        this.validator = new TimeBucketValidator();
        validator.initialize(timeBucket);
    }

    @Test
    void testIsValid() {
        boolean result = this.validator.isValid("202411051254", null);
        assertTrue(result);
    }

    @Test
    void testIsValidWrongValue() {
        boolean result = this.validator.isValid("999999999999", null);
        assertFalse(result);
    }

    @Test
    void testIsValidWrongFormat() {
        boolean result = this.validator.isValid("20241105125400", null);
        assertFalse(result);
    }

    @Test
    void testIsValidInvalidString() {
        boolean result = this.validator.isValid("not_date", null);
        assertFalse(result);
    }
}