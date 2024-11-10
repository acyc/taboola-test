package com.taboola.api.validators;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class TimeBucketValidator implements ConstraintValidator<TimeBucket, String> {
    private SimpleDateFormat dateFormat;

    @Override
    public void initialize(TimeBucket timeBucket) {
        this.dateFormat = new SimpleDateFormat(timeBucket.pattern());
        this.dateFormat.setLenient(false);
    }

    /**
     * Check if the input timeBucket value can be parsed successfully, by the provided pattern.
     * @param inputValue - the input timeBucket value
     * @param constraintValidatorContext - the constraintValidatorContext
     * @return true if the value can be parsed successfully. Otherwise, return false.
     */
    @Override
    public boolean isValid(String inputValue, ConstraintValidatorContext constraintValidatorContext) {
        try {
            dateFormat.parse(inputValue);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }
}
