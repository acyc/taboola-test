package com.taboola.api.validators;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = TimeBucketValidator.class)
public @interface TimeBucket {
    String message() default "Invalid time value";

    String pattern() default "yyyyMMddHHmm";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
