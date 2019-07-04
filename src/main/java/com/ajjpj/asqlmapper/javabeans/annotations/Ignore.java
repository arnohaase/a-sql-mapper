package com.ajjpj.asqlmapper.javabeans.annotations;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Ignore {
    boolean value() default true;
}
