package com.ajjpj.asqlmapper.javabeans.annotations;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Table {
    String value();
}
