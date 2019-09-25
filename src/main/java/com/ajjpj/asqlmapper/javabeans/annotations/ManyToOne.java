package com.ajjpj.asqlmapper.javabeans.annotations;

import java.lang.annotation.*;

@Target({ ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ManyToOne {
    String referencedTable() default "";
    String fk() default "";

    /**
     * only evaluated if fk() is specified - otherwise the database FK constraint's target column is used
     */
    String pk() default "";
}
