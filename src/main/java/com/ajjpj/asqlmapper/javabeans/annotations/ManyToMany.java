package com.ajjpj.asqlmapper.javabeans.annotations;

import java.lang.annotation.*;

@Target({ ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ManyToMany {
    String manyManyTable();
    String fkToMaster() default "";
    String fkToDetail() default "";

    String detailTable() default "";
    String detailPk() default "";
}
