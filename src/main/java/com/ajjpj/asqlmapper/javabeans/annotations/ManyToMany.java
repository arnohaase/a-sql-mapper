package com.ajjpj.asqlmapper.javabeans.annotations;

public @interface ManyToMany {
    String manyManyTable();
    String fkToMaster() default "";
    String fkToDetail() default "";

    String detailTable() default "";
    String detailPk() default "";
}
