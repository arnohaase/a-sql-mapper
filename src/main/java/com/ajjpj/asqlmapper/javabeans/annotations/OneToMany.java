package com.ajjpj.asqlmapper.javabeans.annotations;

import java.lang.annotation.*;

@Target({ ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OneToMany {
    Class<?> elementType() default Void.class;
    String elementTable() default "";
    String fkName() default "";
}
