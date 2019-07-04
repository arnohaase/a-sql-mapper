package com.ajjpj.asqlmapper.demo.rel;

import org.immutables.value.Value;

@Value.Style(typeImmutable = "*", allParameters = true)
@Value.Immutable
public interface AbstractPerson {
    Long id ();
    String name ();
}
