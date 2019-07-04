package com.ajjpj.asqlmapper.demo.rel;

import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.javabeans.annotations.ManyToMany;

public interface AbstractPersonWithAddressesManyToMany {
    Long id();
    String name();

    @ManyToMany(manyManyTable = "person_address")
    AList<Address> addresses();
}
