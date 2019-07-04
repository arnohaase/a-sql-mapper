package com.ajjpj.asqlmapper.demo.rel;

import com.ajjpj.acollections.AList;

public interface AbstractPersonWithAddresses {
    Long id ();
    String name ();
    AList<Address> addresses ();
}
