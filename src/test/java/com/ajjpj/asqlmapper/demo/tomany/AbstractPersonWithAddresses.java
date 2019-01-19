package com.ajjpj.asqlmapper.demo.tomany;


import com.ajjpj.acollections.AList;


public interface AbstractPersonWithAddresses {
    Long id ();
    String name ();
    AList<Address> addresses ();
}
