package com.ajjpj.asqlmapper.core.impl;

/**
 * This is the interface handlers must implement for registration in a {@link CanHandleRegistry}.
 */
public interface CanHandle {
    boolean canHandle(Class<?> cls);
}
