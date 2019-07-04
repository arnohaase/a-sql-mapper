package com.ajjpj.asqlmapper.core.common;

import java.util.stream.Stream;

public interface SqlStream<T> extends Stream<T> {
    /**
     * This method gives access to the current row during processing.<p>
     *
     * This goes against the fundamental abstraction of {@link Stream} and should not be used for regular processing, but
     *  it allows handling special cases where e.g. unmapped columns are evaluated to create object structures.
     */
    SqlRow currentRow();
}
