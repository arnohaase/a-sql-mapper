package com.ajjpj.asqlmapper.core.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.google.common.base.Suppliers;

class ABatchUpdateTest extends AbstractDatabaseTest {

    @Test
    void emptyBatchReturnsGraceful() {
        int[] result = new ABatchUpdate(Collections.emptyList(),
                PrimitiveTypeRegistry.defaults(),
                AVector.empty(),
                AOption.of(Suppliers.ofInstance(conn))
                ).execute();
        assertArrayEquals(new int[0], result);
    }

}