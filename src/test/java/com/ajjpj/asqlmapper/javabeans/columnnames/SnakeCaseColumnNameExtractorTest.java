package com.ajjpj.asqlmapper.javabeans.columnnames;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SnakeCaseColumnNameExtractorTest {
    @Test
    void testSnakeCase() {
        assertEquals("my_simple_property", new SnakeCaseColumnNameExtractor().propertyNameToColumnName(Object.class, "mySimpleProperty"));
        assertEquals("first_and_middle_name", new SnakeCaseColumnNameExtractor().propertyNameToColumnName(Object.class, "firstAndMiddleName"));
    }

    @Test
    void testFirstUpper() {
        assertEquals("url", new SnakeCaseColumnNameExtractor().propertyNameToColumnName(Object.class, "URL"));
        assertEquals("qrcode", new SnakeCaseColumnNameExtractor().propertyNameToColumnName(Object.class, "QRcode"));
    }

    @Test
    void testEmpty() {
        assertEquals("", new SnakeCaseColumnNameExtractor().propertyNameToColumnName(Object.class, ""));
    } 
}
