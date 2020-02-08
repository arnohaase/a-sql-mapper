package com.ajjpj.asqlmapper.core.impl;

import static com.ajjpj.asqlmapper.core.common.CommonPrimitiveHandlers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.annotation.RetentionPolicy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.time.*;
import java.util.Date;
import java.util.UUID;
import java.util.function.Function;

import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import org.junit.jupiter.api.Test;

public class CommonPrimitiveHandlersTest extends AbstractDatabaseTest {

    @Test
    void testString() throws SQLException {
        assertTrue(STRING_HANDLER.canHandle(String.class));
        assertFalse(STRING_HANDLER.canHandle(Clob.class));

        assertEquals("", STRING_HANDLER.fromSql(String.class, ""));
        assertEquals(" ", STRING_HANDLER.fromSql(String.class, " "));
        assertEquals("abc", STRING_HANDLER.fromSql(String.class, "abc"));
        assertEquals(" a bc ", STRING_HANDLER.fromSql(String.class, " a bc "));

        final Clob clob = conn.createClob();
        clob.setString(1, "");
        assertEquals("", STRING_HANDLER.fromSql(String.class, clob));
        clob.setString(1, "asdf");
        assertEquals("asdf", STRING_HANDLER.fromSql(String.class, clob));

        final NClob nclob = conn.createNClob();
        nclob.setString(1, "");
        assertEquals("", STRING_HANDLER.fromSql(String.class, nclob));
        nclob.setString(1, "asdf");
        assertEquals("asdf", STRING_HANDLER.fromSql(String.class, nclob));

        assertEquals("", STRING_HANDLER.toSql(""));
        assertEquals(" ", STRING_HANDLER.toSql(" "));
        assertEquals("abc", STRING_HANDLER.toSql("abc"));
        assertEquals(" a bc ", STRING_HANDLER.toSql(" a bc "));
    }

    @Test
    void testBoolean() {
        assertTrue(BOOLEAN_HANDLER.canHandle(Boolean.class));
        assertTrue(BOOLEAN_HANDLER.canHandle(boolean.class));
        assertFalse(BOOLEAN_HANDLER.canHandle(int.class));

        assertEquals(true, BOOLEAN_HANDLER.fromSql(Boolean.class, true));
        assertEquals(true, BOOLEAN_HANDLER.fromSql(boolean.class, true));
        assertEquals(false, BOOLEAN_HANDLER.fromSql(Boolean.class, false));
        assertEquals(false, BOOLEAN_HANDLER.fromSql(boolean.class, false));

        assertEquals(true, BOOLEAN_HANDLER.toSql(true));
        assertEquals(false, BOOLEAN_HANDLER.toSql(false));
    }

    @Test
    void testNumber() {
        checkNumber(Byte.class, Integer::byteValue);
        checkNumber(byte.class, Integer::byteValue);
        checkNumber(Short.class, Integer::shortValue);
        checkNumber(short.class, Integer::shortValue);
        checkNumber(Integer.class, i -> i);
        checkNumber(int.class, i -> i);
        checkNumber(Long.class, Integer::longValue);
        checkNumber(long.class, Integer::longValue);

        checkNumber(Double.class, Integer::doubleValue);
        checkNumber(double.class, Integer::doubleValue);
        checkNumber(Float.class, Integer::floatValue);
        checkNumber(float.class, Integer::floatValue);

        checkNumber(BigInteger.class, BigInteger::valueOf);
        checkNumber(BigDecimal.class, BigDecimal::valueOf);

        assertFalse(NUMERIC_HANDLER.canHandle(Character.class));
        assertFalse(NUMERIC_HANDLER.canHandle(char.class));
        assertFalse(NUMERIC_HANDLER.canHandle(String.class));
    }

    private <T> void checkNumber(Class<T> cls, Function<Integer, T> fromInt) {
        assertTrue(NUMERIC_HANDLER.canHandle(cls));

        assertEquals(fromInt.apply(1), NUMERIC_HANDLER.fromSql(cls, 1));
        assertEquals(fromInt.apply(2), NUMERIC_HANDLER.fromSql(cls, 2));

        assertEquals(fromInt.apply(1), NUMERIC_HANDLER.toSql(fromInt.apply(1)));
        assertEquals(fromInt.apply(2), NUMERIC_HANDLER.toSql(fromInt.apply(2)));
    }

    enum TestEnum {
        a, B, b, c
    }

    @Test
    void testEnumAsString() {
        assertTrue(ENUM_AS_STRING_HANDLER.canHandle(RetentionPolicy.class));
        assertTrue(ENUM_AS_STRING_HANDLER.canHandle(TestEnum.class));
        assertFalse(ENUM_AS_STRING_HANDLER.canHandle(String.class));

        assertEquals(TestEnum.a, ENUM_AS_STRING_HANDLER.fromSql(TestEnum.class, "a"));
        assertEquals(TestEnum.B, ENUM_AS_STRING_HANDLER.fromSql(TestEnum.class, "B"));
        assertEquals(TestEnum.b, ENUM_AS_STRING_HANDLER.fromSql(TestEnum.class, "b"));
        assertEquals(TestEnum.c, ENUM_AS_STRING_HANDLER.fromSql(TestEnum.class, "c"));

        assertEquals("a", ENUM_AS_STRING_HANDLER.toSql(TestEnum.a));
        assertEquals("B", ENUM_AS_STRING_HANDLER.toSql(TestEnum.B));
        assertEquals("b", ENUM_AS_STRING_HANDLER.toSql(TestEnum.b));
        assertEquals("c", ENUM_AS_STRING_HANDLER.toSql(TestEnum.c));
    }

    @Test
    void testLocalDate() {
        assertTrue(LOCAL_DATE_HANDLER.canHandle(LocalDate.class));
        assertFalse(LOCAL_DATE_HANDLER.canHandle(LocalDateTime.class));
        assertFalse(LOCAL_DATE_HANDLER.canHandle(ZonedDateTime.class));

        assertEquals(LocalDate.of(2018, 11, 23), LOCAL_DATE_HANDLER.fromSql(LocalDate.class, LocalDate.of(2018, 11, 23)));
        assertEquals(LocalDate.of(2018, 11, 23), LOCAL_DATE_HANDLER.fromSql(LocalDate.class, new java.sql.Date(118, 10, 23)));

        assertEquals(LocalDate.of(2022, 8, 31), LOCAL_DATE_HANDLER.toSql(LocalDate.of(2022, 8, 31)));
    }

    @Test
    void testLocalTime() {
        assertTrue(LOCAL_TIME_HANDLER.canHandle(LocalTime.class));
        assertFalse(LOCAL_TIME_HANDLER.canHandle(LocalDateTime.class));
        assertFalse(LOCAL_TIME_HANDLER.canHandle(Instant.class));

        assertEquals(LocalTime.of(15, 11, 23), LOCAL_TIME_HANDLER.fromSql(LocalTime.class, LocalTime.of(15, 11, 23)));
        assertEquals(LocalTime.of(15, 11, 23), LOCAL_TIME_HANDLER.fromSql(LocalTime.class, new java.sql.Time(15, 11, 23)));

        assertEquals(LocalTime.of(15, 8, 31), LOCAL_TIME_HANDLER.toSql(LocalTime.of(15, 8, 31)));
    }

    @Test
    void testInstant() {
        assertTrue(INSTANT_HANDLER.canHandle(Instant.class));
        assertFalse(INSTANT_HANDLER.canHandle(ZonedDateTime.class));
        assertFalse(INSTANT_HANDLER.canHandle(Date.class));

        assertEquals(Instant.ofEpochMilli(1234567), INSTANT_HANDLER.fromSql(Instant.class, Instant.ofEpochMilli(1234567)));
        assertEquals(Instant.ofEpochMilli(1234567), INSTANT_HANDLER.fromSql(Instant.class, new java.sql.Timestamp(1234567)));

        assertEquals(Instant.ofEpochMilli(12345678), INSTANT_HANDLER.toSql(Instant.ofEpochMilli(12345678)));
    }

    @Test
    void testUuidAsString() {
        assertTrue(UUID_AS_STRING_HANDLER.canHandle(UUID.class));
        assertFalse(UUID_AS_STRING_HANDLER.canHandle(String.class));

        final UUID uuid = UUID.randomUUID();

        assertEquals(uuid, UUID_AS_STRING_HANDLER.fromSql(UUID.class, uuid.toString()));
        assertEquals(uuid.toString(), UUID_AS_STRING_HANDLER.toSql(uuid));
    }

    @Test
    void testUuidPassThrough() {
        assertTrue(UUID_SUPPORT_HANDLER.canHandle(UUID.class));
        assertFalse(UUID_SUPPORT_HANDLER.canHandle(String.class));

        final UUID uuid = UUID.randomUUID();

        assertEquals(uuid, UUID_SUPPORT_HANDLER.fromSql(UUID.class, uuid));
        assertEquals(uuid, UUID_SUPPORT_HANDLER.toSql(uuid));
    }

    @Test
    void testBlobPassThrough() throws SQLException {
        assertTrue(BLOB_ETC_PASSTHROUGH_HANDLER.canHandle(Blob.class));
        assertTrue(BLOB_ETC_PASSTHROUGH_HANDLER.canHandle(Clob.class));
        assertTrue(BLOB_ETC_PASSTHROUGH_HANDLER.canHandle(NClob.class));
        assertFalse(BLOB_ETC_PASSTHROUGH_HANDLER.canHandle(byte[].class));
        assertFalse(BLOB_ETC_PASSTHROUGH_HANDLER.canHandle(String.class));

        final Blob blob = conn.createBlob();
        final Clob clob = conn.createClob();
        final NClob nclob = conn.createNClob();

        assertSame(blob, BLOB_ETC_PASSTHROUGH_HANDLER.fromSql(Blob.class, blob));
        assertSame(clob, BLOB_ETC_PASSTHROUGH_HANDLER.fromSql(Blob.class, clob));
        assertSame(nclob, BLOB_ETC_PASSTHROUGH_HANDLER.fromSql(Blob.class, nclob));

        assertSame(blob, BLOB_ETC_PASSTHROUGH_HANDLER.toSql(blob));
        assertSame(clob, BLOB_ETC_PASSTHROUGH_HANDLER.toSql(clob));
        assertSame(nclob, BLOB_ETC_PASSTHROUGH_HANDLER.toSql(nclob));
    }
}
