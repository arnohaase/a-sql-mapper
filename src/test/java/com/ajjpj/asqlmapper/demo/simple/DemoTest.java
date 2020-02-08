package com.ajjpj.asqlmapper.demo.simple;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Wither;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ajjpj.acollections.AMap;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.SqlMapperBuilder;
import com.ajjpj.asqlmapper.core.AInsert;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlEngine;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.LoggingListener;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;

class DemoTest extends AbstractDatabaseTest {
    @SuppressWarnings("WeakerAccess")
    @Value @Builder
    static class Person {
        @Wither long id;
        String name;
    }

    private SqlMapperBuilder builder = new SqlMapperBuilder()
            .withDefaultPkName("id")
            .withBeanStyle(SqlMapperBuilder.BeanStyle.lombok)
            .withDefaultConnectionSupplier(() -> conn);
    private SqlEngine engine;

    @BeforeEach
    void setUp() throws SQLException {
        executeUpdate("create table person(id bigserial primary key, name varchar(200))");
        engine = builder.build(DatabaseDialect.H2).engine();
    }

    @AfterEach
    void tearDown() throws SQLException {
        executeUpdate("drop table person");
        System.out.println(LoggingListener.getAllStatistics().mkString("\n"));
    }

    @Test
    void testDirectJdbc() {
        final AQuery<Long> q1 = engine.longQuery(SqlSnippet.sql("select count(*) from person"));
        assertEquals(Long.valueOf(0), q1.single());

//        final AInsert<Long> i1 = engine.insertLongPk("insert into person (name) values (?)", "Arno");
//        assertEquals(Long.valueOf(1), i1.executeSingle());
//        assertEquals(Long.valueOf(2), i1.executeSingle());
//        assertEquals(Long.valueOf(3), i1.executeSingle());

        assertEquals(Long.valueOf(3), q1.single());

        assertEquals(new Person(2, "Arno"), engine.query(Person.class, "select * from person where id=?", 2).single());
    }

    @Test
    void testMapper() {
        final SqlMapper mapper = builder.build(DatabaseDialect.H2);

        final Person inserted1 = mapper.insert(new Person(0, "Arno"));
        final Person inserted2 = mapper.insert(new Person(0, "Arno"));
        assertEquals(new Person(1, "Arno"), inserted1);
        assertEquals(new Person(2, "Arno"), inserted2);

        assertTrue(mapper.update(new Person(inserted1.getId(), "Arno Haase")));
        assertEquals("Arno Haase", engine.query(Person.class, "select * from person where id=?", 1).single().getName());
        assertEquals("Arno", engine.query(Person.class, "select * from person where id=?", 2).single().getName());

        assertTrue(mapper.patch(Person.class, 1L, AMap.of("name", "whoever")));
        assertEquals("whoever", engine.query(Person.class, "select * from person where id=?", 1).single().getName());
        assertEquals("Arno", engine.query(Person.class, "select * from person where id=?", 2).single().getName());
        assertTrue(mapper.patch(Person.class, 1L, AMap.of("no-mapped-prop", "pling")));

        assertTrue(mapper.delete(inserted1));
        assertEquals(0L, engine.longQuery("select count(*) from person where id=?", 1).single().longValue());
        assertFalse(mapper.delete(inserted1));

        assertTrue(mapper.delete(Person.class, 2L));
        assertEquals(0L, engine.longQuery("select count(*) from person where id=?", 2).single().longValue());
        assertFalse(mapper.delete(Person.class, 2L));
    }
}
