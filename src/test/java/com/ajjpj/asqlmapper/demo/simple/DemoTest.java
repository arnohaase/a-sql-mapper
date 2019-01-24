package com.ajjpj.asqlmapper.demo.simple;

import com.ajjpj.acollections.AMap;
import com.ajjpj.asqlmapper.SqlEngine;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.core.AInsert;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.LoggingListener;
import com.ajjpj.asqlmapper.mapper.BuilderBasedRowExtractor;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistryImpl;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.ImmutableWithBuilderMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.GuessingPkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.DefaultTableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import org.junit.jupiter.api.*;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;


class DemoTest extends AbstractDatabaseTest {
    private SqlEngine engine;
    private LoggingListener loggingListener;

    @BeforeEach void setUp() throws SQLException {
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200))").executeUpdate();
        loggingListener = LoggingListener.createWithStatistics(1000);
        engine = SqlEngine
                .create()
                .withDefaultPkName("id")
                .withRowExtractor(new BuilderBasedRowExtractor()) //TODO provide this from the mapper
                .withListener(loggingListener)
                .withDefaultConnectionSupplier(() -> conn)
                ;
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.prepareStatement("drop table person").executeUpdate();
        System.out.println(loggingListener.getStatistics());
        System.out.println(loggingListener.getStatistics().getStatementStatistics().mkString("\n"));
    }


    @Test void testDirectJdbc() {
        final AQuery<Long> q1 = engine.longQuery(SqlSnippet.sql("select count(*) from person"));
        assertEquals(Long.valueOf(0), q1.single());

        final AInsert<Long> i1 = engine.insertLongPk("insert into person (name) values (?)", "Arno");
        assertEquals(Long.valueOf(1), i1.executeSingle());
        assertEquals(Long.valueOf(2), i1.executeSingle());
        assertEquals(Long.valueOf(3), i1.executeSingle());

        assertEquals(Long.valueOf(3), q1.single());


        assertEquals (Person.of(2L, "Arno"), engine.query(Person.class, "select * from person where id=?", 2).single());
    }

    @Test void testMapper() throws SQLException {
        //TODO simplify setup: convenience factory, defaults, ...
        final SqlMapper mapper = new SqlMapper(engine, new BeanRegistryImpl(new SchemaRegistry(DatabaseDialect.H2),
                new DefaultTableNameExtractor(),
                new GuessingPkStrategyDecider(),
                new ImmutableWithBuilderMetaDataExtractor()
                ), ds);

        final Person inserted1 = mapper.insert(Person.of(0L, "Arno"));
        final Person inserted2 = mapper.insert(Person.of(0L, "Arno"));
        assertEquals(Person.of(1L, "Arno"), inserted1);
        assertEquals(Person.of(2L, "Arno"), inserted2);

        assertTrue(mapper.update(inserted1.withName("Arno Haase")));
        assertEquals("Arno Haase", engine.query(Person.class, "select * from person where id=?", 1).single().name());
        assertEquals("Arno", engine.query(Person.class, "select * from person where id=?", 2).single().name());

        assertTrue(mapper.patch(Person.class, 1L, AMap.of("name", "whoever")));
        assertEquals("whoever", engine.query(Person.class, "select * from person where id=?", 1).single().name());
        assertEquals("Arno", engine.query(Person.class, "select * from person where id=?", 2).single().name());

        assertTrue(mapper.delete(inserted1));
        assertEquals(0L, engine.longQuery("select count(*) from person where id=?", 1).single().longValue());
        assertFalse(mapper.delete(inserted1));

        assertTrue(mapper.delete(Person.class, 2L));
        assertEquals(0L, engine.longQuery("select count(*) from person where id=?", 2).single().longValue());
        assertFalse(mapper.delete(Person.class, 2L));
    }
}
