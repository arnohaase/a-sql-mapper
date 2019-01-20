package com.ajjpj.asqlmapper.demo.simple;

import com.ajjpj.asqlmapper.ASqlEngine;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.core.AInsert;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlSnippet;
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
    private ASqlEngine engine;

    @BeforeEach void setUp() throws SQLException {
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200))").executeUpdate();
        engine = ASqlEngine
                .create()
                .withDefaultPkName("id")
                .withRowExtractor(new BuilderBasedRowExtractor()) //TODO provide this from the mapper
                ;
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.prepareStatement("drop table person").executeUpdate();
    }


    @Test void testDirectJdbc() throws Exception {
        final AQuery<Long> q1 = engine.longQuery(SqlSnippet.sql("select count(*) from person"));
        assertEquals(Long.valueOf(0), q1.single(conn));

        final AInsert<Long> i1 = engine.insertLongPk("insert into person (name) values (?)", "Arno");
        assertEquals(Long.valueOf(1), i1.executeSingle(conn));
        assertEquals(Long.valueOf(2), i1.executeSingle(conn));
        assertEquals(Long.valueOf(3), i1.executeSingle(conn));

        assertEquals(Long.valueOf(3), q1.single(conn));


        assertEquals (Person.of(2L, "Arno"), engine.query(Person.class, "select * from person where id=?", 2).single(conn));
    }

    @Test void testMapper() throws SQLException {
        //TODO simplify setup: convenience factory, defaults, ...
        final SqlMapper mapper = new SqlMapper(engine, new BeanRegistryImpl(new SchemaRegistry(DatabaseDialect.H2),
                new DefaultTableNameExtractor(),
                new GuessingPkStrategyDecider(),
                new ImmutableWithBuilderMetaDataExtractor()
                ), ds);

        final Person inserted1 = mapper.insert(conn, Person.of(0L, "Arno"));
        final Person inserted2 = mapper.insert(conn, Person.of(0L, "Arno"));
        assertEquals(Person.of(1L, "Arno"), inserted1);
        assertEquals(Person.of(2L, "Arno"), inserted2);

        assertEquals(1, mapper.update(conn, inserted1.withName("Arno Haase")));

        assertEquals("Arno Haase", engine.query(Person.class, "select * from person where id=?", 1).single(conn).name());

        assertTrue(mapper.delete(conn, inserted1));
        assertEquals(0L, engine.longQuery("select count(*) from person where id=?", 1).single(conn).longValue());
        assertFalse(mapper.delete(conn, inserted1));

        assertTrue(mapper.delete(conn, Person.class, 2L));
        assertEquals(0L, engine.longQuery("select count(*) from person where id=?", 2).single(conn).longValue());
        assertFalse(mapper.delete(conn, Person.class, 2L));
    }
}
