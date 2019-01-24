package com.ajjpj.asqlmapper.demo.snippets;

import com.ajjpj.asqlmapper.SqlEngine;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistryImpl;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.ImmutableWithBuilderMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.GuessingPkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.DefaultTableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static com.ajjpj.asqlmapper.core.SqlSnippet.concat;
import static com.ajjpj.asqlmapper.core.SqlSnippet.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class SnippetCompositionDemoTest extends AbstractDatabaseTest  {
    private SqlMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200))").executeUpdate();
        conn.prepareStatement("create table person_permissions(person_id bigint references person, user_id bigint, primary key(person_id, user_id))").executeUpdate();
        SqlEngine engine = SqlEngine
                .create()
                .withDefaultConnectionSupplier(() -> conn)
                .withDefaultPkName("id");

        //TODO simplify setup: convenience factory, defaults, ...
        mapper = new SqlMapper(engine,
                new BeanRegistryImpl(new SchemaRegistry(DatabaseDialect.H2),
                        new DefaultTableNameExtractor(),
                        new GuessingPkStrategyDecider(),
                        new ImmutableWithBuilderMetaDataExtractor()
                ));
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.prepareStatement("drop table person_permissions").executeUpdate();
        conn.prepareStatement("drop table person").executeUpdate();
    }

    @Test void testSnippetBuildingBlocks() {
        for (int i=0; i<1000; i++) {
            long personId = mapper.insert(Person.of(0L, String.format("%04d", i))).id();
            mapper.engine().update("insert into person_permissions(person_id, user_id) values(?,?)", personId, 1L).execute();
            if (i%10 == 0)
                mapper.engine().update("insert into person_permissions(person_id, user_id) values(?,?)", personId, 5L).execute();
        }

        final Snippets snippets = new Snippets();

        // we mix in pagination as an appended snippet
        final SqlSnippet page3Query = concat (
                sql("select * from person where name<? order by name", "zzz"),
                snippets.pagination(3, 20)
        );
        final List<Person> page3 = mapper.query(Person.class, page3Query).list();
        assertEquals(20, page3.size());
        for (int i=0; i<20; i++) {
            assertEquals(String.format("%04d", i+60), page3.get(i).name());
        }

        // pagination using the 'wrapping' method
        final SqlSnippet page3Query_2 = snippets.withPagination(3, 20, sql("select * from person where name<? order by name", "zzz"));
        final List<Person> page3_2 = mapper.query(Person.class, page3Query_2).list();
        assertEquals(page3, page3_2);

        // now we add permission filtering to the mix
        final SqlSnippet withPermissionQuery = concat (
                sql("select p.* from person p WHERE p.name<? AND", "zzz"),
                snippets.hasPersonPermission(sql("p.id"), 5L),
                sql("order by p.name"),
                snippets.pagination(1, 20)
        );
        final List<Person> filtered = mapper.query(Person.class, withPermissionQuery).list();
        assertEquals(20, filtered.size());
        for(int i=0; i<20; i++) {
            assertEquals(String.format("%04d", 10*(i+20)), filtered.get(i).name());
        }
    }
}


/**
 * This class has methods for different kinds of snippets that can be used as parts of other SqlSnippets.
 *
 * In actual application code, these should probably not be in the same class, but to illustrate the mechanics, they
 *  are grouped together here.
 */
class Snippets {
    /**
     * pagination as a snippet to append...
     */
    SqlSnippet pagination(int pageNo, int pageSize) {
        return sql("LIMIT ? OFFSET ?", pageSize, pageNo*pageSize);
    }

    /**
     * ... or as a method that wraps an existing snippet
     */
    SqlSnippet withPagination(int pageNo, int pageSize, SqlSnippet query) {
        return concat(query, pagination(pageNo, pageSize));
    }

    /**
     * returns a snippet with a condition checking if a given user may read a person. Note how
     *  the person is passed in as a SqlSnippet: This allows flexible use, e.g. passing in a
     *  subselect query.
     */
    SqlSnippet hasPersonPermission(SqlSnippet personId, long userId) {
        return concat(
                sql("EXISTS (SELECT * FROM person_permissions WHERE person_id="),
                personId,
                sql("AND user_id=?)", userId)
        );
    }
}
