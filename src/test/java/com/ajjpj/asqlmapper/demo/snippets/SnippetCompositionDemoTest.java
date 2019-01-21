package com.ajjpj.asqlmapper.demo.snippets;

import com.ajjpj.asqlmapper.ASqlEngine;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.mapper.BuilderBasedRowExtractor;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistryImpl;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.ImmutableWithBuilderMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.GuessingPkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.DefaultTableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
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
        ASqlEngine engine = ASqlEngine
                .create()
                .withDefaultPkName("id");

        //TODO simplify setup: convenience factory, defaults, ...
        mapper = new SqlMapper(ASqlEngine.create().withDefaultPkName("id"),
                new BeanRegistryImpl(new SchemaRegistry(DatabaseDialect.H2),
                        new DefaultTableNameExtractor(),
                        new GuessingPkStrategyDecider(),
                        new ImmutableWithBuilderMetaDataExtractor()
                ), ds);
    }

    @Test void testSnippetBuildingBlocks() throws Exception {
        for (int i=0; i<1000; i++) {
            long personId = mapper.insert(conn, Person.of(0L, String.format("%04d", i))).id();
            mapper.engine().update("insert into person_permissions(person_id, user_id) values(?,?)", personId, 1L).execute(conn);
            if (i%10 == 0)
                mapper.engine().update("insert into person_permissions(person_id, user_id) values(?,?)", personId, 5L).execute(conn);
        }

        final Snippets snippets = new Snippets();

        // we mix in pagination as a snippet
        final SqlSnippet page3Query = concat (
                sql("select * from person where name<? order by name", "zzz"),
                snippets.pagination(3, 20)
        );
        final List<Person> page3 = mapper.query(Person.class, page3Query).list(conn);
        assertEquals(20, page3.size());
        for (int i=0; i<20; i++) {
            assertEquals(String.format("%04d", i+60), page3.get(i).name());
        }

        // now we add permission filtering to the mix
        final SqlSnippet withPermissionQuery = concat (
                sql("select p.* from person p WHERE p.name<? AND", "zzz"),
                snippets.hasPersonPermission(sql("p.id"), 5L),
                sql("order by p.name"),
                snippets.pagination(1, 20)
        );
        final List<Person> filtered = mapper.query(Person.class, withPermissionQuery).list(conn);
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
    SqlSnippet pagination(int pageNo, int pageSize) {
        return sql("LIMIT ? OFFSET ?", pageSize, pageNo*pageSize);
    }

    SqlSnippet hasPersonPermission(SqlSnippet personId, long userId) {
        return concat(
                sql("EXISTS (SELECT * FROM person_permissions WHERE person_id="),
                personId,
                sql("AND user_id=?)", userId)
        );
    }
}
