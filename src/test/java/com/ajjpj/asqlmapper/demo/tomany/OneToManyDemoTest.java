package com.ajjpj.asqlmapper.demo.tomany;

import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.SqlEngine;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.demo.simple.Person;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistryImpl;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.ImmutableWithBuilderMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.GuessingPkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.DefaultTableNameExtractor;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedValues;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static com.ajjpj.asqlmapper.core.SqlSnippet.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OneToManyDemoTest extends AbstractDatabaseTest  {
    private SqlMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200))").executeUpdate();
        conn.prepareStatement("create table address(id bigserial primary key, person_id bigint references person, street varchar(200), city varchar(200))").executeUpdate();

        //TODO simplify setup: convenience factory, defaults, ...
        mapper = new SqlMapper(SqlEngine.create().withDefaultConnectionSupplier(() -> conn).withDefaultPkName("id"),
                new BeanRegistryImpl(new SchemaRegistry(DatabaseDialect.H2),
                        new DefaultTableNameExtractor(),
                        new GuessingPkStrategyDecider(),
                        new ImmutableWithBuilderMetaDataExtractor()
                ), ds);
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.prepareStatement("drop table address").executeUpdate();
        conn.prepareStatement("drop table person").executeUpdate();
    }

    @Test
    void testOneToMany() {
        final AList<Long> personIds = mapper
                .insertMany(AList.of(Person.of(0L, "Arno1"), Person.of(0L, "Arno2"), Person.of(0L, "Arno3")))
                .map(Person::id);

        final long personId1 = personIds.get(0);
        final long personId2 = personIds.get(1);
        final long personId3 = personIds.get(2);

        //TODO mapper.insertWithExplicitFields(conn, address, AMap.of("person_id", person.id()));
        //TODO mapper.insertMultiWithExplicitFields(conn, person.addresses(), AMap.of("person_id", person.id()));
        // -> ignore 'missing' primary key columns - check that all PK columns missing from bean are autoincrement(?)

        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId1, "street11", "city11").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId1, "street12", "city12").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId1, "street13", "city13").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId2, "street21", "city21").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId2, "street22", "city22").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId2, "street23", "city23").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId3, "street31", "city31").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId3, "street32", "city32").execute();
        mapper.engine().update("insert into address(person_id, street, city) values (?,?,?)", personId3, "street33", "city33").execute();

        //TODO subselect using master query instead of copy&paste
        final ProvidedValues addresses = mapper
                .queryForToManyAList(Address.class, "person_id", Long.class, sql("select * from address where person_id in (?,?) order by id desc", 1, 2))
                .execute();
        final AList<PersonWithAddresses> persons =  mapper
                .query(PersonWithAddresses.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                .withPropertyValues("addresses", addresses)
                .list();
        assertEquals(AList.of(
                PersonWithAddresses.of(personId1, "Arno1", AList.of(Address.of("street13", "city13"),Address.of("street12", "city12"),Address.of("street11", "city11"))),
                PersonWithAddresses.of(personId2, "Arno2", AList.of(Address.of("street23", "city23"),Address.of("street22", "city22"),Address.of("street21", "city21")))
        ), persons);
    }
}
