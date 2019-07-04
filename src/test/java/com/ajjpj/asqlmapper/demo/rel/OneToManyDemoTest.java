package com.ajjpj.asqlmapper.demo.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.ASet;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.SqlMapperBuilder;
import com.ajjpj.asqlmapper.core.SqlEngine;
import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedToManyProperty;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;

public class OneToManyDemoTest extends AbstractDatabaseTest  {
    private SqlMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200))").executeUpdate();
        conn.prepareStatement("create table address(id bigserial primary key, person_id bigint references person, street varchar(200), city varchar(200))").executeUpdate();

        mapper = new SqlMapperBuilder()
                .withDefaultPkName("id")
                .withDefaultConnectionSupplier(() -> conn)
                .withBeanStyle(SqlMapperBuilder.BeanStyle.immutables)
                .build(DatabaseDialect.H2);
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

        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId1, "street11", "city11");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId1, "street12", "city12");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId1, "street13", "city13");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId2, "street21", "city21");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId2, "street22", "city22");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId2, "street23", "city23");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId3, "street31", "city31");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId3, "street32", "city32");
        mapper.engine().executeUpdate("insert into address(person_id, street, city) values (?,?,?)", personId3, "street33", "city33");

        //TODO injected properties in SqlRow (?)

        final SqlEngine engine = mapper.engine();

        {
            final AList<PersonWithAddresses> persons = engine
                    .query(PersonWithAddresses.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withInjectedProperty(new InjectedToManyProperty<>("addresses", "id", Long.class, "person_id",
                            engine.query(Address.class, "select * from address where person_id in (?,?) order by id desc", 1, 2),
                            CollectionBuildStrategy.forAVector()))
                    .list();

            assertEquals(AList.of(
                    PersonWithAddresses.of(personId1, "Arno1", AList.of(Address.of("street13", "city13"), Address.of("street12", "city12"), Address.of("street11", "city11"))),
                    PersonWithAddresses.of(personId2, "Arno2", AList.of(Address.of("street23", "city23"), Address.of("street22", "city22"), Address.of("street21", "city21")))
            ), persons);
        }

        {
            final AList<PersonWithAddresses> persons = engine
                    .query(PersonWithAddresses.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withInjectedProperty(mapper.oneToMany("addresses"))
                    .list();

            assertEquals(2, persons.size());
            assertEquals(ASet.of(Address.of("street13", "city13"), Address.of("street12", "city12"), Address.of("street11", "city11")), persons.get(0).addresses().toSet());
            assertEquals(ASet.of(Address.of("street23", "city23"), Address.of("street22", "city22"), Address.of("street21", "city21")), persons.get(1).addresses().toSet());
        }

        {
            final AList<PersonWithAddresses> persons = mapper
                    .query(PersonWithAddresses.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withOneToMany("addresses")
                    .list();

            assertEquals(2, persons.size());
            assertEquals(ASet.of(Address.of("street13", "city13"), Address.of("street12", "city12"), Address.of("street11", "city11")), persons.get(0).addresses().toSet());
            assertEquals(ASet.of(Address.of("street23", "city23"), Address.of("street22", "city22"), Address.of("street21", "city21")), persons.get(1).addresses().toSet());
        }
    }
}
