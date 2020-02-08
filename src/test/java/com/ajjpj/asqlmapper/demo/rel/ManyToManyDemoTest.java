package com.ajjpj.asqlmapper.demo.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.ASet;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.SqlMapperBuilder;
import com.ajjpj.asqlmapper.core.SqlEngine;
import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedToManyProperty;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ManyToManyDemoTest extends AbstractDatabaseTest {
    private SqlMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        executeUpdate("create table person(id bigserial primary key, name varchar(200))");
        executeUpdate("create table address(id bigserial primary key, street varchar(200), city varchar(200))");
        executeUpdate(
                "create table person_address(person_id bigint references person, address_id bigint references address, primary key(person_id, address_id))");

        mapper = new SqlMapperBuilder()
                .withDefaultPkName("id")
                .withDefaultConnectionSupplier(() -> conn)
                .withBeanStyle(SqlMapperBuilder.BeanStyle.immutables)
                .build(DatabaseDialect.H2);
    }

    @AfterEach
    void tearDown() throws SQLException {
        executeUpdate("drop table person_address");
        executeUpdate("drop table person");
        executeUpdate("drop table address");
    }

    @Test
    public void testManyToMany() throws NoSuchMethodException {
        final long personId1 = mapper.engine().insertLongPk("insert into person(name) values (?)", "Arno1");
        final long personId2 = mapper.engine().insertLongPk("insert into person(name) values (?)", "Arno2");
        final long personId3 = mapper.engine().insertLongPk("insert into person(name) values (?)", "Arno3");

        final long addrId11 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street11", "city11");
        final long addrId12 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street12", "city12");
        final long addrId13 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street13", "city13");
        final long addrId21 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street21", "city21");
        final long addrId22 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street22", "city22");
        final long addrId23 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street23", "city23");
        final long addrId31 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street31", "city31");
        final long addrId32 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street32", "city32");
        final long addrId33 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street33", "city33");

        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId1, addrId11);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId1, addrId12);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId1, addrId13);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId2, addrId21);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId2, addrId22);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId2, addrId23);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId3, addrId31);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId3, addrId32);
        mapper.engine().executeUpdate("insert into person_address (person_id, address_id) values(?,?)", personId3, addrId33);

        final SqlEngine engine = mapper.engine();

        {
            final AList<PersonWithAddresses> persons = engine
                    .query(PersonWithAddresses.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withInjectedProperty(new InjectedToManyProperty<>("addresses", "id", Long.class, "person_id",
                            engine.query(Address.class,
                                    "select pa.person_id, a.* from address a inner join person_address pa on a.id=pa.address_id where pa.person_id in (?,?) order by id desc",
                                    1, 2),
                            CollectionBuildStrategy.forAVector()))
                    .list();

            assertEquals(AList.of(
                    PersonWithAddresses.of(personId1, "Arno1", AList.of(Address.of("street13", "city13"), Address.of("street12", "city12"), Address.of("street11", "city11"))),
                    PersonWithAddresses.of(personId2, "Arno2", AList.of(Address.of("street23", "city23"), Address.of("street22", "city22"), Address.of("street21", "city21")))
            ), persons);
        }

        {
            final AList<PersonWithAddressesManyToMany> persons = engine
                    .query(PersonWithAddressesManyToMany.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withInjectedProperty(mapper.manyToMany("addresses"))
                    .list();

            assertEquals(2, persons.size());
            assertEquals(ASet.of(Address.of("street13", "city13"), Address.of("street12", "city12"), Address.of("street11", "city11")), persons.get(0).addresses().toSet());
            assertEquals(ASet.of(Address.of("street23", "city23"), Address.of("street22", "city22"), Address.of("street21", "city21")), persons.get(1).addresses().toSet());
        }

        {
            final AList<PersonWithAddressesManyToMany> persons = mapper
                    .query(PersonWithAddressesManyToMany.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withManyToMany("addresses")
                    .list();

            assertEquals(2, persons.size());
            assertEquals(ASet.of(Address.of("street13", "city13"), Address.of("street12", "city12"), Address.of("street11", "city11")), persons.get(0).addresses().toSet());
            assertEquals(ASet.of(Address.of("street23", "city23"), Address.of("street22", "city22"), Address.of("street21", "city21")), persons.get(1).addresses().toSet());
        }
    }
}
