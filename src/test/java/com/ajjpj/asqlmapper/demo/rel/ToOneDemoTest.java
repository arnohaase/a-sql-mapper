package com.ajjpj.asqlmapper.demo.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.List;

import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.SqlMapperBuilder;
import com.ajjpj.asqlmapper.core.SqlEngine;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedToOneProperty;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ToOneDemoTest extends AbstractDatabaseTest {
    private SqlMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        conn.prepareStatement("create table address(id bigserial primary key, street varchar(200), city varchar(200))").executeUpdate();
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200), address_id bigint references address)").executeUpdate();

        mapper = new SqlMapperBuilder()
                .withDefaultPkName("id")
                .withDefaultConnectionSupplier(() -> conn)
                .withBeanStyle(SqlMapperBuilder.BeanStyle.immutables)
                .build(DatabaseDialect.H2);
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.prepareStatement("drop table person").executeUpdate();
        conn.prepareStatement("drop table address").executeUpdate();
    }

    @Test
    void testToOne() {
        final List<Long> addressIds = mapper.engine()
                .insertLongPk("INSERT INTO address (street, city) VALUES (?,?),(?,?),(?,?)", "s1", "c1", "s2", "c2", "s3", "c3")
                .executeMulti();

        final long personId1 = mapper.engine()
                .insertLongPk("INSERT INTO person (name, address_id) VALUES (?,?)", "Arno1", addressIds.get(0))
                .executeSingle();
        final long personId2 = mapper.engine()
                .insertLongPk("INSERT INTO person (name, address_id) VALUES (?,?)", "Arno2", addressIds.get(1))
                .executeSingle();
        final long personId3 = mapper.engine()
                .insertLongPk("INSERT INTO person (name, address_id) VALUES (?,?)", "Arno3", addressIds.get(2))
                .executeSingle();

        //TODO annotation?

        final SqlEngine engine = mapper.engine();

        {
            final AList<PersonWithAddress> persons = engine
                    .query(PersonWithAddress.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withInjectedProperty(new InjectedToOneProperty<>("address", "address_id", Long.class, "id",
                            engine.query(Address.class, "select * from address where id in (?,?)", 1, 2)))
                    .list();

            assertEquals(AList.of(
                    PersonWithAddress.of(personId1, "Arno1", Address.of("s1", "c1")),
                    PersonWithAddress.of(personId2, "Arno2", Address.of("s2", "c2"))
            ), persons);
        }

        {
            final AList<PersonWithAddress> persons = engine
                    .query(PersonWithAddress.class, "select * from person where id in(?,?) order by id asc", 1, 2)
                    .withInjectedProperty(mapper.toOne("address")) //TODO fk name explicit / annotation, target table name
                    .list();

            assertEquals(AList.of(
                    PersonWithAddress.of(personId1, "Arno1", Address.of("s1", "c1")),
                    PersonWithAddress.of(personId2, "Arno2", Address.of("s2", "c2"))
            ), persons);
        }
    }
}
