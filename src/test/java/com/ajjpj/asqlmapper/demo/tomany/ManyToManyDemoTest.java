package com.ajjpj.asqlmapper.demo.tomany;

import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.SqlEngine;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
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

public class ManyToManyDemoTest extends AbstractDatabaseTest  {
    private SqlMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        conn.prepareStatement("create table person(id bigserial primary key, name varchar(200))").executeUpdate();
        conn.prepareStatement("create table address(id bigserial primary key, street varchar(200), city varchar(200))").executeUpdate();
        conn.prepareStatement("create table person_address(person_id bigint, address_id bigint, primary key(person_id, address_id))").executeUpdate();

        //TODO simplify setup: convenience factory, defaults, ...
        mapper = new SqlMapper(SqlEngine.create().withDefaultPkName("id").withDefaultConnectionSupplier(() -> conn),
                new BeanRegistryImpl(new SchemaRegistry(DatabaseDialect.H2),
                        new DefaultTableNameExtractor(),
                        new GuessingPkStrategyDecider(),
                        new ImmutableWithBuilderMetaDataExtractor()
                ));
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.prepareStatement("drop table person_address").executeUpdate();
        conn.prepareStatement("drop table person").executeUpdate();
        conn.prepareStatement("drop table address").executeUpdate();
    }

    @Test
    public void testOneToMany() throws SQLException {
        final long personId1 = mapper.engine().insertLongPk("insert into person(name) values (?)", "Arno1").executeSingle();
        final long personId2 = mapper.engine().insertLongPk("insert into person(name) values (?)", "Arno2").executeSingle();
        final long personId3 = mapper.engine().insertLongPk("insert into person(name) values (?)", "Arno3").executeSingle();

        final long addrId11 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street11", "city11").executeSingle();
        final long addrId12 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street12", "city12").executeSingle();
        final long addrId13 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street13", "city13").executeSingle();
        final long addrId21 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street21", "city21").executeSingle();
        final long addrId22 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street22", "city22").executeSingle();
        final long addrId23 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street23", "city23").executeSingle();
        final long addrId31 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street31", "city31").executeSingle();
        final long addrId32 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street32", "city32").executeSingle();
        final long addrId33 = mapper.engine().insertLongPk("insert into address(street, city) values (?,?)", "street33", "city33").executeSingle();

        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId1, addrId11).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId1, addrId12).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId1, addrId13).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId2, addrId21).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId2, addrId22).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId2, addrId23).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId3, addrId31).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId3, addrId32).execute();
        mapper.engine().update("insert into person_address (person_id, address_id) values(?,?)", personId3, addrId33).execute();

        final ProvidedValues addresses = mapper
                .queryForToManyAList(Address.class, "person_id", Long.class, sql("select pa.person_id, a.* from address a inner join person_address pa on a.id=pa.address_id where pa.person_id in (?,?) order by id desc", 1, 2))
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
