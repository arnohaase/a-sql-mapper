package com.ajjpj.asqlmapper.mapper.injectedproperties;

import static com.ajjpj.asqlmapper.core.SqlSnippet.*;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedToOneProperty;
import com.ajjpj.asqlmapper.mapper.beans.BeanMappingRegistry;
import com.ajjpj.asqlmapper.mapper.beans.relations.ToOneSpec;

@SuppressWarnings("unchecked")
public class MappedToOne implements InjectedProperty {
    private final String propertyName;
    private final BeanMappingRegistry beanMappingRegistry;
    private final BiFunction<Class<?>, SqlSnippet, AQuery<?>> queryFactory;

    private final Optional<ToOneSpec> spec;

    private InjectedToOneProperty inner;

    public MappedToOne(String propertyName, BeanMappingRegistry beanMappingRegistry, BiFunction<Class<?>, SqlSnippet, AQuery<?>> queryFactory,
                       Optional<ToOneSpec> spec) {
        this.propertyName = propertyName;
        this.beanMappingRegistry = beanMappingRegistry;
        this.queryFactory = queryFactory;
        this.spec = spec;
    }

    @Override public String propertyName () {
        return propertyName;
    }

    @Override
    public Object mementoPerQuery (Connection conn, Class owningClass, SqlSnippet owningQuery) {
        final ToOneSpec rel = spec.orElseGet(() -> beanMappingRegistry.resolveToOne(conn, owningClass, propertyName));

        //TODO ensure (in the mapper?) that the 'master' foreign key is part of the owning query - back propagation?
        final SqlSnippet detailSql = concat(
                sql("SELECT * FROM " + rel.foreignKeySpec().pkTableName() + " WHERE " + rel.foreignKeySpec().pkColumnName() + " IN (SELECT " + rel.foreignKeySpec().fkColumnName() + " FROM ("),
                owningQuery,
                sql(") X)")
        );
        final AQuery<?> detailQuery = queryFactory.apply(rel.elementClass(), detailSql);

        inner = new InjectedToOneProperty(propertyName, rel.foreignKeySpec().fkColumnName(), rel.keyType(), rel.foreignKeySpec().pkColumnName(), detailQuery);
        return inner.mementoPerQuery(conn, owningClass, owningQuery);
    }

    @Override public AOption<Object> value (Connection conn, SqlRow currentRow, Object memento) {
        return inner.value(conn, currentRow, (Map) memento);
    }
}
