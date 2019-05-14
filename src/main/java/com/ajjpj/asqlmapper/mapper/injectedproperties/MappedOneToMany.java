package com.ajjpj.asqlmapper.mapper.injectedproperties;

import static com.ajjpj.asqlmapper.core.SqlSnippet.concat;
import static com.ajjpj.asqlmapper.core.SqlSnippet.sql;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedToManyProperty;
import com.ajjpj.asqlmapper.mapper.beans.BeanMappingRegistry;
import com.ajjpj.asqlmapper.mapper.beans.relations.OneToManySpec;


@SuppressWarnings("unchecked")
public class MappedOneToMany implements InjectedProperty {
    private final String propertyName;
    private final BeanMappingRegistry beanMappingRegistry;
    private final BiFunction<Class<?>, SqlSnippet, AQuery<?>> queryFactory;

    private final Optional<OneToManySpec> spec;

    private InjectedToManyProperty inner;

    public MappedOneToMany(String propertyName, BeanMappingRegistry beanMappingRegistry, BiFunction<Class<?>, SqlSnippet, AQuery<?>> queryFactory,
                           Optional<OneToManySpec> spec) {
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
        final OneToManySpec rel = spec.orElse(beanMappingRegistry.resolveOneToMany(conn, owningClass, propertyName));

        final SqlSnippet detailSql = concat(
                sql("SELECT * FROM " + rel.foreignKeySpec().fkTableName() + " WHERE " + rel.foreignKeySpec().fkColumnName() + " IN (SELECT " + rel.foreignKeySpec().pkColumnName() + " FROM ("),
                owningQuery,
                sql(") X)")
        );
        final AQuery<?> detailQuery = queryFactory.apply(rel.elementClass(), detailSql);

        inner = new InjectedToManyProperty(propertyName, rel.foreignKeySpec().pkColumnName(), rel.keyType(), rel.foreignKeySpec().fkColumnName(), detailQuery, rel.collectionBuildStrategy());
        return inner.mementoPerQuery(conn, owningClass, owningQuery);
    }

    @Override public AOption<Object> value (Connection conn, SqlRow currentRow, Object memento) {
        return inner.value(conn, currentRow, (Map) memento);
    }
}
