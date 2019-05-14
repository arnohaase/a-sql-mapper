package com.ajjpj.asqlmapper.mapper.injectedproperties;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;
import static com.ajjpj.asqlmapper.core.SqlSnippet.concat;
import static com.ajjpj.asqlmapper.core.SqlSnippet.sql;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedToManyProperty;
import com.ajjpj.asqlmapper.mapper.beans.BeanMapping;
import com.ajjpj.asqlmapper.mapper.beans.BeanMappingRegistry;
import com.ajjpj.asqlmapper.mapper.beans.relations.ManyToManySpec;
import com.ajjpj.asqlmapper.mapper.beans.relations.OneToManySpec;

@SuppressWarnings("unchecked")
public class MappedManyToMany implements InjectedProperty {
    private final String propertyName;
    private final BeanMappingRegistry beanMappingRegistry;
    private final BiFunction<Class<?>, SqlSnippet, AQuery<?>> queryFactory;

    private final Optional<ManyToManySpec> spec;

    private InjectedToManyProperty inner;

    public MappedManyToMany(String propertyName, BeanMappingRegistry beanMappingRegistry, BiFunction<Class<?>, SqlSnippet, AQuery<?>> queryFactory,
                            Optional<ManyToManySpec> spec) {
        this.propertyName = propertyName;
        this.beanMappingRegistry = beanMappingRegistry;
        this.queryFactory = queryFactory;
        this.spec = spec;
    }

    @Override public String propertyName() {
        return propertyName;
    }

    @Override
    public Object mementoPerQuery(Connection conn, Class owningClass, SqlSnippet owningQuery) {
        final ManyToManySpec rel = spec.orElseGet(() -> beanMappingRegistry.resolveManyToMany(conn, owningClass, propertyName));

        final String fkToOwnerAlias = "$$" + rel.fkToOwner();

        final SqlSnippet detailSql = concat(
                sql("SELECT b." + rel.fkToOwner() + " AS \"" + fkToOwnerAlias + "\", a.*"),
                sql("FROM " + rel.collTable() + " a INNER JOIN " + rel.manyManyTable() + " b ON a." + rel.collPk() + "=b." + rel.fkToCollection()),
                sql("WHERE b." + rel.fkToOwner() + " IN (SELECT " + rel.ownerPk() + " FROM ("),
                owningQuery,
                sql(") x)")
        );
        final AQuery<?> detailQuery = queryFactory.apply(rel.elementClass(), detailSql);

        inner = new InjectedToManyProperty(propertyName, rel.ownerPk(), rel.keyType(), fkToOwnerAlias, detailQuery, rel.collectionBuildStrategy());
        return inner.mementoPerQuery(conn, owningClass, owningQuery);
    }

    @Override public AOption<Object> value(Connection conn, SqlRow currentRow, Object memento) {
        return inner.value(conn, currentRow, (Map) memento);
    }
}
