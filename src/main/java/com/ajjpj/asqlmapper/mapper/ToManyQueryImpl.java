package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.impl.SqlHelper;
import com.ajjpj.asqlmapper.core.provided.ProvidedProperties;
import com.ajjpj.asqlmapper.core.provided.ProvidedValues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

//TODO demo / test: bean (with nested to-many), scalar
class ToManyQueryImpl<K,T,R> implements ToManyQuery<K,R> {
    private final RowExtractor beanExtractor;
    private final ProvidedProperties providedProperties;

    private final Class<K> keyType;
    private final String keyColumn;
    private final Class<T> manyType;
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final Collector<T,?,? extends R> collectorPerPk;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;

    ToManyQueryImpl (RowExtractor beanExtractor, ProvidedProperties providedProperties, Class<K> keyType,
                     String keyColumn, Class<T> manyType, SqlSnippet sql, PrimitiveTypeRegistry primTypes, Collector<T, ?, ? extends R> collectorPerPk,
                     AOption<Supplier<Connection>> defaultConnectionSupplier) {
        this.beanExtractor = beanExtractor;
        this.providedProperties = providedProperties;
        this.keyType = keyType;
        this.keyColumn = keyColumn;
        this.manyType = manyType;
        this.sql = sql;
        this.primTypes = primTypes;
        this.collectorPerPk = collectorPerPk;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
    }

    @Override public ProvidedValues execute () {
        return execute(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalArgumentException("no default connection supplier was configured"))
                .get());
    }
    @Override public ProvidedValues execute (Connection conn) {
        final Map<K, List<T>> raw = new HashMap<>();

        return executeUnchecked(() -> {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql());

            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                final ResultSet rs = ps.executeQuery();
                final Object memento = beanExtractor.mementoPerQuery(manyType, primTypes, rs, false);

                while(rs.next()) {
                    final K key = primTypes.fromSql(keyType, rs.getObject(keyColumn));
                    final T value = beanExtractor.fromSql(manyType, primTypes, rs, memento, false, providedProperties);
                    raw.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                }
            }
            finally {
                SqlHelper.closeQuietly(ps);
            }

            final Map<K,R> result = new HashMap<>();
            for(Map.Entry<K,List<T>> e: raw.entrySet()) {
                final R r = e.getValue().stream().collect(collectorPerPk);
                result.put(e.getKey(), r);
            }
            return ProvidedValues.of(keyType, result);
        });

    }

    @Override public ToManyQuery withPropertyValues (String propertyName, String referencedColumnName, ProvidedValues propertyValues) {
        return new ToManyQueryImpl<>(beanExtractor, providedProperties.with(propertyName, referencedColumnName, propertyValues), keyType, keyColumn, manyType, sql, primTypes, collectorPerPk, defaultConnectionSupplier);
    }

    @Override public ToManyQuery withPropertyValues (ProvidedProperties providedProperties) {
        if (this.providedProperties.nonEmpty()) throw new IllegalArgumentException("non-empty provided properties would be overwritten"); //TODO
        return new ToManyQueryImpl<>(beanExtractor, providedProperties, keyType, keyColumn, manyType, sql, primTypes, collectorPerPk, defaultConnectionSupplier);
    }
}
