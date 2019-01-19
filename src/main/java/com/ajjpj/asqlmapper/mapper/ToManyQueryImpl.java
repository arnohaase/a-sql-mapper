package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.AMap;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.impl.SqlHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;


//TODO demo / test: bean (with nested to-many), scalar
class ToManyQueryImpl<K,T,R> implements ToManyQuery<K,R> {
    private final RowExtractor beanExtractor;
    private final AMap<String, Map<?,?>> providedValues;

    private final Class<K> keyType;
    private final String keyColumn;
    private final Class<T> manyType;
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final Collector<T,?,? extends R> collectorPerPk;

    ToManyQueryImpl (RowExtractor beanExtractor, AMap<String, Map<?,?>> providedValues, Class<K> keyType,
                     String keyColumn, Class<T> manyType, SqlSnippet sql, PrimitiveTypeRegistry primTypes, Collector<T, ?, ? extends R> collectorPerPk) {
        this.beanExtractor = beanExtractor;
        this.providedValues = providedValues;
        this.keyType = keyType;
        this.keyColumn = keyColumn;
        this.manyType = manyType;
        this.sql = sql;
        this.primTypes = primTypes;
        this.collectorPerPk = collectorPerPk;

        if (providedValues.nonEmpty() && ! (beanExtractor instanceof BeanRegistryBasedRowExtractor)) {
            throw new IllegalArgumentException("provided values are only supported for bean mappings");
        }
    }

    @Override public Map<K,R> execute (Connection conn) throws SQLException {
        final Map<K, List<T>> raw = new HashMap<>();

        try (final PreparedStatement ps = conn.prepareStatement(sql.getSql())) {
            SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
            final ResultSet rs = ps.executeQuery();
            final Object memento = beanExtractor.mementoPerQuery(manyType, primTypes, rs);

            while(rs.next()) {
                final K key = primTypes.fromSql(keyType, rs.getObject(keyColumn));
                final T value;
                if (beanExtractor instanceof BeanRegistryBasedRowExtractor) {
                    value = ((BeanRegistryBasedRowExtractor) beanExtractor).fromSql(manyType, primTypes, rs, memento, providedValues);
                }
                else {
                    value = beanExtractor.fromSql(manyType, primTypes, rs, memento);
                }
                raw.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
            }
        }

        final Map<K,R> result = new HashMap<>();
        for(Map.Entry<K,List<T>> e: raw.entrySet()) {
            final R r = e.getValue().stream().collect(collectorPerPk);
            result.put(e.getKey(), r);
        }
        return result;
    }

    @Override public ToManyQuery<K,R> withPropertyValues(String propName, Map<Object,Object> providedValues) {
        return new ToManyQueryImpl<>(beanExtractor, this.providedValues.plus(propName, providedValues), keyType, keyColumn, manyType, sql, primTypes, collectorPerPk);
    }
}
