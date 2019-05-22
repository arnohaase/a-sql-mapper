package com.ajjpj.asqlmapper.mapper.injectedproperties;

import java.sql.Connection;
import java.util.Map;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;

/**
 * This is an injected property that is passed in as a {@link java.util.Map}.
 */
public class MapBasedInjectedProperty<K, T> implements InjectedProperty<Object> {
    private final String propertyName;
    private final Map<K,T> values;
    private final Class<K> keyType;
    private final String keyColumn;

    public MapBasedInjectedProperty(String propertyName, Map<K, T> values, Class<K> keyType, String keyColumn) {
        this.propertyName = propertyName;
        this.values = values;
        this.keyType = keyType;
        this.keyColumn = keyColumn;
    }
    @Override public String propertyName() {
        return propertyName;
    }
    @Override public Object mementoPerQuery(Connection conn, Class<?> owningClass, SqlSnippet owningQuery) {
        return null;
    }
    @Override public AOption<Object> value(Connection conn, SqlRow currentRow, Object memento) {
        return AOption.of(values.get(currentRow.get(keyType, keyColumn)));
    }
}
