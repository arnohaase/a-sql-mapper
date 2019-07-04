package com.ajjpj.asqlmapper.core.injectedproperties;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.common.SqlStream;
import com.ajjpj.asqlmapper.core.impl.AQueryImpl;

public class InjectedToOneProperty<T> implements InjectedProperty<Map<Object,T>> {
    private final String propertyName;
    private final String masterKeyName;
    private final Class<?> keyType;
    private final String detailKeyName;
    private final AQuery<T> detailQuery;

    public InjectedToOneProperty(String propertyName, String masterKeyName, Class<?> keyType, String detailKeyName, AQuery<T> detailQuery) {
        this.propertyName = propertyName;
        this.masterKeyName = masterKeyName;
        this.keyType = keyType;
        this.detailKeyName = detailKeyName;
        this.detailQuery = detailQuery;
    }

    @Override public String propertyName () {
        return propertyName;
    }

    @Override public Map<Object,T> mementoPerQuery (Connection conn, Class<?> owningClass, SqlSnippet owningQuery) {
        final Map<Object,T> result = new HashMap<>();

        final SqlStream<T> stream = ((AQueryImpl<T>)detailQuery).streamWithRowAccess(conn);
        //TODO UndeclaredThrowableException?!
        stream.forEach(el -> {
            final Object key = stream.currentRow().get(keyType, detailKeyName);
            result.put(key, el);
            //TODO warn about duplicates?
        });

        return result;
    }

    @Override public AOption<Object> value (Connection conn, SqlRow currentRow, Map<Object,T> memento) {
        final Object curMasterKey = currentRow.get(keyType, masterKeyName);
        return memento.containsKey(curMasterKey) ? AOption.some(memento.get(curMasterKey)) : AOption.none();
    }
}
