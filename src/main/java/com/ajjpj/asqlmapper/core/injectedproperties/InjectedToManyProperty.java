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


public class InjectedToManyProperty<T,C,B> implements InjectedProperty<Map<Object,C>> {
    private final String propertyName;
    private final String masterKeyName;
    private final Class<?> keyType;
    private final String detailKeyName;
    private final AQuery<T> detailQuery;

    private final CollectionBuildStrategy<T,B,C> collectionBuildStrategy;

    public InjectedToManyProperty (String propertyName, String masterKeyName, Class<?> keyType, String detailKeyName, AQuery<T> detailQuery,
                                   CollectionBuildStrategy<T,B,C> collectionBuildStrategy) {
        this.propertyName = propertyName;
        this.masterKeyName = masterKeyName;
        this.keyType = keyType;
        this.detailKeyName = detailKeyName;
        this.detailQuery = detailQuery;
        this.collectionBuildStrategy = collectionBuildStrategy;
    }

    @Override public String propertyName () {
        return propertyName;
    }

    @Override public Map<Object,C> mementoPerQuery (Connection conn, Class<?> owningClass, SqlSnippet owningQuery) {
        final Map<Object,B> resultRaw = new HashMap<>();

        final SqlStream<T> stream = ((AQueryImpl<T>)detailQuery).streamWithRowAccess(conn);
        stream.forEach(el -> {
            final Object key = stream.currentRow().get(keyType, detailKeyName);
            final B coll = resultRaw.computeIfAbsent(key, k -> collectionBuildStrategy.createBuilder());
            collectionBuildStrategy.addElement(coll, el);
        });

        final Map<Object,C> result;
        if(collectionBuildStrategy.requiresFinalization()) {
            result = new HashMap<>();
            for(Map.Entry<Object,B> e: resultRaw.entrySet()) {
                result.put(e.getKey(), collectionBuildStrategy.finalizeBuilder(e.getValue()));
            }
        }
        else {
            //noinspection unchecked
            result = (Map<Object, C>) resultRaw;
        }

        return result;
    }

    @Override public AOption<Object> value (Connection conn, SqlRow currentRow, Map<Object,C> memento) {
        final Object curMasterKey = currentRow.get(keyType, masterKeyName);
        return memento.containsKey(curMasterKey) ? AOption.some(memento.get(curMasterKey)) : AOption.none();
    }
}
