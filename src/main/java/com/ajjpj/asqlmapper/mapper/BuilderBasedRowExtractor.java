package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.AMap;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;
import static com.ajjpj.acollections.util.AUnchecker.executeUncheckedVoid;


//TODO demo: package based 'canHandle' by subclassing

public class BuilderBasedRowExtractor implements RowExtractor {

    @Override public boolean canHandle (Class<?> cls) {
        try {
            return (cls.getMethod("builder").getModifiers() & Modifier.STATIC) != 0;
        }
        catch (Exception exc) {
            return false;
        }
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) {
        //TODO configurable naming conventions
        //TODO caching

        return executeUnchecked(() -> {
            final Object builder = cls.getMethod("builder").invoke(null);

            final AMap<String, Method> properties = AMap.fromMap(Arrays.stream(builder.getClass().getMethods())
                    .filter(m -> m.getReturnType() == builder.getClass())
                    .filter(m -> m.getParameterCount() == 1)
                    .filter(m -> primTypes.isPrimitiveType(m.getParameterTypes()[0]))
                    .collect(Collectors.toMap(m -> m.getName().toLowerCase(), m -> m)));

            for (int i=1; i<=rs.getMetaData().getColumnCount(); i++) {
                final int idx = i;
                final String colName = rs.getMetaData().getColumnName(i);
                properties.getOptional(colName.toLowerCase()).forEach(mtd ->
                        executeUncheckedVoid(() -> mtd.invoke(builder, primTypes.fromSql(mtd.getParameterTypes()[0], rs.getObject(idx))))
                );
            }

            final Method buildMtd = builder.getClass().getMethod("build");
            //noinspection unchecked
            return (T) buildMtd.invoke(builder);
        });
    }
}
