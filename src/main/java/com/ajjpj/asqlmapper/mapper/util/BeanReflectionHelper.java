package com.ajjpj.asqlmapper.mapper.util;

import com.ajjpj.acollections.ASet;

import java.lang.reflect.Method;

import static com.ajjpj.acollections.mutable.AMutableArrayWrapper.wrap;


public class BeanReflectionHelper {
    public static ASet<Class<?>> allSuperTypes(Class<?> cls) {
        if(cls == null) return ASet.empty();

        return allSuperTypes(cls.getSuperclass())
                .plusAll(wrap(cls.getInterfaces()).flatMap(BeanReflectionHelper::allSuperTypes))
                .plus(cls)
                ;
    }


    /**
     * This is not a strict implementation of Java's complex overriding semantics, but it is close enough
     *  for the purpose of getter and setter methods (which is what we are concerned here). If it returns
     *  {@code false}, there is guaranteed to be no overriding, i.e. there are no false negatives.
     */
    private static boolean mightOverride(Method subMethod, Method superMethod) {
        if (! subMethod.getName().equals(superMethod.getName())) return false;
        if (subMethod.getParameterCount() != superMethod.getParameterCount()) return false;

        if (! superMethod.getReturnType().isAssignableFrom(subMethod.getReturnType())) return false;
        for (int i=0; i<subMethod.getParameterCount(); i++) {
            if (!subMethod.getParameterTypes()[i].isAssignableFrom(superMethod.getParameterTypes()[i])) return false;
        }
        return true;
    }

    public static ASet<Method> allSuperMethods(Class<?> cls, Method mtd) {
        return allSuperTypes(cls)
                .flatMap(c -> wrap(c.getMethods()))
                .filter(m -> BeanReflectionHelper.mightOverride(mtd, m))
                .toSet();
    }
}
