package com.ajjpj.asqlmapper.mapper.util;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.util.AThrowingRunnable;
import com.ajjpj.acollections.util.AUnchecker;

import java.lang.reflect.*;
import java.util.concurrent.Callable;

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
     *  for the purpose of getter and setter methods (which is what we are concerned with here). If it returns
     *  {@code false}, there is guaranteed to be no overriding, i.e. there are no false negatives.
     */
    private static boolean mightOverride(Method subMethod, Method superMethod) {
        if (! subMethod.getName().equals(superMethod.getName())) return false;
        if (subMethod.getParameterCount() != superMethod.getParameterCount()) return false;

        if (Modifier.isPrivate(subMethod.getModifiers()) || Modifier.isPrivate(superMethod.getModifiers())) {
            return false;
        }

        final boolean visible = Modifier.isPublic(superMethod.getModifiers()) ||
                Modifier.isProtected(superMethod.getModifiers()) ||
                // this is an approximation to JLS 'overriding' semantics but it should be good enough for handling annotations 'inheritance'
                subMethod.getDeclaringClass().getPackage() == superMethod.getDeclaringClass().getPackage();

        if (!visible) {
            return false;
        }

        for (int i=0; i<subMethod.getParameterCount(); i++) {
            if (subMethod.getParameterTypes()[i] != superMethod.getParameterTypes()[i]) return false;
        }
        return true;
    }

    /**
     * This method's purpose is to find all occurrances of a given method in a class' inheritance hierarchy (i.e.
     *  in interfaces, superclasses and interfaces implemented by superclasses) so we can evaluate annotations placed
     *  on any of these occurrances when dealing with a given method. This does <em>not</em> conform to Java's
     *  annotation semantics (which explicitly ignore inheritance), but it is the way the vast majority of libraries
     *  evaluate annotations, and more importantly it is what users have come to expect.<p>
     *
     * @param cls the class from whose perspective we look at the type hierarchy. This is not necessarily the class
     *            implementing the method we look at: The class can e.g. extend a superclass with the method
     *            implementation and an interface with the same method, but independently of each other.
     * @param mtd the method we are looking at
     * @return a set with all <em>public</em> methods in the class' hierarchy that the method overrides
     */
    public static ASet<Method> allSuperMethods(Class<?> cls, Method mtd) {
        return allSuperTypes(cls)
                .flatMap(c -> wrap(c.getDeclaredMethods()))
                .filter(m -> BeanReflectionHelper.mightOverride(mtd, m))
                .toSet();
    }

    /**
     * extracts and returns a parameterized (collection) type's single type parameter.
     * @param collType the collection type, which is expected to be a {@link ParameterizedType}
     * @return the collection's single generic parameter type (which is expected to not be parameterized)
     */
    public static Class<?> elementType(Type collType) {
        if (! (collType instanceof ParameterizedType))
            throw new IllegalArgumentException("collection type " + collType + " has no element type from which to extract mapping information");

        final Type elType = ((ParameterizedType) collType).getActualTypeArguments()[0];
        if(! (elType instanceof Class<?>))
            throw new IllegalArgumentException("element type " + elType + " is not a simple class - could not extract mapping information");

        return (Class<?>) elType;
    }

    public static <T> T unchecked(Callable<T> c) {
        try {
            return c.call();
        }
        catch(InvocationTargetException exc) {
            AUnchecker.throwUnchecked(exc.getTargetException());
        }
        catch(Throwable exc) {
            AUnchecker.throwUnchecked(exc);
        }
        return null; // for the compiler
    }
}
