package com.ajjpj.asqlmapper.core.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AHashSet;
import com.ajjpj.acollections.immutable.ALinkedList;
import com.ajjpj.acollections.immutable.AVector;

/**
 * This interface provides an abstraction over building and populating any collection. It is pretty generic to
 *  cover the range from mutable java.util collection to immutable builder-based collections
 */
public interface CollectionBuildStrategy<T,B,C> {
    B createBuilder();
    void addElement(B builder, T el);
    boolean requiresFinalization();
    C finalizeBuilder(B builder);

    static <T,C> CollectionBuildStrategy<T,?,C> get(Class<C> collectionClass) {
        final CollectionBuildStrategy result = Registry.registry.get(collectionClass);
        if(result == null)
            throw new IllegalArgumentException("no collection build strategy is registered for collection class " + collectionClass.getName() +
                    " - you can call CollectionBuildStrategy.register() to register it");
        //noinspection unchecked
        return result;
    }

    static void register(Class<?> collectionClass, CollectionBuildStrategy strategy) {
        Registry.registry.put(collectionClass, strategy);
    }

    static <T> JavaHashSetStrategy<T> forJavaUtilSet() {
        //noinspection unchecked
        return (JavaHashSetStrategy<T>) JavaHashSetStrategy.INSTANCE;
    }

    static <T> JavaArrayListStrategy<T> forJavaUtilList() {
        //noinspection unchecked
        return (JavaArrayListStrategy<T>) JavaArrayListStrategy.INSTANCE;
    }

    static <T> AVectorStrategy<T> forAVector() {
        //noinspection unchecked
        return (AVectorStrategy<T>) AVectorStrategy.INSTANCE;
    }

    static <T> ALinkedListStrategy<T> forALinkedList() {
        //noinspection unchecked
        return (ALinkedListStrategy<T>) ALinkedListStrategy.INSTANCE;
    }

    static <T> AHashSetStrategy<T> forAHashSet() {
        //noinspection unchecked
        return (AHashSetStrategy<T>) AHashSetStrategy.INSTANCE;
    }

    class JavaHashSetStrategy<T> implements CollectionBuildStrategy<T,HashSet<T>,HashSet<T>> {
        private static JavaHashSetStrategy<Object> INSTANCE = new JavaHashSetStrategy<>();

        @Override public HashSet<T> createBuilder () {
            return new HashSet<>();
        }

        @Override public void addElement (HashSet<T> builder, T el) {
            builder.add(el);
        }

        @Override public boolean requiresFinalization () {
            return false;
        }

        @Override public HashSet<T> finalizeBuilder (HashSet<T> builder) {
            return builder;
        }
    }

    class JavaArrayListStrategy<T> implements CollectionBuildStrategy<T,ArrayList<T>, ArrayList<T>> {
        private static final JavaArrayListStrategy<Object> INSTANCE = new JavaArrayListStrategy<>();

        @Override public ArrayList<T> createBuilder () {
            return new ArrayList<>();
        }

        @Override public void addElement (ArrayList<T> builder, T el) {
            builder.add(el);
        }

        @Override public boolean requiresFinalization () {
            return false;
        }

        @Override public ArrayList<T> finalizeBuilder (ArrayList<T> builder) {
            return builder;
        }
    }

    class AVectorStrategy<T> implements CollectionBuildStrategy<T, AVector.Builder<T>, AVector<T>> {
        private static final AVectorStrategy<Object> INSTANCE = new AVectorStrategy<>();

        @Override public AVector.Builder<T> createBuilder () {
            return AVector.builder();
        }

        @Override public void addElement (AVector.Builder<T> builder, T el) {
            builder.add(el);
        }

        @Override public boolean requiresFinalization () {
            return true;
        }

        @Override public AVector<T> finalizeBuilder (AVector.Builder<T> builder) {
            return builder.build();
        }
    }

    class ALinkedListStrategy<T> implements CollectionBuildStrategy<T, ALinkedList.Builder<T>, ALinkedList<T>> {
        private static final ALinkedListStrategy<Object> INSTANCE = new ALinkedListStrategy<>();

        @Override public ALinkedList.Builder<T> createBuilder () {
            return ALinkedList.builder();
        }

        @Override public void addElement (ALinkedList.Builder<T> builder, T el) {
            builder.add(el);
        }

        @Override public boolean requiresFinalization () {
            return true;
        }

        @Override public ALinkedList<T> finalizeBuilder (ALinkedList.Builder<T> builder) {
            return builder.build();
        }
    }

    class AHashSetStrategy<T> implements CollectionBuildStrategy<T, AHashSet.Builder<T>, AHashSet<T>> {
        private static final AHashSetStrategy<Object> INSTANCE = new AHashSetStrategy<>();

        @Override public AHashSet.Builder<T> createBuilder () {
            return AHashSet.builder();
        }

        @Override public void addElement (AHashSet.Builder<T> builder, T el) {
            builder.add(el);
        }

        @Override public boolean requiresFinalization () {
            return true;
        }

        @Override public AHashSet<T> finalizeBuilder (AHashSet.Builder<T> builder) {
            return builder.build();
        }
    }

    class Registry {
        private static final Map<Class<?>, CollectionBuildStrategy> registry = new ConcurrentHashMap<>();
        static {
            registry.put(java.lang.Iterable.class, forJavaUtilList());
            registry.put(java.util.Collection.class, forJavaUtilList());
            registry.put(java.util.List.class, forJavaUtilList());
            registry.put(java.util.ArrayList.class, forJavaUtilList());

            registry.put(java.util.Set.class, forJavaUtilSet());
            registry.put(java.util.HashSet.class, forJavaUtilSet());

            registry.put(AList.class, forAVector());
            registry.put(AVector.class, forAVector());
            registry.put(ALinkedList.class, forALinkedList());

            registry.put(ASet.class, forAHashSet());
            registry.put(AHashSet.class, forAHashSet());
        }
    }
}
