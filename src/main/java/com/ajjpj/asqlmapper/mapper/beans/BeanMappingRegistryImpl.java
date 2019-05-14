package com.ajjpj.asqlmapper.mapper.beans;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.javabeans.BeanMetaData;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistry;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.annotations.ManyToMany;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.relations.*;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.ForeignKeySpec;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

public class BeanMappingRegistryImpl implements BeanMappingRegistry {
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataRegistry metaDataRegistry;

    private final OneToManyResolver oneToManyResolver = new DefaultOneToManyResolver(); //TODO make this configurable

    private final Map<Class<?>, BeanMapping> cache = new ConcurrentHashMap<>();
    private final Map<RelMapKey, OneToManySpec> oneToManyCache = new ConcurrentHashMap<>();
    private final Map<RelMapKey, ManyToManySpec> manyToManyCache = new ConcurrentHashMap<>();
    private final Map<RelMapKey, ToOneSpec> toOneCache = new ConcurrentHashMap<>();

    public BeanMappingRegistryImpl(SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider,
                                   BeanMetaDataRegistry metaDataRegistry) {
        this.schemaRegistry = schemaRegistry;
        this.tableNameExtractor = tableNameExtractor;
        this.pkStrategyDecider = pkStrategyDecider;
        this.metaDataRegistry = metaDataRegistry;
    }

    @Override
    public BeanMetaDataRegistry metaDataRegistry() {
        return metaDataRegistry;
    }

    public void clearCache() {
        cache.clear();
    }

    @Override
    public boolean canHandle(Class<?> cls) {
        return metaDataRegistry.canHandle(cls);
    }

    @Override
    public BeanMapping getBeanMapping(Connection conn, Class<?> beanType) {
        return cache.computeIfAbsent(beanType, bt ->
                executeUnchecked(() -> {
                    final BeanMetaData beanMetaData = metaDataRegistry.getBeanMetaData(beanType);

                    final String tableName = tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry);
                    final AOption<TableMetaData> optTableMetaData = schemaRegistry.getTableMetaData(conn, tableName);
                    if (optTableMetaData.isEmpty()) {
                        throw new IllegalArgumentException(beanType + " is associated with table " + tableName + " which does not exist");
                    }

                    final TableMetaData tableMetaData = optTableMetaData.get();

                    final PkStrategy pkStrategy = pkStrategyDecider.pkStrategy(conn, beanType, tableMetaData);

                    final AMap<BeanProperty, ColumnMetaData> mappedProperties = beanMetaData
                            .beanProperties()
                            .values()
                            .flatMap(prop ->
                                    tableMetaData.findColByName(prop.columnName())
                                            .map(col -> new AbstractMap.SimpleImmutableEntry<>(prop, col))
                            )
                            .toMap();

                    return new BeanMapping(
                            beanMetaData,
                            tableMetaData,
                            pkStrategy,
                            mappedProperties);
                })
        );
    }

    //TODO variant with explicitly passed in detail table / fk
    @Override
    public OneToManySpec resolveOneToMany(Connection conn, Class<?> ownerClass, String propertyName) {
        return oneToManyCache.computeIfAbsent(new RelMapKey(ownerClass, propertyName), k -> {
            final BeanMapping ownerMapping = getBeanMapping(conn, ownerClass);

            return oneToManyResolver.resolve(conn, ownerMapping, propertyName, tableNameExtractor, schemaRegistry);
        });
    }

    //TODO variant with passed-in manyManyTable and (optionally?) fks

    @Override
    public ManyToManySpec resolveManyToMany(Connection conn, Class<?> ownerClass, String propertyName) {
        return manyToManyCache.computeIfAbsent(new RelMapKey(ownerClass, propertyName), k -> {
            //TODO extract DefaultManyToManyResolver

            final BeanMapping ownerMapping = getBeanMapping(conn, ownerClass);
            final BeanProperty toManyProp = ownerMapping.beanMetaData().beanProperties().get(propertyName);
            if (toManyProp == null) {
                throw new IllegalArgumentException(ownerClass + " has no mapped property " + propertyName);
            }

            final Class<?> detailElementClass = BeanReflectionHelper.elementType(toManyProp.propType());

            final ManyToMany manyToMany = toManyProp
                    .getAnnotation(ManyToMany.class)
                    .orElseThrow(() -> new IllegalArgumentException("property " + propertyName + " of class " + ownerClass.getName() + " has no @ManyToMany"));

            final TableMetaData manyManySchema = schemaRegistry
                    .getTableMetaData(conn, manyToMany.manyManyTable())
                    .orElseThrow(() -> new IllegalArgumentException("table " + manyToMany.manyManyTable() + " does not exist"));

            final String fkToOwner = manyToMany.fkToMaster().isEmpty() ?
                    manyManySchema.uniqueFkTo(ownerMapping.tableName()).fkColumnName() :
                    manyToMany.fkToMaster();

            final String detailTableName;
            if (manyToMany.detailTable().isEmpty()) {
                detailTableName = tableNameExtractor.tableNameForBean(conn, detailElementClass, schemaRegistry);
            } else {
                detailTableName = manyToMany.detailTable();
            }

            final String fkToCollection;
            if (manyToMany.fkToDetail().isEmpty()) {
                fkToCollection = manyManySchema.uniqueFkTo(detailTableName).fkColumnName();
            } else {
                fkToCollection = manyToMany.fkToDetail();
            }

            final String detailPk;
            if (manyToMany.detailPk().isEmpty()) {
                detailPk = schemaRegistry
                        .getRequiredTableMetaData(conn, detailTableName)
                        .getUniquePkColumn()
                        .colName();
            } else {
                detailPk = manyToMany.detailPk();
            }

            final Class<?> keyType = ownerMapping.pkProperty().propClass();

            return new ManyToManySpec(manyToMany.manyManyTable(), fkToOwner, fkToCollection,
                    ownerMapping.pkProperty().columnName(), detailTableName, detailPk,
                    detailElementClass, CollectionBuildStrategy.get(toManyProp.propClass()),
                    keyType);
        });
    }

    @Override
    public ToOneSpec resolveToOne(Connection conn, Class owningClass, String propertyName) {
        return toOneCache.computeIfAbsent(new RelMapKey(owningClass, propertyName), k -> {
            //TODO extract to default strategy

            final BeanMapping ownerMapping = getBeanMapping(conn, owningClass);
            final BeanProperty refProp = ownerMapping.beanMetaData().beanProperties().get(propertyName); //TODO getRequiredProperty
            if (refProp == null) {
                throw new IllegalArgumentException(owningClass + " has no mapped property " + propertyName);
            }

            final String referencedTable = tableNameExtractor.tableNameForBean(conn, refProp.propClass(), schemaRegistry);
            final ForeignKeySpec foreignKeySpec = ownerMapping.tableMetaData().uniqueFkTo(referencedTable);

            final Class<?> keyType = schemaRegistry
                    .getRequiredTableMetaData(conn, foreignKeySpec.pkTableName())
                    .findColByName(foreignKeySpec.pkColumnName())
                    .get()
                    .colClass()
                    .get();

            return new ToOneSpec(foreignKeySpec, refProp.propClass(), keyType);
        });
    }
    private static class RelMapKey {
        final Class<?> ownerClass;
        final String propertyName;

        RelMapKey(Class<?> ownerClass, String propertyName) {
            this.ownerClass = ownerClass;
            this.propertyName = propertyName;
        }

        @Override
        public String toString() {
            return "FkMapKey{" +
                    "ownerClass=" + ownerClass +
                    ", propertyName='" + propertyName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RelMapKey fkMapKey = (RelMapKey) o;
            return Objects.equals(ownerClass, fkMapKey.ownerClass) &&
                    Objects.equals(propertyName, fkMapKey.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ownerClass, propertyName);
        }
    }
}
