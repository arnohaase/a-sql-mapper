package com.ajjpj.asqlmapper;


import java.sql.Connection;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeHandler;
import com.ajjpj.asqlmapper.core.SqlEngine;
import com.ajjpj.asqlmapper.core.listener.LoggingListener;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistry;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistryImpl;
import com.ajjpj.asqlmapper.javabeans.columnnames.ColumnNameExtractor;
import com.ajjpj.asqlmapper.javabeans.columnnames.DirectColumnNameExtractor;
import com.ajjpj.asqlmapper.javabeans.extractors.BeanMetaDataExtractor;
import com.ajjpj.asqlmapper.javabeans.extractors.ImmutableWithBuilderMetaDataExtractor;
import com.ajjpj.asqlmapper.javabeans.extractors.JavaBeansMetaDataExtractor;
import com.ajjpj.asqlmapper.javabeans.extractors.LombokWithBuilderMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import com.ajjpj.asqlmapper.mapper.SqlMapper;
import com.ajjpj.asqlmapper.mapper.beans.BeanMappingRegistryImpl;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.GuessingPkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.relations.*;
import com.ajjpj.asqlmapper.mapper.beans.tablename.DefaultTableNameExtractor;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;

/**
 * This is a builder class providing a simple way to configure a SqlMapper (and with it a SqlEngine)
 *  for most common use cases.<p>
 *
 *
 * It does <em>not</em> cover the full range of configuration options, but simplifies configuration and offers
 *  defaults. Many configuration options allow application code to provide their own implementations of an
 *  interface; this builder class often hides this, allowing a fixed set of common choices. So if you hit some
 *  limitation when working with this builder class, be sure to look at the full range of configuration options
 *  in {@link SqlMapper} and {@link SqlEngine}.<p>
 *
 * It builds a {@link SqlMapper}. If all you need is a {@link SqlEngine}, you can call {@link SqlMapper#engine()}
 *  on the result and discard the mapper.
 */
public class SqlMapperBuilder {

    public enum BeanStyle {
        javaBeans, immutables, lombok
    }

    private AOption<String> defaultPkName = AOption.empty();
    private boolean withLogging = true;
    private AOption<Supplier<Connection>> defaultConnectionSupplier = AOption.empty();

    private ColumnNameExtractor columnNameExtractor = new DirectColumnNameExtractor();
    private BeanStyle beanStyle = BeanStyle.javaBeans;
    private BeanMetaDataRegistry beanMetaDataRegistry = new BeanMetaDataRegistryImpl(new JavaBeansMetaDataExtractor(columnNameExtractor));

    private AVector<PrimitiveTypeHandler> primitiveTypeHandlers = AVector.empty();

    private OneToManyResolver oneToManyResolver = new DefaultOneToManyResolver();
    private ManyToManyResolver manyToManyResolver = new DefaultManyToManyResolver();
    private ToOneResolver toOneResolver = new DefaultToOneResolver();

    private TableNameExtractor tableNameExtractor = new DefaultTableNameExtractor();
    private PkStrategyDecider pkStrategyDecider = new GuessingPkStrategyDecider();

    public SqlMapperBuilder withDefaultPkName(String defaultPkName) {
        this.defaultPkName = AOption.of(defaultPkName);
        return this;
    }

    public SqlMapperBuilder disableLogging() {
        withLogging = false;
        return this;
    }

    public SqlMapperBuilder withDefaultConnectionSupplier(Supplier<Connection> defaultConnectionSupplier) {
        this.defaultConnectionSupplier = AOption.some(defaultConnectionSupplier);
        return this;
    }

    public SqlMapperBuilder withColumnNameExtractor(ColumnNameExtractor columnNameExtractor) {
        this.columnNameExtractor = columnNameExtractor;
        return withBeanStyle(this.beanStyle);
    }
    public SqlMapperBuilder withBeanStyle(BeanStyle beanStyle) {
        this.beanStyle = beanStyle;
        switch(beanStyle) {
            case javaBeans:
                beanMetaDataRegistry = new BeanMetaDataRegistryImpl(new JavaBeansMetaDataExtractor(columnNameExtractor));
                break;
            case immutables:
                beanMetaDataRegistry = new BeanMetaDataRegistryImpl(new ImmutableWithBuilderMetaDataExtractor(columnNameExtractor));
                break;
            case lombok:
                beanMetaDataRegistry = new BeanMetaDataRegistryImpl(new LombokWithBuilderMetaDataExtractor(columnNameExtractor));
                break;
            default:
                throw new IllegalArgumentException("unsupported bean style " + beanStyle + " - this is a bug");
        }
        return this;
    }
    public SqlMapperBuilder withMetaDataExtractor(BeanMetaDataExtractor metaDataExtractor) {
        beanMetaDataRegistry = new BeanMetaDataRegistryImpl(metaDataExtractor);
        return this;
    }

    public SqlMapperBuilder withPrimitiveTypeHandler(PrimitiveTypeHandler handler) {
        primitiveTypeHandlers.append(handler);
        return this;
    }

    public SqlMapperBuilder withTableNameExtractor(TableNameExtractor tableNameExtractor) {
        this.tableNameExtractor = tableNameExtractor;
        return this;
    }
    public SqlMapperBuilder withPkStrategyDecider(PkStrategyDecider pkStrategyDecider) {
        this.pkStrategyDecider = pkStrategyDecider;
        return this;
    }

    public SqlMapperBuilder withOneToManyResolver(OneToManyResolver resolver) {
        this.oneToManyResolver = resolver;
        return this;
    }
    public SqlMapperBuilder withManyToManyResolver(ManyToManyResolver resolver) {
        this.manyToManyResolver = resolver;
        return this;
    }
    public SqlMapperBuilder withToOneResolver(ToOneResolver resolver) {
        this.toOneResolver = resolver;
        return this;
    }

    private SqlEngine buildEngine() {
        SqlEngine result = SqlEngine.create();

        if(defaultPkName.isPresent())
            result = result.withDefaultPkName(defaultPkName.get());
        if(withLogging)
            result = result.withListener(LoggingListener.createWithStatistics(1000));
        if(defaultConnectionSupplier.isPresent())
            result = result.withDefaultConnectionSupplier(defaultConnectionSupplier.get());

        for(PrimitiveTypeHandler h: primitiveTypeHandlers)
            result = result.withPrimitiveHandler(h);

        return result;
    }

    public SqlMapper build(DatabaseDialect databaseDialect) {
        final SchemaRegistry schemaRegistry = new SchemaRegistry(databaseDialect);
        return new SqlMapper(buildEngine(), new BeanMappingRegistryImpl(
                schemaRegistry,
                tableNameExtractor,
                pkStrategyDecider,
                beanMetaDataRegistry,
                oneToManyResolver,
                manyToManyResolver,
                toOneResolver),
                schemaRegistry, tableNameExtractor);
    }
}
