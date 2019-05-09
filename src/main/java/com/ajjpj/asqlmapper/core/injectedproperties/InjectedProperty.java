package com.ajjpj.asqlmapper.core.injectedproperties;

import java.sql.Connection;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.SqlRow;


/**
 * An 'injected' property is a property with values that are not retrieved from a ResultSet directly. There is
 *  a wide range of other sources for their values:
 *
 * <ul>
 *     <li> a Map literal with the primary key (or some other, potentially non-unique column) as its key - values
 *           may come from some other database, from a REST call or whatever
 *     <li> calculate a values based on one or more database columns (and / or externally provided values) in ways
 *           that go beyond what can be expressed as an expression in a SELECT clause
 *     <li> a pre-fetched to-many (or to-one) relation
 *     <li> etc.
 * </ul>
 *
 * This interface is intentionally very generic in order to cover a lot of possible usage patterns. The common
 *  denominator is that a 'property' value is provided for each row of a query's result.<p>
 *
 * The concept of 'property' is distinct from that of a 'column'. Columns contain primitive values which are
 *  mapped by a PrimitiveTypeHandler, while 'properties' can have any Java type.
 *
 * @param <M> the memento's type
 */
public interface InjectedProperty<M> {
    String propertyName();
    M mementoPerQuery(Connection conn, Class<?> owningClass, SqlSnippet owningQuery);
    AOption<Object> value(Connection conn, SqlRow currentRow, M memento);
}
