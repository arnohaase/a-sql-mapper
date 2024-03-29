package com.ajjpj.asqlmapper.core;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.ajjpj.acollections.AIterator;
import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.mutable.AMutableArrayWrapper;

/**
 * A SqlSnippet is some SQL code with its corresponding parameters. This is the fundamental building block for
 *  working with SQL, with static convenience methods for commonly used building blocks. It is designed for readable
 *  source code ("internal DSL") by doing a static import of its methods.<p>
 *
 * SqlSnippets are <em>immutable</em>, i.e. instances can not be modified and are automatically safe to share across
 *  threads. All operations leave existing SqlSnippets "as is", creating new instances of SqlSnippet.<p>
 *
 * A SqlSnippet need not be valid SQL. Quite the opposite actually: SqlSnippets can be used to build SQL from small,
 *  independent part that may not be recognizable as SQL on their own.
 *
 * TODO SqlSnippets can be used for reusable building blocks / extracting cross-cutting and orthogonal functionality, e.g. permission checks, optimistic locking, tenant separation, ...
 */
public class SqlSnippet {
    /**
     * This is an empty snippet, i.e. a snippet with an empty SQL string and no parameters
     */
    public static SqlSnippet EMPTY = new SqlSnippet("", AVector.empty());

    /**
     * A database independent representation of 'true'
     */
    public static final SqlSnippet TRUE = sql("1<>2");

    /**
     * A database independent representation of 'false'
     */
    public static final SqlSnippet FALSE = sql("1<>1");

    private static final SqlSnippet AND = sql("AND");
    private static final SqlSnippet CLOSE = sql(")");
    private static final SqlSnippet COMMA = sql(",");
    private static final SqlSnippet IN_OPEN = sql("IN (");
    private static final SqlSnippet OPEN = sql("(");
    private static final SqlSnippet OR = sql("OR");
    private static final SqlSnippet WHERE = sql("WHERE");

    private final String sqlString;
    private final AVector<?> params;

    private SqlSnippet (String sqlString, AVector<?> params) {
        this.sqlString = sqlString;
        this.params = params;
    }

    /**
     * @return the SQL (sub)string
     */
    public String getSql () {
        return sqlString;
    }

    /**
     * @return the parameters
     */
    public AList<?> getParams () {
        return params;
    }

    /**
     * Creates a SqlSnippet from a given SQL (sub)string and parameters
     *
     * TODO example
     */
    public static SqlSnippet sql(String sql, Object... params) {
        return new SqlSnippet(sql, AVector.from(params));
    }
    /**
     * Creates a SqlSnippet from a given SQL (sub)string and parameters
     *
     * TODO example
     */
    public static SqlSnippet sql(String sql, List<?> params) {
        return new SqlSnippet(sql, AVector.from(params));
    }

    /**
     * Returns a new SqlSnippet by concatenating existing SQLSnippets, adding a blank between snippets to prevent
     *  accidental "fusing" of consecutive snippets ("SELECT * FROM a" + "WHERE id=?" -> "SELECT * FROM aWHERE id=?").<p>
     *
     * This variant of <em>concat</em> returns a new SqlSnippet by concatenating existing SQLSnippets, accepting
     * snippets as a varargs parameter for human-readable code.
     *
     * TODO example
     */
    public static SqlSnippet concat(SqlSnippet s0, SqlSnippet... snippets) {
        return concat(AIterator.single(s0).concat(AMutableArrayWrapper.from(snippets).iterator()));
    }
    /**
     * Returns a new SqlSnippet by concatenating existing SQLSnippets, adding a blank between snippets to prevent
     *  accidental "fusing" of consecutive snippets ("SELECT * FROM a" + "WHERE id=?" -> "SELECT * FROM aWHERE id=?").<p>
     *
     * This variant of <em>concat</em> accepts a collection of snippets to facilitate algorithmic use where client
     *  code first collects snippets in a collection and then concatenates them.
     *
     * TODO example
     */
    public static SqlSnippet concat(Iterable<SqlSnippet> snippets) {
        return concat(snippets.iterator());
    }
    /**
     * Returns a new SqlSnippet by concatenating existing SQLSnippets, adding a blank between snippets to prevent
     *  accidental "fusing" of consecutive snippets ("SELECT * FROM a" + "WHERE id=?" -> "SELECT * FROM aWHERE id=?").<p>
     *
     * TODO example
     */
    public static SqlSnippet concat(Iterator<SqlSnippet> snippets) {
        return builder().appendAll(snippets).build();
    }

    public static SqlBuilder builder() {
        return new SqlBuilder();
    }

    /**
     * creates a snippet for a value as a parameter
     *
     * TODO example
     */
    public static SqlSnippet param(Object value) {
        return sql("?", value);
    }

    /**
     * creates a snippet with a comma separated list of parameters
     *
     * TODO example
     */
    public static SqlSnippet params(Iterable<?> values) {
        return params(values.iterator());
    }

    /**
     * creates a snippet with a comma separated list of parameters
     *
     * TODO example
     */
    public static SqlSnippet params(Iterator<?> it) {
        return commaSeparated (AIterator.wrap(it).map(SqlSnippet::param));
    }

    public static SqlSnippet commaSeparated (Iterable<SqlSnippet> coll) {
        return commaSeparated(coll.iterator());
    }
    public static SqlSnippet commaSeparated (Iterator<SqlSnippet> it) {
        return combineNoBlank(it, EMPTY, COMMA, EMPTY);
    }

    /**
     * combines a list of snippets into a single new snippet, with a separator snippet inserted in between. Whitespace
     *  is added between snippets automatically to prevent accidental "fusing" of subsequent tokens.
     *
     * TODO example
     */
    public static SqlSnippet combine(Iterable<SqlSnippet> elements, SqlSnippet separator) {
        return combine(elements, EMPTY, separator, EMPTY);
    }

    /**
     * combines a list of snippets into a single new snippet, with a separator snippet inserted in between and prefix
     *  and suffix snippets prepended and appended, respectively. Whitespace is added between snippets automatically
     *  to prevent accidental "fusing" of subsequent tokens.
     *
     * TODO example
     */
    public static SqlSnippet combine(Iterable<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        return combine(elements.iterator(), prefix, separator, suffix);
    }

    /**
     * combines a sequence of snippets into a single new snippet, with a separator snippet inserted in between. Whitespace
     *  is added between snippets automatically to prevent accidental "fusing" of subsequent tokens.
     *
     * TODO example
     */
    public static SqlSnippet combine(Iterator<SqlSnippet> elements, SqlSnippet separator) {
        return combine(elements, EMPTY, separator, EMPTY);
    }

    /**
     * combines a sequence of snippets into a single new snippet, with a separator snippet inserted in between and prefix
     *  and suffix snippets prepended and appended, respectively. Whitespace is added between snippets automatically
     *  to prevent accidental "fusing" of subsequent tokens.
     *
     * TODO example
     */
    public static SqlSnippet combine(Iterator<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        final SqlBuilder result = builder();
        result.append(prefix);
        boolean first = true;
        while (elements.hasNext()) {
            if (first) {
                first = false;
            }
            else {
                result.append(separator);
            }
            result.append(elements.next());
        }
        result.append(suffix);
        return result.build();
    }
    private static SqlSnippet combine(SqlSnippet el0, Iterator<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        final SqlBuilder result = builder();
        result.append(prefix);

        result.append(el0);
        while (elements.hasNext()) {
            result.append(separator);
            result.append(elements.next());
        }

        result.append(suffix);
        return result.build();
    }

    /**
     * combines a list of snippets into a single new snippet, with a separator snippet inserted in between and prefix
     *  and suffix snippets prepended and appended, respectively. *No* whitespace is added between snippets.<p>
     *
     * This is for special cases only where complete control over whitespace is considered more important than
     *  robustness. If in doubt, use {@link #combine(Iterable, SqlSnippet, SqlSnippet, SqlSnippet)} instead.
     *
     */
    public static SqlSnippet combineNoBlank(Iterable<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        return combineNoBlank(elements.iterator(), prefix, separator, suffix);
    }

    /**
     * combines a sequence of snippets into a single new snippet, with a separator snippet inserted in between and prefix
     *  and suffix snippets prepended and appended, respectively. *No* whitespace is added between snippets.<p>
     *
     * This is for special cases only where complete control over whitespace is considered more important than
     *  robustness. If in doubt, use {@link #combine(Iterator, SqlSnippet, SqlSnippet, SqlSnippet)} instead.
     *
     */
    public static SqlSnippet combineNoBlank(Iterator<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        final SqlBuilder result = builder();
        result.appendNoBlank(prefix);

        boolean first = true;
        while (elements.hasNext()) {
            if(first) {
                first = false;
            }
            else {
                result.appendNoBlank(separator);
            }
            result.appendNoBlank(elements.next());
        }

        result.appendNoBlank(suffix);
        return result.build();
    }
    private static SqlSnippet combineNoBlank(SqlSnippet el0, Iterator<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        final SqlBuilder result = builder();
        result.appendNoBlank(prefix);

        result.appendNoBlank(el0);
        while (elements.hasNext()) {
            result.appendNoBlank(separator);
            result.appendNoBlank(elements.next());
        }

        result.appendNoBlank(suffix);
        return result.build();
    }

    /**
     * creates an IN clause containing given snippets. This method is for the general case that the IN clause
     *  contains arbitrary SQL expressions. For the common case of using a list of known values for the IN clause
     *  use {@link #in(Object, Object...)}.
     *
     * TODO example
     */
    public static SqlSnippet inSnippets(SqlSnippet el0, SqlSnippet... elements) {
        return combineNoBlank(el0, Arrays.asList(elements).iterator(), IN_OPEN, COMMA, CLOSE);
    }
    /**
     * creates an IN clause containing given snippets. This method is for the general case that the IN clause
     *  contains arbitrary SQL expressions. For the common case of using a list of known values for the IN clause
     *  use {@link #in(Iterable)}.
     *
     * TODO example
     */
    public static SqlSnippet inSnippets(Iterable<SqlSnippet> elements) {
        return inSnippets(elements.iterator());
    }
    /**
     * creates an IN clause containing given snippets. This method is for the general case that the IN clause
     *  contains arbitrary SQL expressions. For the common case of using a list of known values for the IN clause
     *  use {@link #in(Iterator)}.
     *
     * TODO example
     */
    public static SqlSnippet inSnippets(Iterator<SqlSnippet> elements) {
        if (!elements.hasNext()) {
            throw new IllegalArgumentException("empty IN clause");
        }
        return combineNoBlank(elements, IN_OPEN, COMMA, CLOSE);
    }

    /**
     * creates an IN clause containing given values.
     *
     * TODO example
     */
    public static SqlSnippet in(Object el0, Object... elements) {
        return combineNoBlank(param(el0),
                AMutableArrayWrapper.wrap(elements).map(SqlSnippet::param).iterator(),
                IN_OPEN, COMMA, CLOSE);
    }

    /**
     * creates an IN clause containing given values. NB: This method does *not* check the number of items against
     *  the (RDBMS specific) maximum number of items in an IN clause. To be on the safe side, use
     *  {@link #chunkedIn(SqlSnippet, Iterable, int)} instead.
     *
     * TODO example
     */
    public static SqlSnippet in(Iterable<?> items) {
        return in(items.iterator());
    }

    /**
     * creates an IN clause containing given values. NB: This method does *not* check the number of items against
     *  the (RDBMS specific) maximum number of items in an IN clause. To be on the safe side, use
     *  {@link #chunkedIn(SqlSnippet, Iterable, int)} instead.
     *
     * TODO example
     */
    public static SqlSnippet in(Iterator<?> items) {
        if (!items.hasNext()) {
            throw new IllegalArgumentException("empty IN clause");
        }
        return inSnippets(AVector.of(params(items)));
    }

    /**
     * creates an IN expression with a default maximum chunk size of 1000, see
     *  {@link #chunkedIn(SqlSnippet, Iterable, int)} for details.
     */
    public static SqlSnippet chunkedIn(SqlSnippet value, Iterable<?> items) {
        return chunkedIn(value, items, 1000);
    }

    /**
     * creates an IN expression checking a value against a list of items. If the number of items exceeds a threshold,
     *  a new IN clause is started and joined to the first one with OR.<p>
     *
     * This method creates a snippet that includes the column to be compared, e.g. {@code "person.id IN (?,?)"}.
     *
     * TODO example
     *
     * @param value the value to check (e.g. a column name)
     * @param items the list of values to check against, i.e. the values listed inside the brackets of IN(...)
     * @param maxChunkSize the maximum number of items to put in a single list. When this number is reached, a new
     *                     IN clause is started
     */
    public static SqlSnippet chunkedIn(SqlSnippet value, Iterable<?> items, int maxChunkSize) {
        final AVector.Builder<SqlSnippet> result = AVector.builder();

        List<?> elList = StreamSupport.stream(items.spliterator(), false).collect(Collectors.toCollection(ArrayList::new));
        while(elList.size() > maxChunkSize) {
            final List<?> l = elList.subList(0, maxChunkSize);
            result.add(concat(value, in(l)));
            elList = elList.subList(maxChunkSize, elList.size());
        }

        if(elList.size() > 0) {
            result.add(concat(value, in(elList)));
        }

        final AVector<SqlSnippet> clauses = result.build();
        if(clauses.isEmpty()) {
            return FALSE;
        }
        return or(clauses);
    }

    /**
     * joins a number of snippets into a new snippet by inserting 'AND' between them. If more than one snippet is passed in,
     *  the resulting expression is parenthesized.
     *
     * TODO example
     */
    public static SqlSnippet and(SqlSnippet el0, SqlSnippet... elements) {
        if(elements.length == 0) {
            return el0;
        }

        return combine(el0, Arrays.asList(elements).iterator(), OPEN, AND, CLOSE);
    }

    /**
     * joins a number of snippets into a new snippet by inserting 'AND' between them. If more than one snippet is passed in,
     *  the resulting expression is parenthesized. If the collection of snippets that are joined is empty, the method returns
     *  a database independent literal representation of 'true'.
     *
     * TODO example
     */
    public static SqlSnippet and(Iterable<SqlSnippet> elements) {
        return and(elements.iterator());
    }

    /**
     * joins a number of snippets into a new snippet by inserting 'AND' between them. If more than one snippet is passed in,
     *  the resulting expression is parenthesized. If the collection of snippets that are joined is empty, the method returns
     *  a database independent literal representation of 'true'.
     *
     * TODO example
     */
    public static SqlSnippet and(Iterator<SqlSnippet> elements) {
        if(! elements.hasNext()) {
            return TRUE;
        }

        final SqlSnippet el0 = elements.next();
        if(elements.hasNext()) {
            return combine(el0, elements, OPEN, AND, CLOSE);
        }
        else {
            return el0;
        }
    }

    /**
     * joins a number of snippets into a new snippet by inserting 'OR' between them. If more than one snippet is passed in,
     *  the resulting expression is parenthesized.
     *
     * TODO example
     */
    public static SqlSnippet or(SqlSnippet el0, SqlSnippet... elements) {
        if(elements.length == 0) {
            return el0;
        }

        return combine(el0, Arrays.asList(elements).iterator(), OPEN, OR, CLOSE);
    }

    /**
     * joins a number of snippets into a new snippet by inserting 'OR' between them. If more than one snippet is passed in,
     *  the resulting expression is parenthesized. If the collection of snippets that are joined is empty, the method returns
     *  a database independent literal representation of 'true'.
     *
     * TODO example
     */
    public static SqlSnippet or(Iterable<SqlSnippet> elements) {
        return or(elements.iterator());
    }

    /**
     * joins a number of snippets into a new snippet by inserting 'OR' between them. If more than one snippet is passed in,
     *  the resulting expression is parenthesized. If the collection of snippets that are joined is empty, the method returns
     *  a database independent literal representation of 'true'.
     *
     * TODO example
     */
    public static SqlSnippet or(Iterator<SqlSnippet> elements) {
        if(! elements.hasNext()) {
            return TRUE;
        }

        final SqlSnippet el0 = elements.next();
        if(elements.hasNext()) {
            return combine(el0, elements, OPEN, OR, CLOSE);
        }
        else {
            return el0;
        }
    }

    /**
     * returns a WHERE clause for a given set of conditions, i.e. 'WHERE' followed by an AND'ed list of conditions.
     *
     * TODO example
     */
    public static SqlSnippet whereClause(SqlSnippet cond0, SqlSnippet... conditions) {
        return whereClause(AIterator.concat(AIterator.single(cond0), Arrays.asList(conditions).iterator()));
    }

    /**
     * returns a WHERE clause for a given set of conditions, i.e. 'WHERE' followed by an AND'ed list of conditions.
     *  If the list of conditions is empty, an empty snippet is returned (i.e. there is no WHERE keyword)
     *
     * TODO example
     */
    public static SqlSnippet whereClause(Iterable<SqlSnippet> conditions) {
        return whereClause(conditions.iterator());
    }

    /**
     * returns a WHERE clause for a given set of conditions, i.e. 'WHERE' followed by an AND'ed list of conditions.
     *  If the list of conditions is empty, an empty snippet is returned (i.e. there is no WHERE keyword)
     *
     * TODO example
     */
    public static SqlSnippet whereClause(Iterator<SqlSnippet> conditions) {
        if(! conditions.hasNext()) return EMPTY;
        return combine(conditions, WHERE, AND, EMPTY);
    }

    @Override public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlSnippet that = (SqlSnippet) o;
        return Objects.equals(sqlString, that.sqlString) &&
                Objects.equals(params, that.params);
    }

    @Override public int hashCode () {
        return Objects.hash(sqlString, params);
    }

    @Override public String toString () {
        return "SqlSnippet{" +
                "sql='" + sqlString + '\'' +
                ", params=" + params +
                '}';
    }
}
