package com.ajjpj.asqlmapper.core.listener;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Register this listener on an engine to activate logging of all executed SQL
 */
public class LoggingListener implements SqlEngineEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingListener.class);

    private final ThreadLocal<Instant> start = new ThreadLocal<>();
    private final ThreadLocal<SqlSnippet> curSnippet = new ThreadLocal<>();

    private final SqlStatisticsTracker statisticsTracker;

    private static final Map<LoggingListener, Boolean> all = Collections.synchronizedMap(new WeakHashMap<>());

    public static LoggingListener createWithoutStatistics() {
        return new LoggingListener(false, 0);
    }
    public static LoggingListener createWithStatistics(int firstNLimit) {
        return new LoggingListener(true, firstNLimit);
    }

    private LoggingListener (boolean keepStatistics, int firstNLimit) {
        statisticsTracker = keepStatistics ? new SqlStatisticsTracker(firstNLimit) : null;
        all.put(this, true);
    }

    @Override public void onBeforeQuery (SqlSnippet sql, Class<?> rowClass) {
        start.set(Instant.now());
        curSnippet.set(sql);
        log.debug("executing query {}, mapping results to {}", sql, rowClass);
    }

    @Override public void onAfterQueryExecution () {
        final Instant now = Instant.now();
        final long duration = start.get().until(now, ChronoUnit.MILLIS);
        log.debug("executed query (ResultSet not yet processed), took {}ms", duration);
        if (statisticsTracker != null) statisticsTracker.registerQuery(curSnippet.get().getSql(), duration);
        start.remove();
        curSnippet.remove();
    }

    @Override public void onAfterQueryIteration (int numRows) {
        log.debug("finished ResultSet processing: {} rows", numRows);
    }

    @Override public void onBeforeInsert (SqlSnippet sql, Class<?> pkCls, AVector<String> columnNames) {
        start.set(Instant.now());
        curSnippet.set(sql);
        log.debug("executing insert {}, mapping generated PK values from columns {} as {}", sql, columnNames.mkString("[", ",", "]"), pkCls);
    }

    @Override public void onAfterInsert (Object result) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished inserting (auto-generated PKs: {}), took {}ms", result, duration);
        if (statisticsTracker != null) statisticsTracker.registerInsert(curSnippet.get().getSql(), duration);
        start.remove();
        curSnippet.remove();
    }

    @Override public void onBeforeUpdate (SqlSnippet sql) {
        start.set(Instant.now());
        curSnippet.set(sql);
        log.debug("executing update {}", sql);
    }

    @Override public void onAfterUpdate (long result) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished updating ({} rows), took {}ms", result, duration);
        if (statisticsTracker != null) statisticsTracker.registerUpdate(curSnippet.get().getSql(), duration);
        start.remove();
        curSnippet.remove();
    }

    @Override public void onBeforeBatchUpdate(String sql, int size) {
        start.set(Instant.now());
        curSnippet.set(SqlSnippet.sql(sql));
        log.debug("executing batch update {} ({} batch items)", sql, size);
    }
    @Override public void onAfterBatchUpdate() {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished batch update, took {}ms", duration);
        if (statisticsTracker != null) statisticsTracker.registerUpdate(curSnippet.get().getSql(), duration);
        start.remove();
        curSnippet.remove();
    }
    @Override public void onFailed (Throwable th) {
        final Instant startInstant = start.get();

        if (startInstant != null) {
            final long durationMillis = start.get().until(Instant.now(), ChronoUnit.MILLIS);
            logFailed(th, curSnippet.get(), durationMillis);
            //TODO add to statistics?
        }
        start.remove();
        curSnippet.remove();
    }

    protected void logFailed(Throwable th, SqlSnippet snippet, long durationMillis) {
        log.debug("failed SQL after " + durationMillis + "ms: " + snippet.getSql(), th);
    }

    public SqlStatistics getStatistics () {
        return statisticsTracker != null ? statisticsTracker.snapshot() : null;
    }

    public static ASet<SqlStatistics> getAllStatistics() {
        return ASet.from(all.keySet()).map(LoggingListener::getStatistics);
    }
}
