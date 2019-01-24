package com.ajjpj.asqlmapper.core.listener;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;


/**
 * Register this listener on an engine to activate logging of all executed SQL
 */
public class LoggingListener implements SqlEngineEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingListener.class);

    private final ThreadLocal<Instant> start = new ThreadLocal<>();
    private final ThreadLocal<String> sqlString = new ThreadLocal<>();

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
        sqlString.set(sql.getSql());
        log.debug("executing query {}, mapping results to {}", sql, rowClass);
    }

    @Override public void onAfterQueryExecution () {
        final Instant now = Instant.now();
        final long duration = start.get().until(now, ChronoUnit.MILLIS);
        log.debug("executed query (ResultSet not yet processed), took {}ms", duration);
        if (statisticsTracker != null) statisticsTracker.registerQuery(sqlString.get(), duration);
        start.remove();
        sqlString.remove();
    }

    @Override public void onAfterQueryIteration (int numRows) {
        log.debug("finished ResultSet processing: {} rows", numRows);
    }

    @Override public void onBeforeInsert (SqlSnippet sql, Class<?> pkCls, AVector<String> columnNames) {
        start.set(Instant.now());
        sqlString.set(sql.getSql());
        log.debug("executing insert {}, mapping generated PK values from columns {} as {}", sql, columnNames.mkString("[", ",", "]"), pkCls);
    }

    @Override public void onAfterInsert (Object result) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished inserting, took {}ms", duration);
        if (statisticsTracker != null) statisticsTracker.registerInsert(sqlString.get(), duration);
        start.remove();
        sqlString.remove();
    }

    @Override public void onBeforeUpdate (SqlSnippet sql) {
        start.set(Instant.now());
        sqlString.set(sql.getSql());
        log.debug("executing update {}", sql);
    }

    @Override public void onAfterUpdate (int result) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished updating, took {}ms", duration);
        if (statisticsTracker != null) statisticsTracker.registerUpdate(sqlString.get(), duration);
        start.remove();
        sqlString.remove();
    }

    @Override public void onFailed (Throwable th) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("failed SQL after " + duration + "ms", th);
        //TODO keep in statistics?
        start.remove();
        sqlString.remove();
    }

    public SqlStatistics getStatistics () {
        return statisticsTracker != null ? statisticsTracker.snapshot() : null;
    }

    public static ASet<SqlStatistics> getAllStatistics() {
        return ASet.from(all.keySet()).map(LoggingListener::getStatistics);
    }
}
