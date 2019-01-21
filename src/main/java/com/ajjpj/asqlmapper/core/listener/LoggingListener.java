package com.ajjpj.asqlmapper.core.listener;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;


/**
 * Register this listener on an engine to activate logging of all executed SQL
 */
public class LoggingListener implements SqlEngineEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingListener.class);

    private final ThreadLocal<Instant> start = new ThreadLocal<>();
    private final ThreadLocal<Instant> afterExecution = new ThreadLocal<>(); // for queries

    @Override public void onBeforeQuery (SqlSnippet sql, Class<?> rowClass) {
        start.set(Instant.now());
        log.debug("executing query {}, mapping results to {}", sql, rowClass);
    }

    @Override public void onAfterQueryExecution () {
        final Instant now = Instant.now();
        afterExecution.set(now);
//        log.debug("executed query (ResultSet not yet processed), took {}ms", start.get().until(now, ChronoUnit.MILLIS));
    }

    @Override public void onAfterQueryIteration (int numRows) {
        final long execution = start.get().until(afterExecution.get(), ChronoUnit.MILLIS);
        final long processing = afterExecution.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished SQL query, {} rows - took {}ms (execution) + {}ms (processing) = {}ms total", numRows, execution, processing, execution+processing);
        start.remove();
        afterExecution.remove();
    }

    @Override public void onBeforeInsert (SqlSnippet sql, Class<?> pkCls, AVector<String> columnNames) {
        start.set(Instant.now());
        log.debug("executing insert {}, mapping generated PK values from columns {} as {}", sql, columnNames.mkString("[", ",", "]"), pkCls);
    }

    @Override public void onAfterInsert (Object result) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished inserting, took {}ms", duration);
        start.remove();
    }

    @Override public void onBeforeUpdate (SqlSnippet sql) {
        start.set(Instant.now());
        log.debug("executing update {}", sql);
    }

    @Override public void onAfterUpdate (int result) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("finished updating, took {}ms", duration);
        start.remove();
    }

    @Override public void onFailed (Throwable th) {
        final long duration = start.get().until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("failed SQL after " + duration + "ms", th);
        start.remove();
        afterExecution.remove();
    }
}
