package com.ajjpj.asqlmapper.core.listener;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;


class SqlStatisticsTracker {
    private static final int QUERY=0, INSERT=1, UPDATE=2;

    private final int firstNLimit;
    private final AtomicReference<SqlStatistics> statistics = new AtomicReference<>();

    SqlStatisticsTracker (int firstNLimit) {
        this.firstNLimit = firstNLimit;
        init();
    }

    private void init() {
        statistics.set(new SqlStatistics(Instant.now(), Instant.now(), 0, 0, 0, 0, 0, 0, firstNLimit, AMap.empty()));
    }

    void clear() {
        init();
    }

    SqlStatistics snapshot() {
        return statistics.get();
    }

    void registerQuery(String sql, long durationMillis) {
        doRegister(sql, durationMillis, QUERY);
    }
    void registerInsert(String sql, long durationMillis) {
        doRegister(sql, durationMillis, INSERT);
    }
    void registerUpdate(String sql, long durationMillis) {
        doRegister(sql, durationMillis, UPDATE);
    }

    private void doRegister(String sql, long durationMillis, int kind) {
        SqlStatistics before, after;
        do {
            before = statistics.get();

            final AOption<SqlStatistics.StatementStatistics> optStmtStat = before.getStatisticsByStatement().getOptional(sql);
            final AMap<String, SqlStatistics.StatementStatistics> newStatisticsByStatement;

            final long newNumQueries = before.getNumQueries() + (kind == QUERY ? 1 : 0);
            final long newNumInserts = before.getNumInserts() + (kind == INSERT ? 1 : 0);
            final long newNumUpdates = before.getNumUpdates() + (kind == UPDATE ? 1 : 0);
            final long newTotalQueryMillis = before.getTotalQueryMillis() + (kind == QUERY ? durationMillis : 0);
            final long newTotalInsertMillis = before.getTotalInsertMillis() + (kind == INSERT ? durationMillis : 0);
            final long newTotalUpdateMillis = before.getTotalUpdateMillis() + (kind == UPDATE ? durationMillis : 0);

            if(optStmtStat.isDefined()) {
                newStatisticsByStatement = before.getStatisticsByStatement().plus(sql, optStmtStat.get().plus(durationMillis));
            }
            else {
                if(before.getStatisticsByStatement().size() >= firstNLimit)
                    newStatisticsByStatement = before.getStatisticsByStatement();
                else
                    newStatisticsByStatement = before.getStatisticsByStatement().plus(sql, new SqlStatistics.StatementStatistics(sql, 1, durationMillis, durationMillis, durationMillis));
            }

            after = new SqlStatistics(before.getStartOfTracking(), Instant.now(),
                    newNumQueries, newNumInserts, newNumUpdates,
                    newTotalQueryMillis, newTotalInsertMillis, newTotalUpdateMillis,
                    firstNLimit, newStatisticsByStatement);
        }
        while (! statistics.compareAndSet(before, after));
    }
}
