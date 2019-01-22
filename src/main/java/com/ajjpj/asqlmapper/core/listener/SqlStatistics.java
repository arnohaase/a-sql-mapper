package com.ajjpj.asqlmapper.core.listener;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.AMap;

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;


public class SqlStatistics {
    private final Instant startOfTracking;
    private final Instant endOfTracking;

    private final long numQueries;
    private final long numInserts;
    private final long numUpdates;

    private final long totalQueryMillis;
    private final long totalInsertMillis;
    private final long totalUpdateMillis;

    private final int firstNLimit;
    private final AMap<String, StatementStatistics> statisticsByStatement;

    private AList<StatementStatistics> statementStatistics;

    SqlStatistics (Instant startOfTracking, Instant endOfTracking, long numQueries, long numInserts, long numUpdates,
                   long totalQueryMillis, long totalInsertMillis, long totalUpdateMillis, int firstNLimit, AMap<String, StatementStatistics> statisticsByStatement) {
        this.startOfTracking = startOfTracking;
        this.endOfTracking = endOfTracking;
        this.numQueries = numQueries;
        this.numInserts = numInserts;
        this.numUpdates = numUpdates;
        this.totalQueryMillis = totalQueryMillis;
        this.totalInsertMillis = totalInsertMillis;
        this.totalUpdateMillis = totalUpdateMillis;
        this.firstNLimit = firstNLimit;
        this.statisticsByStatement = statisticsByStatement;
    }

    public Instant getStartOfTracking () {
        return startOfTracking;
    }

    public Instant getEndOfTracking () {
        return endOfTracking;
    }

    public long getNumQueries () {
        return numQueries;
    }

    public long getNumInserts () {
        return numInserts;
    }

    public long getNumUpdates () {
        return numUpdates;
    }

    public long getTotalQueryMillis () {
        return totalQueryMillis;
    }

    public long getTotalInsertMillis () {
        return totalInsertMillis;
    }

    public long getTotalUpdateMillis () {
        return totalUpdateMillis;
    }

    public int getFirstNLimit () {
        return firstNLimit;
    }

    public AMap<String, StatementStatistics> getStatisticsByStatement () {
        return statisticsByStatement;
    }

    public AList<StatementStatistics> getStatementStatistics() {
        if (statementStatistics == null) {
            statementStatistics = statisticsByStatement.values().toVector()
                    .sorted(Comparator.<StatementStatistics,String>comparing(stmtStat -> String.format("%019d", stmtStat.totalMillis()) + stmtStat.sql).reversed());
        }
        return statementStatistics;
    }

    @Override public String toString () {
        return "SqlStatistics{" +
                "startOfTracking=" + startOfTracking +
                ", endOfTracking=" + endOfTracking +
                ", numQueries=" + numQueries +
                ", numInserts=" + numInserts +
                ", numUpdates=" + numUpdates +
                ", totalQueryMillis=" + totalQueryMillis +
                ", totalInsertMillis=" + totalInsertMillis +
                ", totalUpdateMillis=" + totalUpdateMillis +
                ", firstNLimit=" + firstNLimit +
                ", statisticsByStatement=" + statisticsByStatement +
                '}';
    }

    public static class StatementStatistics {
        private final String sql;
        private final long numInvocations;
        private final long minMillis;
        private final long maxMillis;
        private final long totalMillis;

        StatementStatistics (String sql, long numInvocations, long minMillis, long maxMillis, long totalMillis) {
            this.sql = sql;
            this.numInvocations = numInvocations;
            this.minMillis = minMillis;
            this.maxMillis = maxMillis;
            this.totalMillis = totalMillis;
        }

        public String sql() {
            return sql;
        }
        public long numInvocations() {
            return numInvocations;
        }
        public long totalMillis() {
            return totalMillis;
        }
        public long minMillis() {
            return minMillis;
        }
        public long maxMillis() {
            return maxMillis;
        }
        public long avgMillis() {
            return totalMillis / numInvocations;
        }

        @Override public String toString () {
            return "StatementStatistics{" +
                    "sql='" + sql + '\'' +
                    ", numInvocations=" + numInvocations +
                    ", minMillis=" + minMillis +
                    ", maxMillis=" + maxMillis +
                    ", totalMillis=" + totalMillis +
                    '}';
        }

        StatementStatistics plus (long durationMillis) {
            return new StatementStatistics(sql, numInvocations+1, Math.min(minMillis, durationMillis), Math.max(maxMillis, durationMillis), totalMillis+durationMillis);
        }
    }
}
