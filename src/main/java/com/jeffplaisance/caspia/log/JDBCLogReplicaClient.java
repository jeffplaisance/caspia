package com.jeffplaisance.caspia.log;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class JDBCLogReplicaClient implements LogReplicaClient {

    private final DataSource ds;
    private final String table;
    private boolean enabled = true;

    public JDBCLogReplicaClient(DataSource ds, String table) {
        this.ds = ds;
        this.table = table;
    }

    @Override
    public LogReplicaState read(long id) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                final PreparedStatement ps = c.prepareStatement("select proposal, accepted, val from "+table+" where id = ?")
        ) {
            ps.setLong(1, id);
            try (final ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return LogReplicaState.EMPTY;
                return new LogReplicaState(rs.getInt(1), rs.getInt(2), rs.getBytes(3));
            }
        }
    }

    @Override
    public boolean compareAndSet(long id, int proposal, int accepted, byte[] value, int expect_proposal, int expect_accepted) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("update "+table+" set proposal = ?, accepted = ?, val = ? where id = ? AND proposal = ? AND accepted = ?")
        ) {
            ps.setInt(1, proposal);
            ps.setInt(2, accepted);
            ps.setBytes(3, value);
            ps.setLong(4, id);
            ps.setInt(5, expect_proposal);
            ps.setInt(6, expect_accepted);
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public boolean putIfAbsent(long id, int proposal, int accepted, byte[] value) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("insert ignore into "+table+" (id, proposal, accepted, val) values (?, ?, ?, ?)")
        ) {
            ps.setLong(1, id);
            ps.setInt(2, proposal);
            ps.setInt(3, accepted);
            ps.setBytes(4, value);
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public long readLastIndex() throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                final PreparedStatement ps = c.prepareStatement("select max(id) from "+table);
                final ResultSet rs = ps.executeQuery()
        ) {
            if (!rs.next()) return 0;
            return rs.getLong(1);
        }
    }

    public boolean setEnabled(boolean enabled) {
        return this.enabled = enabled;
    }
}
