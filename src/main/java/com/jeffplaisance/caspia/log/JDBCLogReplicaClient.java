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
    public boolean compareAndSet(long id, LogReplicaState update, LogReplicaState expect) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("update "+table+" set proposal = ?, accepted = ?, val = ? where id = ? AND proposal = ? AND accepted = ?")
        ) {
            ps.setInt(1, update.getProposal());
            ps.setInt(2, update.getAccepted());
            ps.setBytes(3, update.getValue());
            ps.setLong(4, id);
            ps.setInt(5, expect.getProposal());
            ps.setInt(6, expect.getAccepted());
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public boolean putIfAbsent(long id, LogReplicaState update) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("insert ignore into "+table+" (id, proposal, accepted, val) values (?, ?, ?, ?)")
        ) {
            ps.setLong(1, id);
            ps.setInt(2, update.getProposal());
            ps.setInt(3, update.getAccepted());
            ps.setBytes(4, update.getValue());
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
