package com.jeffplaisance.caspia.register;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class JDBCRegisterReplicaClient implements RegisterReplicaClient {

    private final DataSource ds;
    private final String table;
    private final long replicaId;
    private boolean enabled = true;

    public JDBCRegisterReplicaClient(DataSource ds, String table, long replicaId) {
        this.ds = ds;
        this.table = table;
        this.replicaId = replicaId;
    }

    private static void writeVLong(ByteArrayDataOutput out, long l) {
        while (true) {
            byte b = (byte) (l&0x7F);
            l>>>=7;
            if (l == 0) {
                out.writeByte(b);
                break;
            }
            b |= 0x80;
            out.writeByte(b);
        }
    }

    private static long readVLong(ByteArrayDataInput in) {
        long ret = 0;
        long shift = 0;
        while (true) {
            final long b = in.readByte();
            ret |= (b&0x7F)<<shift;
            if (b > 0) return ret;
            shift += 7;
        }
    }

    private static byte[] serialize(long[] replicaIds) {
        final ByteArrayDataOutput out = ByteStreams.newDataOutput((replicaIds.length + 1) * 4);
        writeVLong(out, replicaIds.length);
        for (long l : replicaIds) {
            writeVLong(out, l);
        }
        return out.toByteArray();
    }

    private static long[] deserialize(byte[] encodedReplicaIds) {
        final ByteArrayDataInput in = ByteStreams.newDataInput(encodedReplicaIds);
        final int length = (int) readVLong(in);
        final long[] ret = new long[length];
        for (int i = 0; i < length; i++) {
            ret[i] = readVLong(in);
        }
        return ret;
    }

    @Override
    public RegisterReplicaResponse read(Object id) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                final PreparedStatement ps = c.prepareStatement("select proposal, accepted, val, replicas, quorum_modified = ?, changed_replica = ? from "+table+" where id = ?")
        ) {
            ps.setObject(1, id);
            try (final ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return RegisterReplicaResponse.EMPTY;
                return new RegisterReplicaResponse(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getBytes(3),
                        deserialize(rs.getBytes(4)),
                        rs.getByte(5),
                        rs.getLong(6)
                );
            }
        }
    }

    @Override
    public boolean compareAndSet(Object id, long proposal, long accepted, byte[] value, long[] replicas, byte quorumModified, long changedReplica, long expectProposal, long expectAccepted) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("update "+table+" set proposal = ?, accepted = ?, val = ?, replicas = ?, quorum_modified = ?, changed_replica = ? where id = ? AND proposal = ? AND accepted = ?")
        ) {
            ps.setLong(1, proposal);
            ps.setLong(2, accepted);
            ps.setBytes(3, value);
            ps.setBytes(4, serialize(replicas));
            ps.setByte(5, quorumModified);
            ps.setLong(6, changedReplica);
            ps.setObject(7, id);
            ps.setLong(8, expectProposal);
            ps.setLong(9, expectAccepted);
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public boolean putIfAbsent(Object id, long proposal, long accepted, byte[] value, long[] replicas, byte quorumModified, long changedReplica) throws Exception {
        if (!enabled) throw new IOException();
        try (
                final Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("insert ignore into "+table+" (id, proposal, accepted, val, replicas, quorum_modified, changed_replica) values (?, ?, ?, ?, ?, ?, ?)")
        ) {
            ps.setObject(1, id);
            ps.setLong(2, proposal);
            ps.setLong(3, accepted);
            ps.setBytes(4, value);
            ps.setBytes(5, serialize(replicas));
            ps.setByte(6, quorumModified);
            ps.setLong(7, changedReplica);
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public long getReplicaId() {
        return replicaId;
    }

    public boolean setEnabled(boolean enabled) {
        return this.enabled = enabled;
    }

    @Override
    public void close() {}
}
