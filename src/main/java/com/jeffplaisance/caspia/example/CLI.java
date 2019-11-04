package com.jeffplaisance.caspia.example;

import com.jeffplaisance.caspia.log.LogClient;
import com.jeffplaisance.caspia.log.JDBCLogReplicaClient;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CLI {
    public static List<JDBCLogReplicaClient> replicas;
    public static LogClient client;

    private static final List<String> connectStrings = Arrays.asList(
            "jdbc:mariadb://localhost:3306/replica1",
            "jdbc:mariadb://localhost:3306/replica2",
            "jdbc:mariadb://localhost:3306/replica3"
    );

    public static DataSource getDataSource(String connectString, String username, String password) {
        final BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUrl(connectString);
        ds.setUsername(username);
        ds.setPassword(password);
        return ds;
    }

    public static void init(String username, String password) {
        replicas = connectStrings.stream()
                .map(str -> new JDBCLogReplicaClient(getDataSource(str, username, password), "log01"))
                .collect(Collectors.toList());
        client = new LogClient(replicas);
    }
}
