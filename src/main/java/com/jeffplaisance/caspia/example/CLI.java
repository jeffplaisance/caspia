/*
Copyright 2023 Jeff Plaisance

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.jeffplaisance.caspia.example;

import com.google.common.primitives.Longs;
import com.jeffplaisance.caspia.common.NonDelimitedStringTranscoder;
import com.jeffplaisance.caspia.log.LogClient;
import com.jeffplaisance.caspia.log.JDBCLogReplicaClient;
import com.jeffplaisance.caspia.register.JDBCRegisterReplicaClient;
import com.jeffplaisance.caspia.register.RegisterClient;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CLI {
    public static List<JDBCLogReplicaClient> replicas;
    public static LogClient client;
    public static RegisterClient<String> register;

    private static final List<String> connectStrings = Arrays.asList(
            "jdbc:mariadb://localhost:3306/replica1",
            "jdbc:mariadb://localhost:3306/replica2",
            "jdbc:mariadb://localhost:3306/replica3",
            "jdbc:mariadb://localhost:3306/replica4",
            "jdbc:mariadb://localhost:3306/replica5"
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
        replicas = connectStrings.subList(0, 3).stream()
                .map(str -> new JDBCLogReplicaClient(getDataSource(str, username, password), "log01"))
                .collect(Collectors.toList());
        client = new LogClient(replicas);
        register = new RegisterClient<>(
                Longs.asList(1, 2, 3, 4, 5),
                x -> new JDBCRegisterReplicaClient(getDataSource(connectStrings.get(x.intValue()-1), username, password),"registers", x),
                new NonDelimitedStringTranscoder(),
                "jeff");
    }
}
