package com.taboola.spark;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DbInstance {
    private static final String JDBC_URL = "jdbc:hsqldb:hsql://localhost/xdb";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";
    private static final String JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";

    static DbInstance INSTANCE = new DbInstance();

    private final HikariDataSource dataSource;

    private DbInstance(){
        dataSource = getDataSource();
    }

    HikariDataSource getDatasource(){
        return this.dataSource;
    }

    private HikariDataSource getDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PASSWORD);
        config.setDriverClassName(JDBC_DRIVER);
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }
}
