package io.dazzleduck.sql.ducklake;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class MetadataConfig {
    private static String qualifier = ".";
    private static HikariDataSource postgresPool = null;

    public static void init(String backend) {
        qualifier = backend.equalsIgnoreCase("postgres") ? ".main." : ".";
    }

    /**
     * Configures a pooled Postgres JDBC connection for metadata writes.
     * When set, Phase 2 of replace() uses this pool instead of DuckDB,
     * enabling INSERT...RETURNING for race-free snapshot ID generation.
     *
     * <p>Requires the JVM timezone to be a valid IANA name (e.g. UTC, America/Los_Angeles).
     * Non-IANA aliases such as "US/Pacific" will be rejected by the Postgres container.
     * Set {@code -Duser.timezone=UTC} in your JVM args (or Surefire argLine) to ensure this.
     */
    public static void initPostgres(String host, int port, String database, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://%s:%d/%s".formatted(host, port, database));
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(30_000);
        postgresPool = new HikariDataSource(config);
    }

    /** Closes the pool and resets all configuration to defaults. Call in @AfterEach to avoid test pollution. */
    public static void reset() {
        qualifier = ".";
        if (postgresPool != null) {
            postgresPool.close();
            postgresPool = null;
        }
    }

    /** Returns true if a Postgres pool has been configured via initPostgres(). */
    public static boolean isPostgres() {
        return postgresPool != null;
    }

    /**
     * Borrows a connection from the Postgres pool.
     * Caller is responsible for closing it (returns it to the pool).
     *
     * @throws IllegalStateException if initPostgres() has not been called
     */
    public static Connection getPostgresConnection() throws SQLException {
        if (postgresPool == null) {
            throw new IllegalStateException(
                    "Postgres connection not configured. Call MetadataConfig.initPostgres() first.");
        }
        return postgresPool.getConnection();
    }

    public static String q() { return qualifier; }
}
