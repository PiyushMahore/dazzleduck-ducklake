package io.dazzleduck.sql.ducklake;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Utility class for DuckLake operations.
 * <p>
 * Provides methods to look up table information from DuckLake metadata tables.
 */
public final class DuckLakeUtil {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeUtil.class);

    private static final String GET_TABLE_ID_SQL = "SELECT table_id FROM %s.ducklake_table WHERE table_name = '%s'";

    private DuckLakeUtil() {
        // Utility class - no instantiation
    }

    /**
     * Looks up the table ID for a given table name from DuckLake metadata.
     *
     * @param metadataDatabase The metadata database name (e.g., "__ducklake_metadata_<catalog>")
     * @param tableName        The table name to look up
     * @return The table ID, or null if not found
     */
    public static Long lookupTableId(String metadataDatabase, String tableName) {
        if (metadataDatabase == null || metadataDatabase.isBlank()) {
            logger.warn("Cannot lookup table_id: metadataDatabase is null or blank");
            return null;
        }
        if (tableName == null || tableName.isBlank()) {
            logger.warn("Cannot lookup table_id: tableName is null or blank");
            return null;
        }

        try {
            String sql = GET_TABLE_ID_SQL.formatted(metadataDatabase, escapeSql(tableName));
            Long tableId = ConnectionPool.collectFirst(sql, Long.class);
            if (tableId == null) {
                logger.debug("Table '{}' not found in metadata database '{}'", tableName, metadataDatabase);
            }
            return tableId;
        } catch (SQLException e) {
            logger.error("Failed to lookup table_id for table '{}' in '{}': {}",
                    tableName, metadataDatabase, e.getMessage());
            return null;
        }
    }

    /**
     * Looks up the table ID for a given table name, throwing an exception if not found.
     *
     * @param metadataDatabase The metadata database name
     * @param tableName        The table name to look up
     * @return The table ID
     * @throws IllegalStateException if the table is not found
     */
    public static long requireTableId(String metadataDatabase, String tableName) {
        Long tableId = lookupTableId(metadataDatabase, tableName);
        if (tableId == null) {
            throw new IllegalStateException(
                    "Table '%s' not found in metadata database '%s'".formatted(tableName, metadataDatabase));
        }
        return tableId;
    }

    /**
     * Escapes single quotes in SQL strings.
     */
    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }
}
