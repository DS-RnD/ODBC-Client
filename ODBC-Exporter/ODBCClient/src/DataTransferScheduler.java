import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

/**
 * DataTransferScheduler is responsible for scheduling and managing the transfer of data
 * from a source database to multiple destination databases.
 */
public class DataTransferScheduler {

    private static final int BATCH_SIZE = 1000;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(DataTransferScheduler.class);
    private static final Map<String, List<DestinationConfig>> tableDestinationsMap = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);
    private static final Map<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * The main method is the entry point of the program.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            LocalDateTime currentTime = LocalDateTime.now();
            System.out.println("Program started at " + currentTime);

            // Load configuration from JSON file
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode config = objectMapper.readTree(new File("../config/config.json"));

            // Parse source parameters
            SourceConfig sourceConfig = parseSourceConfig(config.get("source_parameters"));

            // Create HikariCP data source for the source database and add it to dataSourceMap
            HikariDataSource sourceDataSource = createDataSource(sourceConfig);
            String sourceKey = sourceConfig.ip + "_" + sourceConfig.db;
            dataSourceMap.put(sourceKey, sourceDataSource);
            System.out.println("Got the source parameters !!");

            // Parse export parameters and gather servers by table
            JsonNode exportConfig = config.get("export_parameters");
            Iterator<JsonNode> exportHosts = exportConfig.get("hosts").elements();

            while (exportHosts.hasNext()) {
                JsonNode exportHost = exportHosts.next();
                DestinationConfig destinationConfig = parseDestinationConfig(exportHost);

                // Create HikariCP data source for each destination and add it to dataSourceMap
                HikariDataSource destinationDataSource = createDataSource(destinationConfig);
                String destinationKey = destinationConfig.destinationIP + "_" + destinationConfig.destinationDB;
                dataSourceMap.put(destinationKey, destinationDataSource);

                for (String tableName : objectMapper.convertValue(exportHost.get("tables_to_export"), String[].class)) {
                    tableDestinationsMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(destinationConfig);
                }
            }
            System.out.println("Got the export parameters !!");
            System.out.println("Length of tableDestinationsMap is " + tableDestinationsMap.size());
            System.out.println("Length of dataSourceMap is " + dataSourceMap.size());

            // Schedule tasks
            scheduleDataExportTasks(sourceDataSource, sourceConfig.db, currentTime);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses the source configuration from a JSON node.
     *
     * @param sourceConfigNode The JSON node containing the source configuration
     * @return The parsed SourceConfig object
     */
    private static SourceConfig parseSourceConfig(JsonNode sourceConfigNode) {
        JsonNode odbcConfig = sourceConfigNode.get("odbc_configuration");
        return new SourceConfig(
                odbcConfig.get("ip").asText(),
                odbcConfig.get("db").asText(),
                odbcConfig.get("user_name").asText(),
                odbcConfig.get("encrypted_password").asText(),
                odbcConfig.get("port").asInt()
        );
    }

    /**
     * Creates a HikariCP data source from the given source configuration.
     *
     * @param config The source configuration
     * @return The created HikariDataSource
     */
    private static HikariDataSource createDataSource(SourceConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        String sourceUrl = String.format("jdbc:sqlserver://%s:%d;DatabaseName=%s;trustServerCertificate=true",
                config.ip, config.port, config.db);
        hikariConfig.setJdbcUrl(sourceUrl);
        hikariConfig.setUsername(config.userName);
        hikariConfig.setPassword(config.password);
        hikariConfig.setMaximumPoolSize(20);
        hikariConfig.setMinimumIdle(10);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setConnectionTimeout(30000);
        return new HikariDataSource(hikariConfig);
    }

    /**
     * Creates a HikariCP data source from the given destination configuration.
     *
     * @param config The destination configuration
     * @return The created HikariDataSource
     */
    private static HikariDataSource createDataSource(DestinationConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        String destUrl = String.format("jdbc:sqlserver://%s:%d;DatabaseName=%s;trustServerCertificate=true",
                config.destinationIP, config.port, config.destinationDB);
        hikariConfig.setJdbcUrl(destUrl);
        hikariConfig.setUsername(config.userName);
        hikariConfig.setPassword(config.password);
        hikariConfig.setMaximumPoolSize(20);
        hikariConfig.setMinimumIdle(10);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setConnectionTimeout(30000);
        return new HikariDataSource(hikariConfig);
    }

    /**
     * Parses the destination configuration from a JSON node.
     *
     * @param exportHost The JSON node containing the destination configuration
     * @return The parsed DestinationConfig object
     */
    private static DestinationConfig parseDestinationConfig(JsonNode exportHost) {
        JsonNode odbcConfig = exportHost.get("odbc_configuration");
        return new DestinationConfig(
                odbcConfig.get("ip").asText(),
                odbcConfig.get("db").asText(),
                exportHost.get("ScheduleIntervalInSeconds").asInt(),
                odbcConfig.get("user_name").asText(),
                odbcConfig.get("encrypted_password").asText(),
                odbcConfig.get("port").asInt()
        );
    }

    /**
     * Schedules the data export tasks.
     *
     * @param sourceDataSource The data source for the source database
     * @param sourceDB The source database name
     * @param currentTime The current time
     */
    private static void scheduleDataExportTasks(HikariDataSource sourceDataSource, String sourceDB, LocalDateTime currentTime) {
        for (Map.Entry<String, List<DestinationConfig>> entry : tableDestinationsMap.entrySet()) {
            String tableName = entry.getKey();
            List<DestinationConfig> destinationConfigs = entry.getValue();
            int scheduleInterval = destinationConfigs.get(0).scheduleInterval;

            LocalDateTime nearClockTime = currentTime.truncatedTo(ChronoUnit.HOURS);
            while (!nearClockTime.isAfter(currentTime)) {
                nearClockTime = nearClockTime.plusSeconds(scheduleInterval);
            }
            long initialDelay = Duration.between(currentTime, nearClockTime).getSeconds();
            System.out.println("Initial Delay " + initialDelay);
            scheduler.scheduleAtFixedRate(() -> {
                executorService.submit(() ->
                        exportData(sourceDataSource, sourceDB, tableName, destinationConfigs));
            }, initialDelay, scheduleInterval, TimeUnit.SECONDS);
        }
    }

    /**
     * Exports data from the source database to the destination databases.
     *
     * @param sourceDataSource The data source for the source database
     * @param sourceDB The source database name
     * @param tableName The name of the table to export
     * @param destinationConfigs The list of destination configurations
     */
    private static void exportData(HikariDataSource sourceDataSource, String sourceDB, String tableName, List<DestinationConfig> destinationConfigs) {
        LocalDateTime currentTime = LocalDateTime.now();
        System.out.println("Export starts at " + currentTime);
        try (Connection sourceConn = sourceDataSource.getConnection()) {
            String finalTable = sourceDB + "." + sourceDB + "." + tableName;
            String exportQuery = "SELECT * FROM " + finalTable + "_temp WHERE record_timestamp BETWEEN ? AND ?";

            try (PreparedStatement exportStmt = sourceConn.prepareStatement(exportQuery)) {
                exportStmt.setString(1, currentTime.minusSeconds(destinationConfigs.get(0).scheduleInterval).format(formatter));
                exportStmt.setString(2, currentTime.format(formatter));
                long startTime = System.currentTimeMillis();
                try (ResultSet rs = exportStmt.executeQuery()) {
                    processResultSetAndInsertData(rs, destinationConfigs, tableName);
                }
                System.out.println("Data exported successfully at " + LocalDateTime.now());
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;
                System.out.println("Duration : " + duration);
                // Delete rows older than 5 minutes
                String deleteQuery = "DELETE FROM " + finalTable + "_temp WHERE record_timestamp < ?";
                try (PreparedStatement deleteStmt = sourceConn.prepareStatement(deleteQuery)) {
                    long seconds = destinationConfigs.get(0).scheduleInterval * 2L;
                    deleteStmt.setString(1, currentTime.minusSeconds(seconds).format(formatter));
                    int deletedRows = deleteStmt.executeUpdate();
                    System.out.println("Deleted " + deletedRows + " rows from " + finalTable + "_temp");
                }
                System.out.println("Data deleted successfully at " + LocalDateTime.now());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Processes the result set and inserts the data into the destination databases.
     *
     * @param rs The result set from the source database
     * @param destinationConfigs The list of destination configurations
     * @param sourceTableName The name of the source table
     * @throws SQLException If a database access error occurs
     */
    private static void processResultSetAndInsertData(ResultSet rs, List<DestinationConfig> destinationConfigs, String sourceTableName) throws SQLException {
        List<String> rows = new ArrayList<>();
        int rowCount = 0;

        while (rs.next()) {
            rowCount++;
            StringBuilder rowBuilder = new StringBuilder("(");
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 2; i <= columnCount; i++) {
                rowBuilder.append("'").append(rs.getString(i)).append("'");
                if (i < columnCount) {
                    rowBuilder.append(",");
                }
            }
            rowBuilder.append(")");
            rows.add(rowBuilder.toString());

            if (rowCount % BATCH_SIZE == 0) {
                executeInsertQuery(destinationConfigs, sourceTableName, rows);
                rows.clear();
            }
        }

        if (!rows.isEmpty()) {
            executeInsertQuery(destinationConfigs, sourceTableName, rows);
        }
        System.out.println("No of Rows added " + rowCount);
    }

    /**
     * Executes the insert query to insert data into the destination databases.
     *
     * @param destinationConfigs The list of destination configurations
     * @param sourceTableName The name of the source table
     * @param rows The rows to be inserted
     * @throws SQLException If a database access error occurs
     */
    private static void executeInsertQuery(List<DestinationConfig> destinationConfigs, String sourceTableName, List<String> rows) throws SQLException {
        for (DestinationConfig destinationConfig : destinationConfigs) {
            executorService.submit(() -> {
                String destKey = destinationConfig.destinationIP + "_" + destinationConfig.destinationDB;
                HikariDataSource destDataSource = dataSourceMap.get(destKey);

                if (destDataSource != null) {
                    String insertQuery = "INSERT INTO " + destinationConfig.destinationDB + "." + destinationConfig.destinationDB + "." + sourceTableName + " VALUES " + String.join(",", rows);

                    try (Connection destConn = destDataSource.getConnection();
                         Statement insertStmt = destConn.createStatement()) {
                        insertStmt.executeUpdate(insertQuery);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("Batch added to " + destKey);
            });
        }
    }

    /**
     * Represents the configuration for the source database.
     */
    static class SourceConfig {
        String ip;
        String db;
        String userName;
        String password;
        int port;

        /**
         * Constructs a SourceConfig object.
         *
         * @param ip The IP address of the source database
         * @param db The name of the source database
         * @param userName The username for the source database
         * @param password The password for the source database
         * @param port The port number for the source database
         */
        SourceConfig(String ip, String db, String userName, String password, int port) {
            this.ip = ip;
            this.db = db;
            this.userName = userName;
            this.password = password;
            this.port = port;
        }
    }

    /**
     * Represents the configuration for the destination database.
     */
    static class DestinationConfig {
        String destinationIP;
        String destinationDB;
        int scheduleInterval;
        String userName;
        String password;
        int port;

        /**
         * Constructs a DestinationConfig object.
         *
         * @param destinationIP The IP address of the destination database
         * @param destinationDB The name of the destination database
         * @param scheduleInterval The schedule interval for data export
         * @param userName The username for the destination database
         * @param password The password for the destination database
         * @param port The port number for the destination database
         */
        DestinationConfig(String destinationIP, String destinationDB, int scheduleInterval, String userName, String password, int port) {
            this.destinationIP = destinationIP;
            this.destinationDB = destinationDB;
            this.scheduleInterval = scheduleInterval;
            this.userName = userName;
            this.password = password;
            this.port = port;
        }
    }
}
