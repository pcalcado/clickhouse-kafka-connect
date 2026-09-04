package kafka_connector;

import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.kafka.connect.ClickHouseSinkConnector;
import com.clickhouse.kafka.connect.sink.ClickHouseSinkConfig;
import com.clickhouse.kafka.connect.sink.data.Record;
import com.clickhouse.kafka.connect.sink.db.ClickHouseWriter;
import com.clickhouse.kafka.connect.sink.db.helper.ClickHouseHelperClient;
import com.clickhouse.kafka.connect.util.QueryIdentifier;
import com.clickhouse.kafka.connect.util.jmx.SinkTaskStatistics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class CompressionLab {

    private static final String CLICKHOUSE_IMAGE_DEFAULT = "clickhouse/clickhouse-server:25.8.18.1";
    private static final String CLICKHOUSE_DATABASE_DEFAULT = "default";
    private static final String CLICKHOUSE_PASSWORD_DEFAULT = "test_password";
    private static final String INSERT_FORMAT_DEFAULT = "json";
    private static final String CLIENT_VERSION_DEFAULT = "V2";
    private static final int CLICKHOUSE_PORT_DEFAULT = 8123;
    private static final int ROWS_DEFAULT = 100000;
    private static final int PAYLOAD_BYTES_DEFAULT = 1024;

    public static void main(String[] args) throws Exception {
        LabConfig config = LabConfig.fromArgs(args);
        if (!"V2".equals(config.clientVersion) && !"V1".equals(config.clientVersion)) {
            throw new IllegalArgumentException("clientVersion must be V1 or V2");
        }

        ClickHouseEndpoint externalEndpoint = externalEndpointFromEnv();
        if (externalEndpoint != null) {
            runLab(config, externalEndpoint);
            return;
        }

        try (ClickHouseContainer container = new ClickHouseContainer(config.image)
                .withPassword(CLICKHOUSE_PASSWORD_DEFAULT)) {
            container.start();

            ClickHouseEndpoint endpoint = new ClickHouseEndpoint(
                    container.getHost(),
                    container.getMappedPort(CLICKHOUSE_PORT_DEFAULT),
                    container.getUsername(),
                    container.getPassword(),
                    false,
                    container.getContainerId());

            runLab(config, endpoint);
        }
    }

    private static void runLab(LabConfig config, ClickHouseEndpoint endpoint) {
        String databaseName = "compression_lab_" + UUID.randomUUID().toString().replace("-", "");
        String tableNameBase = "insert_" + config.clientVersion.toLowerCase(Locale.ROOT) + "_"
                + config.insertFormat + "_" + config.rows;
        String uncompressedTableName = tableNameBase + "_off";
        String compressedTableName = tableNameBase + "_on";

        ClickHouseHelperClient adminClient = createClient(endpoint, CLICKHOUSE_DATABASE_DEFAULT, true, false);
        try {
            executeSql(adminClient, String.format("CREATE DATABASE IF NOT EXISTS `%s`", databaseName), null);
            createTable(adminClient, databaseName, uncompressedTableName);
            createTable(adminClient, databaseName, compressedTableName);

            List<Record> uncompressedRecords = createRecords(
                    uncompressedTableName,
                    config.rows,
                    databaseName,
                    config.payloadBytes,
                    config.insertFormat);
            List<Record> compressedRecords = createRecords(
                    compressedTableName,
                    config.rows,
                    databaseName,
                    config.payloadBytes,
                    config.insertFormat);

            if (config.clientCompression != null) {
                String tableName = config.clientCompression ? compressedTableName : uncompressedTableName;
                List<Record> records = config.clientCompression ? compressedRecords : uncompressedRecords;
                Measurement measurement = runInsert(config, endpoint, databaseName, tableName, records, config.clientCompression);
                printSingleSummary(config, endpoint.containerId, measurement);
            } else {
                Measurement uncompressed = runInsert(config, endpoint, databaseName, uncompressedTableName, uncompressedRecords, false);
                Measurement compressed = runInsert(config, endpoint, databaseName, compressedTableName, compressedRecords, true);
                printSummary(config, endpoint.containerId, uncompressed, compressed);
            }
        } finally {
            try {
                executeSql(adminClient, String.format("DROP TABLE IF EXISTS `%s`.`%s`", databaseName, uncompressedTableName), databaseName);
                executeSql(adminClient, String.format("DROP TABLE IF EXISTS `%s`.`%s`", databaseName, compressedTableName), databaseName);
                executeSql(adminClient, String.format("DROP DATABASE IF EXISTS `%s`", databaseName), null);
            } catch (Exception ignored) {
                // Best effort cleanup for lab resources.
            }
            adminClient.close();
        }
    }

    private static void createTable(ClickHouseHelperClient adminClient, String databaseName, String tableName) {
        executeSql(adminClient, String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.`%s` (`off16` Int32, `str` String) Engine=MergeTree ORDER BY off16",
                databaseName, tableName), databaseName);
    }

    private static Measurement runInsert(
            LabConfig config,
            ClickHouseEndpoint endpoint,
            String databaseName,
            String tableName,
            List<Record> records,
            boolean clientCompression) {
        ClickHouseWriter writer = new ClickHouseWriter(new SinkTaskStatistics(0));
        try {
            if (!writer.start(new ClickHouseSinkConfig(writerProps(endpoint, databaseName, config, clientCompression)))) {
                throw new IllegalStateException("Failed to start ClickHouseWriter");
            }

            long bytesBefore = endpoint.containerId != null ? readContainerNetIoBytes(endpoint.containerId) : -1L;
            long startedAt = System.nanoTime();
            writer.doInsert(records, new QueryIdentifier(tableName, "compression-lab-" + clientCompression));
            long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;
            long bytesAfter = endpoint.containerId != null ? readContainerNetIoBytes(endpoint.containerId) : -1L;

            long totalNetIoBytes = bytesBefore >= 0 && bytesAfter >= 0 ? bytesAfter - bytesBefore : -1L;
            return new Measurement(clientCompression, elapsedMs, totalNetIoBytes);
        } catch (Exception e) {
            throw new RuntimeException("Compression lab insert failed", e);
        } finally {
            writer.stop();
        }
    }

    private static Map<String, String> writerProps(
            ClickHouseEndpoint endpoint,
            String databaseName,
            LabConfig config,
            boolean clientCompression) {
        return Map.of(
                ClickHouseSinkConnector.HOSTNAME, endpoint.host,
                ClickHouseSinkConnector.PORT, String.valueOf(endpoint.port),
                ClickHouseSinkConnector.USERNAME, endpoint.username,
                ClickHouseSinkConnector.PASSWORD, endpoint.password,
                ClickHouseSinkConnector.DATABASE, databaseName,
                ClickHouseSinkConnector.SSL_ENABLED, String.valueOf(endpoint.ssl),
                ClickHouseSinkConnector.CLIENT_VERSION, config.clientVersion,
                ClickHouseSinkConfig.CLIENT_COMPRESSION, String.valueOf(clientCompression),
                ClickHouseSinkConfig.INSERT_FORMAT, config.insertFormat
        );
    }

    private static ClickHouseHelperClient createClient(
            ClickHouseEndpoint endpoint,
            String database,
            boolean useClientV2,
            boolean clientCompression) {
        return new ClickHouseHelperClient.ClickHouseClientBuilder(endpoint.host, endpoint.port, null, null, -1)
                .setDatabase(database)
                .setUsername(endpoint.username)
                .setPassword(endpoint.password)
                .sslEnable(endpoint.ssl)
                .useClientV2(useClientV2)
                .setClientCompression(clientCompression)
                .build();
    }

    private static void executeSql(ClickHouseHelperClient client, String sql, String databaseForRequest) {
        QuerySettings settings = new QuerySettings();
        if (databaseForRequest != null && !databaseForRequest.isBlank()) {
            settings.setDatabase(databaseForRequest);
        }

        try (QueryResponse ignored = client.getClient().query(sql, settings).get()) {
            // no-op
        } catch (Exception e) {
            throw new RuntimeException("Failed SQL: " + sql, e);
        }
    }

    private static List<Record> createRecords(
            String topic,
            int totalRows,
            String database,
            int payloadBytes,
            String insertFormat) {
        switch (insertFormat) {
            case "string":
                return createStringRecords(topic, totalRows, database, payloadBytes);
            case "json":
                return createJsonRecords(topic, totalRows, database, payloadBytes);
            default:
                throw new IllegalArgumentException("Unsupported insertFormat for lab: " + insertFormat);
        }
    }

    private static List<Record> createJsonRecords(String topic, int totalRows, String database, int payloadBytes) {
        List<Record> result = new ArrayList<>(totalRows);
        String repeatedValue = repeatedValue(payloadBytes);
        for (int n = 0; n < totalRows; n++) {
            Map<String, Object> value = Map.of(
                    "off16", n,
                    "str", repeatedValue
            );
            SinkRecord sinkRecord = new SinkRecord(
                    topic,
                    0,
                    null,
                    null,
                    null,
                    value,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME);
            result.add(Record.convert(sinkRecord, false, ".", database, false));
        }
        return result;
    }

    private static List<Record> createStringRecords(String topic, int totalRows, String database, int payloadBytes) {
        List<Record> result = new ArrayList<>(totalRows);
        String repeatedValue = repeatedValue(payloadBytes);
        for (int n = 0; n < totalRows; n++) {
            String jsonEachRowLine = String.format("{\"off16\":%d,\"str\":\"%s\"}%n", n, repeatedValue);
            SinkRecord sinkRecord = new SinkRecord(
                    topic,
                    0,
                    null,
                    null,
                    null,
                    jsonEachRowLine,
                    n,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME);
            result.add(Record.convert(sinkRecord, false, ".", database, false));
        }
        return result;
    }

    private static String repeatedValue(int payloadBytes) {
        return "x".repeat(Math.max(1, payloadBytes));
    }

    private static ClickHouseEndpoint externalEndpointFromEnv() {
        String host = System.getenv("CLICKHOUSE_HOST");
        if (host == null || host.isBlank()) {
            return null;
        }

        int port = Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_PORT", String.valueOf(CLICKHOUSE_PORT_DEFAULT)));
        String username = System.getenv().getOrDefault("CLICKHOUSE_USER", "default");
        String password = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "");
        boolean ssl = Boolean.parseBoolean(System.getenv().getOrDefault("CLICKHOUSE_SSL", "false"));
        return new ClickHouseEndpoint(host, port, username, password, ssl, null);
    }

    private static long readContainerNetIoBytes(String containerId) {
        try {
            Process process = new ProcessBuilder(
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.NetIO}}",
                    containerId)
                    .redirectErrorStream(true)
                    .start();

            String output;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                output = reader.readLine();
            }

            int exitCode = process.waitFor();
            if (exitCode != 0 || output == null || output.isBlank()) {
                throw new IllegalStateException("Could not read docker stats for container " + containerId);
            }

            String[] rxTx = output.split("/");
            if (rxTx.length != 2) {
                throw new IllegalStateException("Unexpected docker stats NetIO format: " + output);
            }

            return parseBytes(rxTx[0].trim()) + parseBytes(rxTx[1].trim());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to read container network IO", e);
        }
    }

    private static long parseBytes(String value) {
        String normalized = value.trim().toUpperCase(Locale.ROOT).replace("IB", "B");
        String numberPart = normalized.replaceAll("[^0-9.]", "");
        String unitPart = normalized.replaceAll("[0-9.\\s]", "");

        double number = Double.parseDouble(numberPart);
        switch (unitPart) {
            case "B":
                return (long) number;
            case "KB":
                return (long) (number * 1_000L);
            case "MB":
                return (long) (number * 1_000_000L);
            case "GB":
                return (long) (number * 1_000_000_000L);
            default:
                throw new IllegalArgumentException("Unsupported byte unit: " + value);
        }
    }

    private static void printSingleSummary(LabConfig config, String containerId, Measurement measurement) {
        System.out.println("CompressionLab");
        System.out.println("containerId=" + containerId);
        System.out.println("clientVersion=" + config.clientVersion);
        System.out.println("insertFormat=" + config.insertFormat);
        System.out.println("rows=" + config.rows);
        System.out.println("payloadBytes=" + config.payloadBytes);
        System.out.println("clientCompression=" + measurement.clientCompression);
        System.out.println("elapsedMs=" + measurement.elapsedMs);
        if (measurement.totalNetIoBytes >= 0) {
            System.out.println("totalNetIoBytes=" + measurement.totalNetIoBytes);
            System.out.println("bytesPerRow=" + (measurement.totalNetIoBytes / (double) config.rows));
        }
    }

    private static void printSummary(
            LabConfig config,
            String containerId,
            Measurement uncompressed,
            Measurement compressed) {
        System.out.println("CompressionLab");
        System.out.println("containerId=" + containerId);
        System.out.println("clientVersion=" + config.clientVersion);
        System.out.println("insertFormat=" + config.insertFormat);
        System.out.println("rows=" + config.rows);
        System.out.println("payloadBytes=" + config.payloadBytes);
        System.out.println();
        System.out.println("clientCompression=false");
        System.out.println("  elapsedMs=" + uncompressed.elapsedMs);
        System.out.println("  totalNetIoBytes=" + uncompressed.totalNetIoBytes);
        System.out.println("  bytesPerRow=" + (uncompressed.totalNetIoBytes / (double) config.rows));
        System.out.println();
        System.out.println("clientCompression=true");
        System.out.println("  elapsedMs=" + compressed.elapsedMs);
        System.out.println("  totalNetIoBytes=" + compressed.totalNetIoBytes);
        System.out.println("  bytesPerRow=" + (compressed.totalNetIoBytes / (double) config.rows));
        if (uncompressed.totalNetIoBytes >= 0 && compressed.totalNetIoBytes >= 0) {
            System.out.println();
            System.out.println("compressedVsUncompressedRatio="
                    + (compressed.totalNetIoBytes / (double) uncompressed.totalNetIoBytes));
        }
    }

    private static class LabConfig {
        private final String image;
        private final String clientVersion;
        private final String insertFormat;
        private final int rows;
        private final int payloadBytes;
        private final Boolean clientCompression;

        private LabConfig(String image, String clientVersion, String insertFormat, int rows, int payloadBytes,
                Boolean clientCompression) {
            this.image = image;
            this.clientVersion = clientVersion;
            this.insertFormat = insertFormat;
            this.rows = rows;
            this.payloadBytes = payloadBytes;
            this.clientCompression = clientCompression;
        }

        private static LabConfig fromArgs(String[] args) {
            String image = System.getenv().getOrDefault("CLICKHOUSE_IMAGE", CLICKHOUSE_IMAGE_DEFAULT);
            String clientVersion = CLIENT_VERSION_DEFAULT;
            String insertFormat = INSERT_FORMAT_DEFAULT;
            int rows = ROWS_DEFAULT;
            int payloadBytes = PAYLOAD_BYTES_DEFAULT;
            Boolean clientCompression = null;

            for (String arg : args) {
                String[] parts = arg.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Expected --key=value arguments, got: " + arg);
                }
                switch (parts[0]) {
                    case "--clientVersion":
                        clientVersion = parts[1];
                        break;
                    case "--insertFormat":
                        insertFormat = parts[1];
                        break;
                    case "--rows":
                        rows = Integer.parseInt(parts[1]);
                        break;
                    case "--payloadBytes":
                        payloadBytes = Integer.parseInt(parts[1]);
                        break;
                    case "--clientCompression":
                        clientCompression = Boolean.parseBoolean(parts[1]);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + parts[0]);
                }
            }

            return new LabConfig(image, clientVersion, insertFormat, rows, payloadBytes, clientCompression);
        }
    }

    private static class Measurement {
        private final boolean clientCompression;
        private final long elapsedMs;
        private final long totalNetIoBytes;

        private Measurement(boolean clientCompression, long elapsedMs, long totalNetIoBytes) {
            this.clientCompression = clientCompression;
            this.elapsedMs = elapsedMs;
            this.totalNetIoBytes = totalNetIoBytes;
        }
    }

    private static class ClickHouseEndpoint {
        private final String host;
        private final int port;
        private final String username;
        private final String password;
        private final boolean ssl;
        private final String containerId;

        private ClickHouseEndpoint(String host, int port, String username, String password, boolean ssl, String containerId) {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.ssl = ssl;
            this.containerId = containerId;
        }
    }
}
