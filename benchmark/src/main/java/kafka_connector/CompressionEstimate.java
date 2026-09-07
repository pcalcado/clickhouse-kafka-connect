package kafka_connector;

import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import net.jpountz.lz4.LZ4Factory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class CompressionEstimate {

    private static final int ROWS_DEFAULT = 100000;
    private static final int PAYLOAD_BYTES_DEFAULT = 1024;
    private static final String INSERT_FORMAT_DEFAULT = "json";
    private static final String PAYLOAD_MODE_DEFAULT = "profile";
    private static final long PAYLOAD_SEED_DEFAULT = 528L;
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final String[] REGIONS = {
            "us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1",
            "ap-northeast-1", "sa-east-1", "ca-central-1", "me-central-1", "af-south-1"
    };
    private static final String[] EVENT_TYPES = {
            "login", "purchase", "click", "open", "close"
    };

    public static void main(String[] args) throws Exception {
        EstimateConfig config = EstimateConfig.fromArgs(args);
        if (!"json".equals(config.insertFormat) && !"string".equals(config.insertFormat)) {
            throw new IllegalArgumentException("insertFormat must be json or string");
        }
        if (!"repeated".equals(config.payloadMode)
                && !"seeded".equals(config.payloadMode)
                && !"profile".equals(config.payloadMode)) {
            throw new IllegalArgumentException("payloadMode must be repeated, seeded, or profile");
        }

        byte[] raw = buildPayload(config).getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compress(raw);

        System.out.println("CompressionEstimate method=clickhouse-native-lz4-in-memory");
        System.out.println("estimatedInsertFormat=" + config.insertFormat);
        System.out.println("estimatedRows=" + config.rows);
        System.out.println("estimatedPayloadBytes=" + config.payloadBytes);
        System.out.println("estimatedPayloadMode=" + config.payloadMode);
        System.out.println("estimatedPayloadSeed=" + config.payloadSeed);
        System.out.println("estimatedRawBytes=" + raw.length);
        System.out.println("estimatedClickHouseLz4Bytes=" + compressed.length);
        System.out.println("estimatedClickHouseLz4Ratio=" + (compressed.length / (double) raw.length));
    }

    private static String buildPayload(EstimateConfig config) {
        StringBuilder payload = new StringBuilder();
        for (int row = 0; row < config.rows; row++) {
            if ("profile".equals(config.payloadMode)) {
                payload.append(profileJsonLine(config.payloadBytes, config.payloadSeed, row));
            } else {
                payload.append(String.format("{\"off16\":%d,\"str\":\"%s\"}%n",
                        row, valueForRow(config.payloadBytes, config.payloadMode, config.payloadSeed, row)));
            }
        }
        return payload.toString();
    }

    private static byte[] compress(byte[] raw) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ClickHouseLZ4OutputStream stream = new ClickHouseLZ4OutputStream(
                output,
                LZ4Factory.fastestInstance().fastCompressor(),
                ClickHouseLZ4OutputStream.UNCOMPRESSED_BUFF_SIZE)) {
            stream.write(raw);
        }
        return output.toByteArray();
    }

    private static String valueForRow(int payloadBytes, String payloadMode, long payloadSeed, int rowNumber) {
        if ("seeded".equals(payloadMode)) {
            return seededToken(Math.max(1, payloadBytes), payloadSeed, rowNumber);
        }
        return "x".repeat(Math.max(1, payloadBytes));
    }

    private static String profileJsonLine(int payloadBytes, long payloadSeed, int rowNumber) {
        int tokenLength = Math.max(8, payloadBytes / 2);
        return String.format(
                "{\"user_id\":%d,\"session_id\":\"%s\",\"request_id\":\"%s\",\"region\":\"%s\",\"event_type\":\"%s\"}%n",
                new Random(payloadSeed + rowNumber).nextInt(Integer.MAX_VALUE),
                seededToken(tokenLength, payloadSeed + 1_000_000L, rowNumber),
                seededToken(tokenLength, payloadSeed + 2_000_000L, rowNumber),
                REGIONS[new Random(payloadSeed + rowNumber).nextInt(REGIONS.length)],
                EVENT_TYPES[new Random(payloadSeed * 3 + rowNumber).nextInt(EVENT_TYPES.length)]);
    }

    private static String seededToken(int length, long payloadSeed, int rowNumber) {
        Random random = new Random(payloadSeed + rowNumber);
        StringBuilder value = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            value.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return value.toString();
    }

    private static class EstimateConfig {
        private final String insertFormat;
        private final int rows;
        private final int payloadBytes;
        private final String payloadMode;
        private final long payloadSeed;

        private EstimateConfig(String insertFormat, int rows, int payloadBytes, String payloadMode, long payloadSeed) {
            this.insertFormat = insertFormat;
            this.rows = rows;
            this.payloadBytes = payloadBytes;
            this.payloadMode = payloadMode;
            this.payloadSeed = payloadSeed;
        }

        private static EstimateConfig fromArgs(String[] args) {
            String insertFormat = INSERT_FORMAT_DEFAULT;
            int rows = ROWS_DEFAULT;
            int payloadBytes = PAYLOAD_BYTES_DEFAULT;
            String payloadMode = PAYLOAD_MODE_DEFAULT;
            long payloadSeed = PAYLOAD_SEED_DEFAULT;

            for (String arg : args) {
                String[] parts = arg.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Expected --key=value arguments, got: " + arg);
                }
                switch (parts[0]) {
                    case "--insertFormat":
                        insertFormat = parts[1];
                        break;
                    case "--rows":
                        rows = Integer.parseInt(parts[1]);
                        break;
                    case "--payloadBytes":
                        payloadBytes = Integer.parseInt(parts[1]);
                        break;
                    case "--payloadMode":
                        payloadMode = parts[1];
                        break;
                    case "--payloadSeed":
                        payloadSeed = Long.parseLong(parts[1]);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + parts[0]);
                }
            }

            return new EstimateConfig(insertFormat, rows, payloadBytes, payloadMode, payloadSeed);
        }
    }
}
