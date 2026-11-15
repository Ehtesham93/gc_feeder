package org.mahindradatahandler.config;

import lombok.Getter;
import lombok.Setter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

@Getter
@Setter
public class LibConfig {
    public static final String APP_ID_TAG = "id";
    public static final String KINESIS_CONS_TAG = "kinesisconsumer";
    public static final String CLICKHOUSE_PUB_TAG = "clickhouseproducer";
    public static final String REDIS_PUB_TAG = "redisproducer";
    public static final String META_DB_TAG = "metadb";
    public static final String HANDLER_TAG = "datahandler";
    public static final String LOG_METRICS_WRITER_TAG = "lognmetricswriter";

    private ObjectMapper objectMapper = new ObjectMapper();
    private String applicationName;
    private String streamName;
    private String awsRegion;
    // private String stopFile;
    // private ArrayList<String> shardIds;
    // Instance variables
    private String configPath;
    private JsonNode kinesisConsumerProps;
    private JsonNode clickhousePubProps;
    private JsonNode redisPubProps;
    private JsonNode metaDbProps;
    private JsonNode logMetricsWriterProps;
    // private JsonNode handlerProps;
    private String appId;
    private boolean isValid = false;
    private ArrayList<String> requiredTags = new ArrayList<>(Arrays.asList(
            APP_ID_TAG,
            KINESIS_CONS_TAG,
            CLICKHOUSE_PUB_TAG,
            REDIS_PUB_TAG,
            META_DB_TAG,
            LOG_METRICS_WRITER_TAG));

    public LibConfig() {
        String configPath = getConfigFilePath();
        this.configPath = configPath;
        if (!fileExists(configPath)) {
            System.out.println("Config file not found");
            throw new RuntimeException("Config file not found");
        }

        System.out.println("Loading config file: " + configPath);
        initProperties();
    }

    private String getConfigFilePath() {
        String appEnv = System.getenv("APP_ENV");
        System.out.println("App environment: " + appEnv);
        String configPath = "src/main/resources/";
        if (appEnv != null && appEnv.equals("PRODUCTION")) {
            configPath += "prod.json";
        } else if (appEnv != null && appEnv.equals("STAGING")) {
            configPath += "staging.json";
        } else {
            configPath += "sampleconfig.json";
        }

        return configPath;
    }

    private void initProperties() {
        try {
            JsonNode root = objectMapper.readTree(new File(configPath));

            // Validate required tags
            for (String requiredTag : requiredTags) {
                if (!root.has(requiredTag)) {
                    System.out.println("Please provide " + requiredTag + " tag in the config");
                    return;
                }
            }

            // Set basic properties
            this.appId = root.get(APP_ID_TAG).asText();
            // Set component properties
            this.kinesisConsumerProps = root.get(KINESIS_CONS_TAG);
            this.applicationName = this.kinesisConsumerProps.get("applicationname").asText();
            this.streamName = this.kinesisConsumerProps.get("streamname").asText();
            this.awsRegion = this.kinesisConsumerProps.get("awsregion").asText();
            // this.shardIds = new ArrayList<>();
            // this.kinesisConsumerProps.get("shardids").forEach(id ->
            // this.shardIds.add(id.asText()));

            this.clickhousePubProps = root.get(CLICKHOUSE_PUB_TAG);
            this.redisPubProps = root.get(REDIS_PUB_TAG);
            this.metaDbProps = root.get(META_DB_TAG);
            this.logMetricsWriterProps = root.get(LOG_METRICS_WRITER_TAG);
            // this.handlerProps = root.get(HANDLER_TAG);
            // this.stopFile = this.handlerProps.get("stopfile").asText();
            applyEnvOverrides();
            this.isValid = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Credentials injected by ECS from Secrets Manager take precedence over the
     * config file. An unset or blank variable leaves the config file value alone,
     * so environments that do not define a secret still start up.
     */
    private void applyEnvOverrides() {
        overrideFromEnv(this.metaDbProps, "username", "PGDB_USR");
        overrideFromEnv(this.metaDbProps, "password", "PGDB_PSW");
        overrideFromEnv(this.clickhousePubProps, "user", "CLICKHOUSE_USR");
        overrideFromEnv(this.clickhousePubProps, "password", "CLICKHOUSE_PSW");
    }

    private void overrideFromEnv(JsonNode props, String field, String envName) {
        String value = System.getenv(envName);
        System.out.println(envName + " = " + value);
        if (value != null && !value.trim().isEmpty() && props instanceof ObjectNode) {
            ((ObjectNode) props).put(field, value.trim());
        }
    }

    // Utility methods
    public boolean fileExists(String path) {
        return new File(path).exists();
    }

    public boolean removeFile(String filename) {
        return removeFile(new File(filename));
    }

    public boolean removeFile(File filename) {
        try {
            return filename.delete();
        } catch (Exception var2) {
            return false;
        }
    }
}
