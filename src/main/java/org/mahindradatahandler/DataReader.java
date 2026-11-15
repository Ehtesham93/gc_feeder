package org.mahindradatahandler;

import org.mahindradatahandler.config.LibConfig;
import in.intellicar.infra.lognmetricswriter.observability.*;
// import org.kinesisreader.metrics.OtelMetricsHandler;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import io.prometheus.metrics.exporter.pushgateway.PushGateway;
// import io.opentelemetry.sdk.OpenTelemetrySdk;
// import io.opentelemetry.sdk.metrics.SdkMeterProvider;
// import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
// import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;

/* Created By
 * Omkar G
 * Intellicar
 * 1 July 2025
 */

public class DataReader {

    private Logger log;
    private Metrics metrics;
    private LoggerConfig logConfig;
    private LibConfig config;
    private KCLConsumer consumer;
    private DataHandler dataHandler;
    // private OtelMetricsHandler metricsHandler;

    public DataReader(LibConfig config) {
        this.config = config;
        logConfig = new LoggerConfig();
        if (this.config.getLogMetricsWriterProps().has("environment")) {
            logConfig.setEnvironment(this.config.getLogMetricsWriterProps().get("environment").asText());
        }
        if (this.config.getLogMetricsWriterProps().has("service")) {
            logConfig.setService(this.config.getLogMetricsWriterProps().get("service").asText());
        }
        if (this.config.getLogMetricsWriterProps().has("instance")) {
            logConfig.setInstance(this.config.getLogMetricsWriterProps().get("instance").asText());
        }
        String taskArn = System.getenv("TASK_ARN");
        if (taskArn != null && !taskArn.isBlank()) {
            logConfig.setInstance(taskArn);
        }
        if (this.config.getLogMetricsWriterProps().has("ip")) {
            logConfig.setIp(this.config.getLogMetricsWriterProps().get("ip").asText());
        }
        String taskIp = System.getenv("TASK_IP");
        if (taskIp != null && !taskIp.isBlank()) {
            logConfig.setIp(taskIp);
        }
        if (this.config.getLogMetricsWriterProps().has("logLevel")) {
            logConfig.setLogLevel(this.config.getLogMetricsWriterProps().get("logLevel").asText());
        }
        if (this.config.getLogMetricsWriterProps().has("logToConsole")) {
            logConfig.setLogToConsole(this.config.getLogMetricsWriterProps().get("logToConsole").asBoolean());
        }if (this.config.getLogMetricsWriterProps().has("maxSizeBytes")) {
            logConfig.setMaxSizeBytes(this.config.getLogMetricsWriterProps().get("maxSizeBytes").asInt());
        }
        if (this.config.getLogMetricsWriterProps().has("maxBackups")) {
            logConfig.setMaxBackups(this.config.getLogMetricsWriterProps().get("maxBackups").asInt());
        }
        if (this.config.getLogMetricsWriterProps().has("checkIntervalMs")) {
            logConfig.setCheckIntervalMs(this.config.getLogMetricsWriterProps().get("checkIntervalMs").asInt());
        }
        if (this.config.getLogMetricsWriterProps().has("autoInstrument")) {
            logConfig.setAutoInstrument(this.config.getLogMetricsWriterProps().get("autoInstrument").asBoolean());
        }
        if (this.config.getLogMetricsWriterProps().has("flushInterval")) {
            logConfig.setFlushInterval(this.config.getLogMetricsWriterProps().get("flushInterval").asInt());
        }
        
        this.log = new Logger(logConfig);
        this.log.startThread();
        ObservabilityBridge.setLogger(this.log);
        this.metrics = this.log.getMetrics();
        // this.metricsHandler = new OtelMetricsHandler();
    }

    public static void main(String[] args) {
        DataReader reader = new DataReader(new LibConfig());
        reader.log.info("Starting KinesisReader");

        // Add shutdown hook once, before starting the application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            reader.log.info("Received shutdown signal, stopping DataReader");
            reader.stop();
        }));

        if (reader.config.isValid()) {
            reader.dataHandler = new DataHandler(reader.config, reader.log, reader.metrics);
            reader.log.info("Starting DataReader");
            reader.start();
        } else {
            reader.log.error("Config is not valid, please check!!!");
            System.exit(1);
        }

        reader.log.info("Reader started...");
    }

    public void start() {
        this.log.info("Starting KinesisReader");
        // this.consumer = new KCLConsumer(this.config, this.metricsHandler);
        if (!this.dataHandler.initialize()) {
            this.log.error("Failed to initialize dataHandler");
            throw new RuntimeException("Failed to initialize dataHandler");
        }
        this.consumer = new KCLConsumer(this.config, this.dataHandler, this.log, this.metrics);
        this.consumer.start();
        this.log.info("KinesisReader started");
    }

    public void stop() {
        this.log.info("Stopping KinesisReader");
        if (this.consumer != null) {
            this.consumer.stop();
        }
        if (this.dataHandler != null) {
            this.dataHandler.stopHandler();
        }
        this.log.info("KinesisReader stopped");
        this.metrics.stopThread();
        this.log.stopThread();
    }

    // private void initializeOtelMetricsExporter() {
    // // Configure OTLP Metric Exporter
    // OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
    // .setEndpoint("10.178.0.230:10001")
    // .build();

    // // Set up the MeterProvider
    // SdkMeterProvider meterProvider = SdkMeterProvider.builder()
    // .registerMetricReader(PeriodicMetricReader.builder(exporter)
    // .setInterval(Duration.ofSeconds(30)) // Export every 30 seconds
    // .build())
    // .build();

    // // Register the MeterProvider globally
    // OpenTelemetrySdk.builder()
    // .setMeterProvider(meterProvider)
    // .buildAndRegisterGlobal();
    // }

    // private void initializePromMetricsHandler() {
    // PushGateway pushGatewayArg = PushGateway.builder()
    // .address("10.178.0.230:10008")
    // .job("kinesis-reader")
    // .build();
    // }
}
