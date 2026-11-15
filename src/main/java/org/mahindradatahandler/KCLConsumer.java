package org.mahindradatahandler;

import java.time.Instant;
import java.util.UUID;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.time.temporal.ChronoUnit;
import org.mahindradatahandler.config.LibConfig;
import org.mahindradatahandler.config.AwsClientConfig;
import in.intellicar.infra.lognmetricswriter.observability.*;
// import org.kinesisreader.metrics.OtelMetricsHandler;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

public class KCLConsumer {

        private Logger log;
        private Metrics metrics;

        private LibConfig config;
        private KinesisAsyncClient kinesisClient;
        private Scheduler scheduler;
        private Region region;
        private DataHandler dataHandler;
        // private OtelMetricsHandler metricsHandler;

        // Auto-restart functionality
        private final ScheduledExecutorService healthMonitor = Executors.newScheduledThreadPool(1);
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
        private final AtomicLong lastProgressTime = new AtomicLong(System.currentTimeMillis());
        private final AtomicReference<Instant> lastHealthyState = new AtomicReference<>(Instant.now());
        private volatile Thread schedulerThread;

        // Configuration for auto-restart
        private final long HEALTH_CHECK_INTERVAL_SECONDS = 60; // Check every minute
        private final long MAX_IDLE_TIME_SECONDS = 300; // 5 minutes without progress = restart
        private final long MAX_NETWORK_FAILURE_TIME_SECONDS = 120; // 2 minutes of network issues = restart
        private final int MAX_RESTART_ATTEMPTS = 5;
        private int restartAttemptCount = 0;

        public KCLConsumer(LibConfig config, DataHandler dataHandler, Logger log, Metrics metrics) {
                this.log = log;
                this.metrics = metrics;
                this.config = config;
                this.region = Region.of(config.getAwsRegion());
                this.kinesisClient = KinesisAsyncClient.builder()
                                .region(this.region)
                                .httpClient(AwsClientConfig.createHttpClient())
                                .overrideConfiguration(AwsClientConfig.createOverrideConfig())
                                .build();
                this.dataHandler = dataHandler;
                // this.metricsHandler = metricsHandler;
        }

        public void start() {
                if (isRunning.get()) {
                        this.log.warn("KCLConsumer is already running");
                        return;
                }

                this.log.info("Starting KCLConsumer with auto-restart capability");
                isShuttingDown.set(false);
                startScheduler();
                startHealthMonitoring();
        }

        private void startScheduler() {
                try {
                        DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                                        .region(region)
                                        .httpClient(AwsClientConfig.createHttpClient())
                                        .build();

                        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder()
                                        .region(region)
                                        .httpClient(AwsClientConfig.createHttpClient())
                                        .build();

                        // Create factory with reference to this consumer for auto-restart functionality
                        KCLShardRecordProcessorFactory factory = new KCLShardRecordProcessorFactory(this.config,
                                        this.dataHandler, this.log, this.metrics, this);

                        // find oldest checkpoint time from dataHandler checkpoint state
                        Instant oldestCheckpointTime = this.dataHandler != null
                                        ? this.dataHandler.getOldestShardCheckpointTime().orElse(null)
                                        : null;

                        StreamTracker streamTracker = new SingleStreamTracker(this.config.getStreamName(),
                                        InitialPositionInStreamExtended
                                                        .newInitialPosition(InitialPositionInStream.LATEST));
                        // also check if oldestCheckpointTime is not older than 6days
                        if (oldestCheckpointTime != null
                                        && oldestCheckpointTime.isAfter(Instant.now().minus(6, ChronoUnit.DAYS))) {
                                Date checkpDateTimestamp = Date.from(oldestCheckpointTime.minusSeconds(60));
                                streamTracker = new SingleStreamTracker(this.config.getStreamName(),
                                                InitialPositionInStreamExtended
                                                                .newInitialPositionAtTimestamp(checkpDateTimestamp));
                        }

                        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                                        this.config.getStreamName(),
                                        this.config.getApplicationName(),
                                        this.kinesisClient,
                                        dynamoDbAsyncClient,
                                        cloudWatchClient,
                                        UUID.randomUUID().toString(),
                                        // new KCLShardRecordProcessorFactory(this.config, this.metricsHandler));
                                        factory);
                        configsBuilder.leaseManagementConfig()
                                        .maxLeasesForWorker(15)
                                        .failoverTimeMillis(300000) // 5 minutes - maximum allowed
                                        .shardSyncIntervalMillis(60000); // Renew every minute

                        // Configure metrics
                        MetricsConfig metricsConfig = configsBuilder.metricsConfig();
                        metricsConfig.metricsLevel(MetricsLevel.NONE);
                        metricsConfig.metricsFactory(new NullMetricsFactory());

                        this.scheduler = new Scheduler(
                                        configsBuilder.checkpointConfig(),
                                        configsBuilder.coordinatorConfig(),
                                        configsBuilder.leaseManagementConfig(),
                                        configsBuilder.lifecycleConfig()
                                                        .readTimeoutsToIgnoreBeforeWarning(3),
                                        metricsConfig,
                                        configsBuilder.processorConfig(),
                                        configsBuilder.retrievalConfig()
                                                        .retrievalSpecificConfig(
                                                                        new PollingConfig(this.config.getStreamName(),
                                                                                        this.kinesisClient))
                                                        .streamTracker(streamTracker));

                        this.schedulerThread = new Thread(this.scheduler);
                        this.schedulerThread.setDaemon(true);
                        this.schedulerThread.start();

                        isRunning.set(true);
                        lastProgressTime.set(System.currentTimeMillis());
                        lastHealthyState.set(Instant.now());
                        restartAttemptCount = 0;

                        this.log.info("KCLConsumer scheduler started successfully");

                } catch (Exception e) {
                        this.log.error("Failed to start KCL scheduler", e);
                        isRunning.set(false);
                        scheduleRestart("Scheduler startup failure");
                }
        }

        private void startHealthMonitoring() {
                healthMonitor.scheduleAtFixedRate(
                                this::performHealthCheck,
                                HEALTH_CHECK_INTERVAL_SECONDS,
                                HEALTH_CHECK_INTERVAL_SECONDS,
                                TimeUnit.SECONDS);
        }

        private void performHealthCheck() {
                if (isShuttingDown.get()) {
                        return;
                }

                try {
                        // Check if scheduler thread is still alive
                        if (!isSchedulerAlive()) {
                                this.log.error("Scheduler thread is not alive");
                                scheduleRestart("Scheduler thread died");
                                return;
                        }

                        // Check for processing progress
                        if (!checkProcessingProgress()) {
                                this.log.error("No processing progress detected");
                                scheduleRestart("No processing progress");
                                return;
                        }

                        // Check for network health
                        if (!checkNetworkHealth()) {
                                this.log.error("Network health check failed");
                                scheduleRestart("Network health failure");
                                return;
                        }

                        // If we reach here, everything is healthy
                        lastHealthyState.set(Instant.now());
                        this.log.debug("Health check passed - KCL consumer is healthy");

                } catch (Exception e) {
                        this.log.error("Error during health check", e);
                }
        }

        private boolean isSchedulerAlive() {
                return schedulerThread != null && schedulerThread.isAlive() && isRunning.get();
        }

        private boolean checkProcessingProgress() {
                long currentTime = System.currentTimeMillis();

                // Update progress time based on dataHandler activity
                if (dataHandler != null && dataHandler.hasShardCheckpointTimes()) {
                        Instant latestCheckpoint = dataHandler.getLatestShardCheckpointTime().orElse(null);
                        if (latestCheckpoint != null && latestCheckpoint.isAfter(
                                        Instant.ofEpochMilli(lastProgressTime.get()))) {
                                lastProgressTime.set(latestCheckpoint.toEpochMilli());
                                return true;
                        }
                }

                // Check if we've been idle too long
                long idleTime = currentTime - lastProgressTime.get();
                if (idleTime > MAX_IDLE_TIME_SECONDS * 1000) {
                        this.log.warn("Consumer has been idle for " + idleTime / 1000 + " seconds (max: "
                                        + MAX_IDLE_TIME_SECONDS + ")");
                        return false;
                }

                return true;
        }

        private boolean checkNetworkHealth() {
                Instant now = Instant.now();
                long timeSinceHealthy = java.time.Duration.between(lastHealthyState.get(), now).getSeconds();

                if (timeSinceHealthy > MAX_NETWORK_FAILURE_TIME_SECONDS) {
                        this.log.warn("Extended period without healthy state: " + timeSinceHealthy + " seconds");
                        return false;
                }

                return true;
        }

        private void scheduleRestart(String reason) {
                if (isShuttingDown.get()) {
                        return;
                }

                if (restartAttemptCount >= MAX_RESTART_ATTEMPTS) {
                        this.log.error("Maximum restart attempts (" + MAX_RESTART_ATTEMPTS + ") reached for reason: "
                                        + reason + ". " +
                                        "Manual intervention required.");
                        isRunning.set(false);
                        return;
                }

                restartAttemptCount++;
                this.log.warn("Scheduling KCL restart #" + restartAttemptCount + " due to: " + reason);

                // Stop current scheduler
                stopScheduler();

                // Schedule restart with exponential backoff
                int delaySeconds = Math.min(60, 10 * restartAttemptCount);
                healthMonitor.schedule(() -> {
                        if (!isShuttingDown.get()) {
                                this.log.info("Attempting to restart KCL scheduler attempt- " + restartAttemptCount);
                                startScheduler();
                        }
                }, delaySeconds, TimeUnit.SECONDS);
        }

        private void stopScheduler() {
                isRunning.set(false);

                if (scheduler != null) {
                        try {
                                this.log.info("Stopping KCL scheduler for restart");
                                scheduler.shutdown();

                                // Wait for graceful shutdown
                                if (schedulerThread != null && schedulerThread.isAlive()) {
                                        schedulerThread.join(5000); // Wait up to 5 seconds
                                }

                        } catch (Exception e) {
                                this.log.error("Error stopping scheduler", e);
                        }
                }
        }

        // Method to be called when records are successfully processed
        public void recordProcessed() {
                lastProgressTime.set(System.currentTimeMillis());
                lastHealthyState.set(Instant.now());
        }

        // Method to be called when network errors are detected
        public void networkErrorDetected() {
                this.log.warn("Network error detected");
                // Don't update lastHealthyState - let the health check mechanism handle it
        }

        public void stop() {
                this.log.info("Shutting down KCLConsumer");
                isShuttingDown.set(true);
                isRunning.set(false);

                // Stop health monitoring
                healthMonitor.shutdown();
                try {
                        if (!healthMonitor.awaitTermination(10, TimeUnit.SECONDS)) {
                                healthMonitor.shutdownNow();
                        }
                } catch (InterruptedException e) {
                        healthMonitor.shutdownNow();
                        Thread.currentThread().interrupt();
                }

                // Stop scheduler
                if (this.scheduler != null) {
                        this.scheduler.shutdown();
                }

                this.log.info("KCLConsumer shutdown complete");
        }
}
