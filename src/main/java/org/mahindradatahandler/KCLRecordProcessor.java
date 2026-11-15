package org.mahindradatahandler;

// import java.text.SimpleDateFormat;
// import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
// import java.util.concurrent.atomic.AtomicLong;
import org.mahindradatahandler.config.LibConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import in.intellicar.infra.lognmetricswriter.observability.*;

// import scala.annotation.bridge;
// import org.xerial.snappy.Snappy;
// import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;

// Additional imports for network error detection
import java.net.SocketTimeoutException;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

public class KCLRecordProcessor implements ShardRecordProcessor {
    private Logger log;
    private Metrics metrics;
    private LibConfig config;
    private String shardId;
    private DataHandler dataHandler;
    private KCLConsumer kclConsumer;
    private long metricsInterval;
    private long lastMetricsTime;

    public KCLRecordProcessor(LibConfig config, DataHandler dataHandler, Logger log, Metrics metrics, KCLConsumer kclConsumer) {
        this.config = config;
        this.dataHandler = dataHandler;
        this.log = log;
        this.metrics = metrics;
        this.kclConsumer = kclConsumer;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        try {
            this.shardId = initializationInput.shardId();
            this.metricsInterval = 60000;
            this.lastMetricsTime = 0;
            this.log.info("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber());
        } catch (Exception e) {
            this.log.error("Error initializing KCLRecordProcessor", e);
            handleNetworkError(e);
            throw new RuntimeException("Failed to initialize shard");
        }
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            AtomicInteger recordsProcessed = new AtomicInteger(0);
            AtomicInteger records = new AtomicInteger(0);
            dataHandler.setKinesisCheckpointer(this.shardId, processRecordsInput.checkpointer());

            // Retry processing records until pusher is alive and all records are processed
            // boolean allRecordsProcessed = false;
            // while (!allRecordsProcessed) {
                // Check if pusher is alive before processing
                while (!dataHandler.isPusherAlive.get()) {
                    this.log.info("Back pressure detected, waiting for pusher to be alive before retrying records");
                    Thread.sleep(10000);
                }

                // Reset counters for this attempt
                recordsProcessed.set(0);
                records.set(0);

                // Process records and let ClickhPusher handle checkpointing via callbacks
                processRecordsInput.records().forEach(
                        r -> {
                            try {
                                if (dataHandler.processRecord(this.shardId, r.partitionKey(), r.sequenceNumber(),
                                        r.data(),
                                        r.approximateArrivalTimestamp())) {
                                    recordsProcessed.incrementAndGet();
                                    // Signal successful processing to KCLConsumer
                                    if (kclConsumer != null) {
                                        kclConsumer.recordProcessed();
                                    }
                                }
                                records.incrementAndGet();
                            } catch (Exception e) {
                                this.log.error("Error processing record: " + e);
                                handleNetworkError(e);
                            }
                        });

                // Check if we processed all records successfully and pusher is still alive
                // (back pressure)
                if (!dataHandler.isPusherAlive.get()) {
                    this.log.info("Pusher became unavailable during processing, retrying all records");
                }
            // }

            // Continue with metrics and checkpointing only after successful processing
            if (System.currentTimeMillis() - lastMetricsTime > metricsInterval) {
                // ObjectNode metricsNode = new ObjectMapper().createObjectNode();
                // metricsNode.put("shardid", this.shardId);
                dataHandler.getMetrics(this.shardId);
                if (processRecordsInput.millisBehindLatest() != null) {
                    // metricsNode.put("records_lag", processRecordsInput.millisBehindLatest());
                    this.metrics.gauge("kcl.records_lag", dataHandler.createMap("help", "records_lag"))
                               .set(processRecordsInput.millisBehindLatest(), dataHandler.createMap("shardid", this.shardId));
                }
                // metricsNode.put("records", records.get());
                this.metrics.gauge("kcl.records", dataHandler.createMap("help", "total_records_processed"))
                               .set(records.get(), dataHandler.createMap("shardid", this.shardId));
                // metricsNode.put("records_processed", recordsProcessed.get());
                this.metrics.gauge("kcl.records_processed", dataHandler.createMap("help", "actual_records_processed"))
                               .set(recordsProcessed.get(), dataHandler.createMap("shardid", this.shardId));
                // metricsNode.put("appid", this.config.getAppId());
                // metricsNode.put("currtime", System.currentTimeMillis());
                
                lastMetricsTime = System.currentTimeMillis();
            }

            if (processRecordsInput.checkpointer() != null) {
                performCheckpointWithRetry(processRecordsInput);
            }
        } catch (Exception e) {
            this.log.error("Error processing records", e);
            handleNetworkError(e);
            throw new RuntimeException("Failed to process records", e);
        }
    }

    private void performCheckpointWithRetry(ProcessRecordsInput processRecordsInput) {
        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                processRecordsInput.checkpointer().checkpoint();
                // Signal successful checkpoint
                if (kclConsumer != null) {
                    kclConsumer.recordProcessed();
                }
                return;
            } catch (ShutdownException e) {
                this.log.warn("Lost lease for shard " + shardId + ", skipping checkpoint");
                return;
            } catch (Exception e) {
                retryCount++;
                this.log.warn("Checkpoint attempt " + retryCount + " failed for shard " + shardId + ": " + e);

                if (isNetworkError(e)) {
                    handleNetworkError(e);

                    if (retryCount < maxRetries) {
                        try {
                            Thread.sleep(1000 * retryCount); // Exponential backoff
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                } else {
                    break; // Non-network error, don't retry
                }
            }
        }

        if (retryCount >= maxRetries) {
            this.log.error("Checkpoint failed after " + maxRetries + " attempts for shard " + shardId);
        }
    }

    private void handleNetworkError(Exception e) {
        if (isNetworkError(e)) {
            this.log.warn("Network error detected in shard " + shardId + ": " + e);
            if (kclConsumer != null) {
                kclConsumer.networkErrorDetected();
            }
        }
    }

    private boolean isNetworkError(Exception e) {
        return e instanceof SocketTimeoutException ||
                e instanceof ConnectException ||
                e instanceof TimeoutException ||
                e.getCause() instanceof SocketTimeoutException ||
                e.getCause() instanceof ConnectException ||
                e.getMessage() != null && (e.getMessage().contains("timeout") ||
                        e.getMessage().contains("connection") ||
                        e.getMessage().contains("network") ||
                        e.getMessage().contains("DefaultPromise.tryFailure") ||
                        e.getMessage().contains("NettyUtils") ||
                        e.getMessage().contains("io.netty"));
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        this.log.info("Lost lease, so terminating.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            performShardEndCheckpoint(shardEndedInput);
        } catch (Exception e) {
            this.log.error("Exception while checkpointing at shard end. Giving up.", e);
            handleNetworkError(e);
            throw new RuntimeException("Failed to stop handler", e);
        }
    }

    private void performShardEndCheckpoint(ShardEndedInput shardEndedInput) {
        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                shardEndedInput.checkpointer().checkpoint();
                return;
            } catch (Exception e) {
                retryCount++;
                this.log.warn("Shard end checkpoint attempt " + retryCount + " failed: " + e);

                if (isNetworkError(e) && retryCount < maxRetries) {
                    handleNetworkError(e);
                    try {
                        Thread.sleep(1000 * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            performShutdownCheckpoint(shutdownRequestedInput);
        } catch (Exception e) {
            this.log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
            handleNetworkError(e);
        }
    }

    private void performShutdownCheckpoint(ShutdownRequestedInput shutdownRequestedInput) {
        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                shutdownRequestedInput.checkpointer().checkpoint();
                return;
            } catch (Exception e) {
                retryCount++;
                this.log.warn("Shutdown checkpoint attempt " + retryCount + " failed: " + e);

                if (isNetworkError(e) && retryCount < maxRetries) {
                    handleNetworkError(e);
                    try {
                        Thread.sleep(1000 * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    break;
                }
            }
        }
    }
}
