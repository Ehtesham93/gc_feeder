package org.mahindradatahandler;

import in.intellicar.infra.clickhousebucket.producer.BucketProducer;
import in.intellicar.infra.clickhousebucket.producer.BucketProducerCallback;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;
import in.intellicar.infra.lognmetricswriter.observability.*;
// import software.amazon.kinesis.processor.RecordProcessorCheckpointer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.TreeMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class ClickhPusher extends Thread implements BucketProducerCallback {
    // Shared, thread-safe mapper: creating an ObjectMapper per record is expensive (CPU + garbage)
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Logger log;
    private Metrics metrics;
    public final String GPS_HISTORY_TABLEID = "gpsdata";
    public final String GPS_LATEST_TABLEID = "GPS";
    public final String CAN_HISTORY_TABLEID = "candata";
    public final String CAN_LATEST_TABLEID = "CAN";
    public final String LIVENESS_LATEST_TABLEID = "LIVENESS";
    public final String RAW_REGULARDATA_ZEO_TABLEID = "RAW_REGULARDATA_ZEO";
    public final String RAW_REGULARDATA_ZEOP2_TABLEID = "RAW_REGULARDATA_ZEOP2";
    public final String RAW_REGULARDATA_TREO_TABLEID = "RAW_REGULARDATA_TREO";
    public final String RAW_REGULARDATA_TREO2163AH_TABLEID = "RAW_REGULARDATA_TREO2163AH";
    public final String RAW_REGULARDATA_A301_TABLEID = "RAW_REGULARDATA_A301";
    public final String RAW_REGULARDATA_T305_TABLEID = "RAW_REGULARDATA_T305";
    public final String RAW_REGULARDATA_T380_TABLEID = "RAW_REGULARDATA_T380";
    public final String RAW_GPSDATA_TABLEID = "RAW_GPSDATA";
    public final String RAW_SUMMARYDATA_A301_TABLEID = "RAW_SUMMARYDATA_A301";
    public final String RAW_SUMMARYDATA_COMMON_TABLEID = "RAW_SUMMARYDATA_COMMON";
    public final String RAW_ALERTDATA_TABLEID = "RAW_ALERTDATA";
    public final String RAW_TRIPALERT_TABLEID = "RAW_TRIPALERT";
    public final String RAW_CHARGEINSIGHT_TABLEID = "RAW_CHARGEINSIGHT";
    public final String RAW_TRIPINSIGHT_TABLEID = "RAW_TRIPINSIGHT";
    BucketProducer clkhProducer;
    private DataHandler dataHandler;
    public boolean isClickhInit = false;
    private AtomicBoolean isStopped = new AtomicBoolean(false);

    // Per-shard checkpoint tracking
    private HashMap<String, TreeMap<String, Boolean>> shardSequenceNumbers = new HashMap<>();
    private HashMap<String, TreeMap<String, Instant>> shardSequenceToApproxtime = new HashMap<>();
    private HashMap<String, String> lastCheckpointedSequences = new HashMap<>();
    private HashMap<String, Long> lastCheckpointTimes = new HashMap<>();
    private static final long CHECKPOINT_INTERVAL_MS = 60000; // Shard Checkpoint interval
    private static final long GLOBAL_CHECKPOINT_INTERVAL_MS = 5 * 60 * 1000; // Global Checkpoint interval
    private static long LAST_GLOBAL_CHECKPOINT_TIME = 0; // Global Checkpoint interval
    private AtomicBoolean isCheckpointTriggered = new AtomicBoolean(false);

    // Metrics
    private AtomicLong nSendClickhGpsLatest = new AtomicLong(0);
    private AtomicLong nSendClickhGpsHistory = new AtomicLong(0);
    private AtomicLong nSendClickhCanLatest = new AtomicLong(0);
    private AtomicLong nSendClickhCanHistory = new AtomicLong(0);
    private AtomicLong nSendClickhLivenessData = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataZeo = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataZeoP2 = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataTreo = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataTreo2163ah = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataA301 = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataT305 = new AtomicLong(0);
    private AtomicLong nSendClickhRawRegularDataT380 = new AtomicLong(0);
    private AtomicLong nSendClickhRawGpsData = new AtomicLong(0);
    private AtomicLong nSendClickhRawSummaryDataA301 = new AtomicLong(0);
    private AtomicLong nSendClickhRawSummaryDataCommon = new AtomicLong(0);
    private AtomicLong nSendClickhRawAlertData = new AtomicLong(0);
    private AtomicLong nSendClickhRawTripAlert = new AtomicLong(0);
    private AtomicLong nSendClickhRawChargeInsight = new AtomicLong(0);
    private AtomicLong nSendClickhRawTripInsight = new AtomicLong(0);
    private AtomicLong nSuccessCallback = new AtomicLong(0);
    private AtomicLong nFailureCallback = new AtomicLong(0);

    public ClickhPusher(String applicationName, JsonNode cassProperties, Logger log, Metrics metrics) {
        this.log = log;
        this.metrics = metrics;
        // this.config = config;
        String gpsInsertQuery = "lmmdata.gpsdata";
        String gpsLatestInsertQuery = "lmmdata_latest.gpsdatalatest";
        String canInsertQuery = "lmmdata.candata";
        String canLatestInsertQuery = "lmmdata_latest.candatalatest";
        String livenessLatestInsertQuery = "lmmdata_latest.livenessdata";
        String rawRegularDataZeoInsertQuery = "rawdata.regulardata_zeo";
        String rawRegularDataZeoP2InsertQuery = "rawdata.regulardata_zeop2";
        String rawRegularDataTreoInsertQuery = "rawdata.regulardata_treo";
        String rawRegularDataTreo2163ahInsertQuery = "rawdata.regulardata_treo2163ah";
        String rawRegularDataA301InsertQuery = "rawdata.regulardata_a301";
        String rawRegularDataT305InsertQuery = "rawdata.regulardata_t305";
        String rawRegularDataT380InsertQuery = "rawdata.regulardata_t380";
        String rawGpsDataInsertQuery = "rawdata.gpsdata";
        String rawSummaryDataA301InsertQuery = "rawdata.summarydata_a301";
        String rawSummaryDataCommonInsertQuery = "rawdata.summarydata_common";
        String rawAlertDataInsertQuery = "rawdata.alertdata";
        String rawTripAlertInsertQuery = "rawdata.tripalert";
        String rawChargeInsightInsertQuery = "rawdata.chargeinsight";
        String rawTripInsightInsertQuery = "rawdata.tripinsight";
        
        // History + latest/raw tables all use non-bucketed global inserts
        HashMap<String, String> clkglobalTablesMap = new HashMap<>();
        clkglobalTablesMap.put(GPS_HISTORY_TABLEID, gpsInsertQuery);
        clkglobalTablesMap.put(CAN_HISTORY_TABLEID, canInsertQuery);
        clkglobalTablesMap.put(GPS_LATEST_TABLEID, gpsLatestInsertQuery);
        clkglobalTablesMap.put(CAN_LATEST_TABLEID, canLatestInsertQuery);
        clkglobalTablesMap.put(LIVENESS_LATEST_TABLEID, livenessLatestInsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_ZEO_TABLEID, rawRegularDataZeoInsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_ZEOP2_TABLEID, rawRegularDataZeoP2InsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_TREO_TABLEID, rawRegularDataTreoInsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_TREO2163AH_TABLEID, rawRegularDataTreo2163ahInsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_A301_TABLEID, rawRegularDataA301InsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_T305_TABLEID, rawRegularDataT305InsertQuery);
        clkglobalTablesMap.put(RAW_REGULARDATA_T380_TABLEID, rawRegularDataT380InsertQuery);
        clkglobalTablesMap.put(RAW_GPSDATA_TABLEID, rawGpsDataInsertQuery);
        clkglobalTablesMap.put(RAW_SUMMARYDATA_A301_TABLEID, rawSummaryDataA301InsertQuery);
        clkglobalTablesMap.put(RAW_SUMMARYDATA_COMMON_TABLEID, rawSummaryDataCommonInsertQuery);
        clkglobalTablesMap.put(RAW_ALERTDATA_TABLEID, rawAlertDataInsertQuery);
        clkglobalTablesMap.put(RAW_TRIPALERT_TABLEID, rawTripAlertInsertQuery);
        clkglobalTablesMap.put(RAW_CHARGEINSIGHT_TABLEID, rawChargeInsightInsertQuery);
        clkglobalTablesMap.put(RAW_TRIPINSIGHT_TABLEID, rawTripInsightInsertQuery);

        this.clkhProducer = new BucketProducer("clickhPusher_" + applicationName, cassProperties, "lmmdata",
                0L, new HashMap<>(), clkglobalTablesMap, this);
    }

    public boolean init(DataHandler dataHandler) {
        // this.shardId = shardId;
        this.dataHandler = dataHandler;
        this.isClickhInit = this.clkhProducer.initProducer();
        return this.isClickhInit;
    }

    public void setKinesisCheckpointer(String shardid,
            software.amazon.kinesis.processor.RecordProcessorCheckpointer checkpointer) {
        if (shardid == null) {
            // log.error("shardid is null in setKinesisCheckpointer");
            return;
        }

        try {
            // Initialize per-shard tracking
            synchronized (this.shardSequenceNumbers) {
                if (!this.shardSequenceNumbers.containsKey(shardid)) {
                    this.shardSequenceNumbers.put(shardid, new TreeMap<>());
                }
            }
            synchronized (this.shardSequenceToApproxtime) {
                if (!this.shardSequenceToApproxtime.containsKey(shardid)) {
                    this.shardSequenceToApproxtime.put(shardid, new TreeMap<>());
                }
            }
            synchronized (this.lastCheckpointedSequences) {
                if (!this.lastCheckpointedSequences.containsKey(shardid)) {
                    this.lastCheckpointedSequences.put(shardid, null);
                }
            }
            synchronized (this.lastCheckpointTimes) {
                if (!this.lastCheckpointTimes.containsKey(shardid)) {
                    this.lastCheckpointTimes.put(shardid, 0L);
                }
            }
        } catch (Exception e) {
            this.log.error("Failed to initialize tracking for shard: " + shardid, e);
        }
    }

    public void sendGlobal(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        if (this.isStopped.get()) {
            this.log.info("ClickhPusher is stopped, skipping sendGlobal");
            return;
        }

        nSendClickhGpsLatest.incrementAndGet();
        ObjectNode requestContext = OBJECT_MAPPER.createObjectNode();
        requestContext.put("shardid", shardid);
        requestContext.put("sequenceNumber", sequenceNumber);

        boolean isRecordAccepted = this.clkhProducer.sendGlobalData(requestContext, valnode, GPS_LATEST_TABLEID,
                recordsize, valnode);

        if (isRecordAccepted) {
            // Track sequence number for this shard
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    shardSequences.put(sequenceNumber, false); // false means not processed yet
                    synchronized (this.shardSequenceToApproxtime) {
                        shardSequenceToApproxtime.get(shardid).put(sequenceNumber, approximateArrivalTimestamp);
                    }
                }
            }
        }
    }

    public void sendGpsHistory(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        if (this.isStopped.get()) {
            this.log.info("ClickhPusher is stopped, skipping sendGpsHistory");
            return;
        }

        nSendClickhGpsHistory.incrementAndGet();
        ObjectNode requestContext = OBJECT_MAPPER.createObjectNode();
        requestContext.put("shardid", shardid);
        requestContext.put("sequenceNumber", sequenceNumber);

        boolean isRecordAccepted = this.clkhProducer.sendGlobalData(requestContext, valnode,
                GPS_HISTORY_TABLEID, recordsize, valnode);

        if (isRecordAccepted) {
            // Track sequence number for this shard
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    shardSequences.put(sequenceNumber, false); // false means not processed yet
                    synchronized (this.shardSequenceToApproxtime) {
                        shardSequenceToApproxtime.get(shardid).put(sequenceNumber, approximateArrivalTimestamp);
                    }
                }
            }
        }
    }

    public void sendLivenessData(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        if (this.isStopped.get()) {
            this.log.info("ClickhPusher is stopped, skipping sendLivenessData");
            return;
        }
        nSendClickhLivenessData.incrementAndGet();
        ObjectNode requestContext = OBJECT_MAPPER.createObjectNode();
        requestContext.put("shardid", shardid);
        requestContext.put("sequenceNumber", sequenceNumber);

        boolean isRecordAccepted = this.clkhProducer.sendGlobalData(requestContext, valnode, LIVENESS_LATEST_TABLEID,
                recordsize, valnode);
        if (isRecordAccepted) {
            // Track sequence number for this shard
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    shardSequences.put(sequenceNumber, false); // false means not processed yet
                    synchronized (this.shardSequenceToApproxtime) {
                        shardSequenceToApproxtime.get(shardid).put(sequenceNumber, approximateArrivalTimestamp);
                    }
                }
            }
        }
    }

    private void sendRawGlobalData(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp, String tableId, AtomicLong counter, String skipLogName) {
        if (this.isStopped.get()) {
            this.log.info("ClickhPusher is stopped, skipping " + skipLogName);
            return;
        }
        counter.incrementAndGet();
        // Capture the application insertion time for every rawdata table.
        valnode.put("proctime", System.currentTimeMillis());
        ObjectNode requestContext = OBJECT_MAPPER.createObjectNode();
        requestContext.put("shardid", shardid);
        requestContext.put("sequenceNumber", sequenceNumber);

        boolean isRecordAccepted = this.clkhProducer.sendGlobalData(requestContext, valnode, tableId, recordsize,
                valnode);
        if (isRecordAccepted) {
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    shardSequences.put(sequenceNumber, false);
                    synchronized (this.shardSequenceToApproxtime) {
                        shardSequenceToApproxtime.get(shardid).put(sequenceNumber, approximateArrivalTimestamp);
                    }
                }
            }
        }
    }

    public void sendRawRegularDataZeo(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_ZEO_TABLEID, nSendClickhRawRegularDataZeo, "sendRawRegularDataZeo");
    }

    public void sendRawRegularDataZeoP2(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_ZEOP2_TABLEID, nSendClickhRawRegularDataZeoP2, "sendRawRegularDataZeoP2");
    }

    public void sendRawRegularDataTreo(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_TREO_TABLEID, nSendClickhRawRegularDataTreo, "sendRawRegularDataTreo");
    }

    public void sendRawRegularDataTreo2163ah(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_TREO2163AH_TABLEID, nSendClickhRawRegularDataTreo2163ah, "sendRawRegularDataTreo2163ah");
    }

    public void sendRawRegularDataA301(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_A301_TABLEID, nSendClickhRawRegularDataA301, "sendRawRegularDataA301");
    }

    public void sendRawRegularDataT305(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_T305_TABLEID, nSendClickhRawRegularDataT305, "sendRawRegularDataT305");
    }

    public void sendRawRegularDataT380(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
        Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_REGULARDATA_T380_TABLEID, nSendClickhRawRegularDataT380, "sendRawRegularDataT380");
    }

    public void sendRawGpsData(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_GPSDATA_TABLEID, nSendClickhRawGpsData, "sendRawGpsData");
    }

    public void sendRawSummaryDataA301(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_SUMMARYDATA_A301_TABLEID, nSendClickhRawSummaryDataA301, "sendRawSummaryDataA301");
    }

    public void sendRawSummaryDataCommon(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_SUMMARYDATA_COMMON_TABLEID, nSendClickhRawSummaryDataCommon, "sendRawSummaryDataCommon");
    }

    public void sendRawAlertData(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_ALERTDATA_TABLEID, nSendClickhRawAlertData, "sendRawAlertData");
    }

    public void sendRawTripAlert(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_TRIPALERT_TABLEID, nSendClickhRawTripAlert, "sendRawTripAlert");
    }

    public void sendRawChargeInsight(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_CHARGEINSIGHT_TABLEID, nSendClickhRawChargeInsight, "sendRawChargeInsight");
    }

    public void sendRawTripInsight(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        sendRawGlobalData(shardid, sequenceNumber, recordsize, valnode, approximateArrivalTimestamp,
                RAW_TRIPINSIGHT_TABLEID, nSendClickhRawTripInsight, "sendRawTripInsight");
    }

    public void sendCanGlobal(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        if (this.isStopped.get()) {
            this.log.info("ClickhPusher is stopped, skipping sendCanGlobal");
            return;
        }

        nSendClickhCanLatest.incrementAndGet();
        ObjectNode requestContext = OBJECT_MAPPER.createObjectNode();
        requestContext.put("shardid", shardid);
        requestContext.put("sequenceNumber", sequenceNumber);

        boolean isRecordAccepted = this.clkhProducer.sendGlobalData(requestContext, valnode, CAN_LATEST_TABLEID,
                recordsize, valnode);

        if (isRecordAccepted) {
            // Track sequence number for this shard
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    shardSequences.put(sequenceNumber, false); // false means not processed yet
                    synchronized (this.shardSequenceToApproxtime) {
                        shardSequenceToApproxtime.get(shardid).put(sequenceNumber, approximateArrivalTimestamp);
                    }
                }
            }
        }
    }

    public void sendCanHistory(String shardid, String sequenceNumber, int recordsize, ObjectNode valnode,
            Instant approximateArrivalTimestamp) {
        if (this.isStopped.get()) {
            this.log.info("ClickhPusher is stopped, skipping sendCanHistory");
            return;
        }

        nSendClickhCanHistory.incrementAndGet();
        ObjectNode requestContext = OBJECT_MAPPER.createObjectNode();
        requestContext.put("shardid", shardid);
        requestContext.put("sequenceNumber", sequenceNumber);

        boolean isRecordAccepted = this.clkhProducer.sendGlobalData(requestContext, valnode,
                CAN_HISTORY_TABLEID, recordsize, valnode);

        if (isRecordAccepted) {
            // Track sequence number for this shard
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    shardSequences.put(sequenceNumber, false); // false means not processed yet
                    synchronized (this.shardSequenceToApproxtime) {
                        shardSequenceToApproxtime.get(shardid).put(sequenceNumber, approximateArrivalTimestamp);
                    }
                }
            }
        }
    }

    public void stop(int timer) {
        this.isStopped.set(true);
        this.clkhProducer.closeProducer(timer);
        return;
    }

    @Override
    public void onSuccess(BucketProducer.ActiveRequest request) {
        try {
            nSuccessCallback.incrementAndGet();
            // log.info("ClickhPusher Batch success callback");
            // log.info("Batch success - BatchNo: {}, BatchSize: {}, IsSuccess: {}",
            // request.batchno, request.batchsize,
            // request.isSuccess);

            dataHandler.isPusherAlive.set(true);
            if (this.metrics != null) {
                this.metrics.setComponentHealth("clickhouse", true);
            }

            // Check if consumerCheckpoints is null or empty
            if (request.consumerCheckpoints == null) {
                // log.warn("consumerCheckpoints is null in onSuccess callback");
                return;
            }

            for (JsonNode eachConsumerCheckpoint : request.consumerCheckpoints) {
                if (eachConsumerCheckpoint == null) {
                    // log.warn("eachConsumerCheckpoint is null in onSuccess callback");
                    continue;
                }

                String processedSequence = eachConsumerCheckpoint.get("sequenceNumber").asText();
                String shardid = eachConsumerCheckpoint.get("shardid").asText();

                // Add null checks for debugging
                if (shardid == null) {
                    // log.error("shardid is null in onSuccess callback");
                    continue;
                }
                if (processedSequence == null) {
                    // log.error("processedSequence is null in onSuccess callback");
                    continue;
                }

                // Initialize tracking for this shard if it doesn't exist
                synchronized (this.shardSequenceNumbers) {
                    if (!shardSequenceNumbers.containsKey(shardid)) {
                        shardSequenceNumbers.put(shardid, new TreeMap<>());
                    }
                }
                synchronized (this.lastCheckpointedSequences) {
                    if (!lastCheckpointedSequences.containsKey(shardid)) {
                        lastCheckpointedSequences.put(shardid, null);
                    }
                }
                synchronized (this.lastCheckpointTimes) {
                    if (!lastCheckpointTimes.containsKey(shardid)) {
                        lastCheckpointTimes.put(shardid, System.currentTimeMillis());
                    }
                }
                synchronized (this.shardSequenceToApproxtime) {
                    if (!shardSequenceToApproxtime.containsKey(shardid)) {
                        shardSequenceToApproxtime.put(shardid, new TreeMap<>());
                    }
                }

                synchronized (this.shardSequenceNumbers) {
                    TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                    if (shardSequences != null) {
                        try {
                            if (shardSequences.containsKey(processedSequence)) {
                                shardSequences.put(processedSequence, true); // mark as processed
                            }
                        } catch (Exception e) {
                            this.log.error("Failed to put sequence " + processedSequence + " in TreeMap for shard " + shardid, e);
                        }
                    }
                }
            }

            // log.info("Triggering checkpoint from clickhouse producer");
            triggerCheckpoint();
        } catch (Exception e) {
            this.log.error("Failed to checkpoint in Kinesis", e);
        }
    }

    public void triggerCheckpoint() {
        if (!isCheckpointTriggered.compareAndSet(false, true)) {
            // log.info("Checkpoint already triggered, skipping");
            return;
        }

        try {
            List<String> shardIds;
            synchronized (this.shardSequenceNumbers) {
                shardIds = new ArrayList<>(shardSequenceNumbers.keySet());
            }

            for (String shardid : shardIds) {
                boolean hasSequences;
                synchronized (this.shardSequenceNumbers) {
                    TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                    hasSequences = shardSequences != null && !shardSequences.isEmpty();
                }
                if (!hasSequences) {
                    // log.info("sequence size 0 for shard {}", shardid);
                    continue;
                }
                Long lastCheckpointTime;
                synchronized (this.lastCheckpointTimes) {
                    lastCheckpointTime = lastCheckpointTimes.get(shardid);
                }
                if (lastCheckpointTime != null
                        && (System.currentTimeMillis() - lastCheckpointTime) >= CHECKPOINT_INTERVAL_MS
                        && !this.isStopped.get()) {
                    triggerEachCheckpoint(shardid);
                }
            }

            if ((System.currentTimeMillis() - LAST_GLOBAL_CHECKPOINT_TIME) >= GLOBAL_CHECKPOINT_INTERVAL_MS) {
                this.dataHandler.publishCheckpoint();
                LAST_GLOBAL_CHECKPOINT_TIME = System.currentTimeMillis();
            }
        } finally {
            isCheckpointTriggered.set(false);
        }
    }

    public synchronized void triggerEachCheckpoint(String shardid) {
        try {
            String sequenceToCheckpoint = null;
            Instant checkpointApproxTime = null;

            // Hold the map monitor across the whole find + cleanup: KCL threads and the
            // producer's parallel onSuccess callbacks mutate these TreeMaps concurrently,
            // so iterating outside the monitor throws ConcurrentModificationException.
            synchronized (this.shardSequenceNumbers) {
                TreeMap<String, Boolean> shardSequences = this.shardSequenceNumbers.get(shardid);
                if (shardSequences != null) {
                    // Find the longest contiguous sequence of processed records
                    sequenceToCheckpoint = findCheckpointableSequence(shardid, shardSequences);

                    if (sequenceToCheckpoint != null) {
                        synchronized (this.shardSequenceToApproxtime) {
                            TreeMap<String, Instant> approxTimes = shardSequenceToApproxtime.get(shardid);
                            if (approxTimes != null) {
                                checkpointApproxTime = approxTimes.get(sequenceToCheckpoint);
                            }
                        }
                        // Clean up all sequences up to and including checkpointed sequence
                        cleanupProcessedSequences(shardid, shardSequences, sequenceToCheckpoint);
                    }
                }
            }

            // Checkpoint if we have a valid sequence and enough time has passed
            if (sequenceToCheckpoint != null) {
                this.dataHandler.recordShardCheckpointTime(shardid, checkpointApproxTime);
                synchronized (this.lastCheckpointedSequences) {
                    this.lastCheckpointedSequences.put(shardid, sequenceToCheckpoint);
                }
                synchronized (this.lastCheckpointTimes) {
                    this.lastCheckpointTimes.put(shardid, System.currentTimeMillis());
                }

                this.log.info("Successfully checkpointed in Kinesis for shard " + shardid + " at sequence number: " + sequenceToCheckpoint);
            }
        } catch (Exception e) {
            log.error("Failed to checkpoint in Kinesis", e);
        }
    }

    /** Caller must hold the monitor of {@code this.shardSequenceNumbers}. */
    private String findCheckpointableSequence(String shardid, TreeMap<String, Boolean> shardSequences) {
        String lastCheckpointed;
        synchronized (this.lastCheckpointedSequences) {
            lastCheckpointed = lastCheckpointedSequences.get(shardid);
        }
        String lastContiguous = null;
        String lastUsableUnprocessed = null;

        for (Map.Entry<String, Boolean> entry : shardSequences.entrySet()) {
            String currentSequence = entry.getKey();
            Boolean isProcessed = entry.getValue();

            // Only consider sequences that are greater than the last checkpointed sequence
            if (lastCheckpointed != null && currentSequence.compareTo(lastCheckpointed) <= 0) {
                continue; // Skip sequences that are already checkpointed or older
            }

            if (lastUsableUnprocessed == null) {
                lastUsableUnprocessed = currentSequence;
            }

            // If the sequence is processed, update the last contiguous sequence and keep
            // going until we find an unprocessed sequence
            if (isProcessed) {
                lastContiguous = currentSequence;
            } else {
                if (lastContiguous != null) {
                    break;
                }
            }
        }

        // Return the sequence only if it's different from the last checkpointed and
        // valid
        return lastContiguous != null &&
                (lastCheckpointed == null || lastContiguous.compareTo(lastCheckpointed) > 0) ? lastContiguous
                        : lastUsableUnprocessed;
    }

    /** Caller must hold the monitor of {@code this.shardSequenceNumbers}. */
    private void cleanupProcessedSequences(String shardid, TreeMap<String, Boolean> shardSequences,
            String upToSequence) {
        Iterator<Map.Entry<String, Boolean>> iterator = shardSequences.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Boolean> entry = iterator.next();
            if (entry.getKey().compareTo(upToSequence) <= 0) {
                iterator.remove();
                synchronized (this.shardSequenceToApproxtime) {
                    TreeMap<String, Instant> approxTimes = shardSequenceToApproxtime.get(shardid);
                    if (approxTimes != null) {
                        approxTimes.remove(entry.getKey());
                    }
                }
            } else {
                break;
            }
        }
    }

    @Override
    public void onFailure(BucketProducer.ActiveRequest request, Throwable t) {
        try {
            nFailureCallback.incrementAndGet();
            this.log.error("ClickhPusher Batch failure callback");
            this.log.error("Batch failure - BatchNo: " + request.batchno + " Cause: " + t, t);

            // NOTE: onFailure now means the library permanently dropped this batch after
            // exhausting retries (transient failures are retried internally and produce
            // backpressure by blocking sendData). Do NOT park the consumer by setting
            // isPusherAlive=false here - with no batches left in flight nothing would ever
            // flip it back to true, which used to freeze the feeder until redeploy.

            // Check if consumerCheckpoints is null or empty
            if (request.consumerCheckpoints == null) {
                // log.warn("consumerCheckpoints is null in onFailure callback");
                return;
            }

            for (JsonNode eachConsumerCheckpoint : request.consumerCheckpoints) {
                if (eachConsumerCheckpoint == null) {
                    // log.warn("eachConsumerCheckpoint is null in onFailure callback");
                    continue;
                }

                String processedSequence = eachConsumerCheckpoint.get("sequenceNumber").asText();
                String shardid = eachConsumerCheckpoint.get("shardid").asText();

                // Add null checks for debugging
                if (shardid == null) {
                    this.log.error("shardid is null in onFailure callback");
                    continue;
                }
                if (processedSequence == null) {
                    this.log.error("processedSequence is null in onFailure callback");
                    continue;
                }

                // Initialize tracking for this shard if it doesn't exist
                synchronized (this.shardSequenceNumbers) {
                    if (!shardSequenceNumbers.containsKey(shardid)) {
                        this.shardSequenceNumbers.put(shardid, new TreeMap<>());
                    }
                }
                synchronized (this.shardSequenceToApproxtime) {
                    if (!shardSequenceToApproxtime.containsKey(shardid)) {
                        this.shardSequenceToApproxtime.put(shardid, new TreeMap<>());
                    }
                }

                synchronized (this.shardSequenceNumbers) {
                    TreeMap<String, Boolean> shardSequences = shardSequenceNumbers.get(shardid);
                    if (shardSequences != null) {
                        try {
                            if (shardSequences.containsKey(processedSequence)) {
                                shardSequences.remove(processedSequence); // remove the sequence from the map
                                synchronized (this.shardSequenceToApproxtime) {
                                    shardSequenceToApproxtime.get(shardid).remove(processedSequence);
                                }
                            }
                        } catch (Exception e) {
                            this.log.error("Failed to remove sequence " + processedSequence + " from TreeMap for shard " + shardid,
                                    e);
                        }
                    } else {
                        this.log.warn("shardSequences is null for shard: " + shardid + " in failure");
                    }
                }
            }
        } catch (Exception e) {
            this.log.error("Failed to checkpoint in Kinesis", e);
        }
    }

    public boolean getMetrics(String shardId) {
        // ObjectMapper objectMapper = new ObjectMapper();
        // ObjectNode objectNode = objectMapper.createObjectNode();
        // objectNode.put("nSendClickhGpsLatest", nSendClickhGpsLatest.get());
        this.metrics.gauge("clickhHandler.nSendClickhGpsLatest", dataHandler.createMap("help", "total_send_clickh_gps_latest"))
                               .set(nSendClickhGpsLatest.get(), new HashMap<>());
        // objectNode.put("nSendClickhGpsHistory", nSendClickhGpsHistory.get());
        this.metrics.gauge("clickhHandler.nSendClickhGpsHistory", dataHandler.createMap("help", "total_send_clickh_gps_history"))
                               .set(nSendClickhGpsHistory.get(), new HashMap<>());
        // objectNode.put("nSendClickhCanLatest", nSendClickhCanLatest.get());
        this.metrics.gauge("clickhHandler.nSendClickhCanLatest", dataHandler.createMap("help", "total_send_clickh_can_latest"))
                               .set(nSendClickhCanLatest.get(), new HashMap<>());
        // objectNode.put("nSendClickhCanHistory", nSendClickhCanHistory.get());
        this.metrics.gauge("clickhHandler.nSendClickhCanHistory", dataHandler.createMap("help", "total_send_clickh_can_history"))
                               .set(nSendClickhCanHistory.get(), new HashMap<>());
        // objectNode.put("nSendClickhLivenessData", nSendClickhLivenessData.get());
        this.metrics.gauge("clickhHandler.nSendClickhLivenessData", dataHandler.createMap("help", "total_send_clickh_liveness_data"))
                               .set(nSendClickhLivenessData.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataZeo", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_zeo"))
                               .set(nSendClickhRawRegularDataZeo.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataZeoP2", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_zeop2"))
                               .set(nSendClickhRawRegularDataZeoP2.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataTreo", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_treo"))
                               .set(nSendClickhRawRegularDataTreo.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataTreo2163ah", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_treo2163ah"))
                               .set(nSendClickhRawRegularDataTreo2163ah.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataA301", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_a301"))
                               .set(nSendClickhRawRegularDataA301.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataT305", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_t305"))
                               .set(nSendClickhRawRegularDataT305.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawRegularDataT380", dataHandler.createMap("help", "total_send_clickh_raw_regular_data_t380"))
                               .set(nSendClickhRawRegularDataT380.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawGpsData", dataHandler.createMap("help", "total_send_clickh_raw_gpsdata"))
                               .set(nSendClickhRawGpsData.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawSummaryDataA301", dataHandler.createMap("help", "total_send_clickh_raw_summarydata_a301"))
                               .set(nSendClickhRawSummaryDataA301.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawSummaryDataCommon", dataHandler.createMap("help", "total_send_clickh_raw_summarydata_common"))
                               .set(nSendClickhRawSummaryDataCommon.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawAlertData", dataHandler.createMap("help", "total_send_clickh_raw_alertdata"))
                               .set(nSendClickhRawAlertData.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawTripAlert", dataHandler.createMap("help", "total_send_clickh_raw_tripalert"))
                               .set(nSendClickhRawTripAlert.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawChargeInsight", dataHandler.createMap("help", "total_send_clickh_raw_chargeinsight"))
                               .set(nSendClickhRawChargeInsight.get(), new HashMap<>());
        this.metrics.gauge("clickhHandler.nSendClickhRawTripInsight", dataHandler.createMap("help", "total_send_clickh_raw_tripinsight"))
                               .set(nSendClickhRawTripInsight.get(), new HashMap<>());
        // objectNode.put("nSuccessCallback", nSuccessCallback.get());
        this.metrics.gauge("clickhHandler.nSuccessCallback", dataHandler.createMap("help", "total_success_callback"))
                               .set(nSuccessCallback.get(), new HashMap<>());
        // objectNode.put("nFailureCallback", nFailureCallback.get());
        this.metrics.gauge("clickhHandler.nFailureCallback", dataHandler.createMap("help", "total_failure_callback"))
                               .set(nFailureCallback.get(), new HashMap<>());

        return true;    
    }

}
