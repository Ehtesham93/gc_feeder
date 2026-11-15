package org.mahindradatahandler;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.codehaus.jackson.JsonNode;
import java.time.LocalTime;
import java.time.LocalDate;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.mahindradatahandler.config.LibConfig;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import in.intellicar.infra.lognmetricswriter.observability.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.ArrayList;
import java.util.Optional;

/* Created By
 * Omkar G
 * Intellicar
 * 1 July 2025
 */

public class DataHandler extends Thread {
        private Logger log;
        private Metrics metrics;

        private LibConfig config;
        // private String shardId;
        private DataCheckp checkpoint;
        private AbsrpLatestPublisher absrpLtstPublisher;
        private ClickhPusher clickhPusher;
        public AtomicBoolean isPusherAlive = new AtomicBoolean(true);
        private AtomicBoolean isStopped = new AtomicBoolean(false);
        private VehicleParamFetch vehicleParamFetch;
        public Map<String, Instant> lastShardIdCheckpointTime = new ConcurrentHashMap<>();

        // Metrics
        private AtomicLong nMsgsReceived = new AtomicLong(0);
        private AtomicLong nMsgsProcessed = new AtomicLong(0);
        private AtomicLong nMsgsNull = new AtomicLong(0);
        private AtomicLong nMsgsBufferRewound = new AtomicLong(0);
        private AtomicLong nMsgsZeroDataBuf = new AtomicLong(0);
        private AtomicLong nMsgsEmptyData = new AtomicLong(0);
        private AtomicLong nMsgsInvalidVin = new AtomicLong(0);
        private AtomicLong nMsgsVinMismatch = new AtomicLong(0);
        private AtomicLong nMsgsT380Check = new AtomicLong(0);
        private AtomicLong nMsgsVinNotFound = new AtomicLong(0);
        private AtomicLong nMsgsNullVehicleParamMeta = new AtomicLong(0);
        private AtomicLong nMsgsVehicleParamMetaNotFound = new AtomicLong(0);
        private AtomicLong nMsgsNullVehicleOdoProcMeta = new AtomicLong(0);
        private AtomicLong nMsgsVehicleOdoProcMetaNotFound = new AtomicLong(0);
        private AtomicLong nMsgsProcessingError = new AtomicLong(0);
        private AtomicLong nGpsMsgsInvalidTimestamp = new AtomicLong(0);
        private AtomicLong nGpsMsgsOutOfRange = new AtomicLong(0);
        private AtomicLong nRegulardataParamMetaNotFound = new AtomicLong(0);
        private AtomicLong nCanMsgsInvalidTimestamp = new AtomicLong(0);
        private AtomicLong nCanMsgsOutOfRange = new AtomicLong(0);
        private AtomicLong nMsgsCanOutOfOrder = new AtomicLong(0);
        private AtomicLong nMsgsInvalidRecordType = new AtomicLong(0);
        private AtomicLong nMsgsNoRecordType = new AtomicLong(0);
        private AtomicLong nCanMsgsCheckpUpdateFailed = new AtomicLong(0);
        private AtomicLong nMsgsCanZeroOdo = new AtomicLong(0);
        private AtomicLong nCanMsgsProcessingError = new AtomicLong(0);
        private AtomicLong nCanMsgsSeqNumOutOfOrder = new AtomicLong(0);
        private AtomicLong nMsgsMaxOOOBeacons = new AtomicLong(0);
        private AtomicLong nCanMsgsBufferCheckError = new AtomicLong(0);
        private AtomicLong nCheckpToRedisStopped = new AtomicLong(0);
        private AtomicLong nCheckpToRedisSkipped = new AtomicLong(0);
        private AtomicLong nCheckpToRedisError = new AtomicLong(0);
        private AtomicLong nGpsLivenessInvalidTimestamp = new AtomicLong(0);
        private AtomicLong nGpsLivenessOutOfDate = new AtomicLong(0);
        private AtomicLong nGpsLivenessProcessingError = new AtomicLong(0);
        private AtomicLong nCanLivenessInvalidTimestamp = new AtomicLong(0);
        private AtomicLong nCanLivenessProcessingError = new AtomicLong(0);
        private AtomicLong nMsgsGps = new AtomicLong(0);
        private AtomicLong nMsgsCan = new AtomicLong(0);
        private AtomicLong nSendClickh = new AtomicLong(0);
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
        private AtomicLong nRawDataWriteError = new AtomicLong(0);

        private static final int VEH_DATA_CHECKP_FETCH_INTERVAL = 3 * 60000;
        private static final int MAX_BEACON_TO_REMOVE_OUT_OF_ORDER = 30;

        private ArrayList<String> T380checkvins = new ArrayList<String>(Arrays.asList("MA1AR2ZA7S3L00001",
                        "MA1AR2ZA7S3L00002", "MA1AR2ZA7S3L00003", "MA1AR2ZA7S3L00004", "MA1AS2ZA7S3L00011",
                        "MA1AR2ZA7S3L00005", "MA1AS2ZA7S3L00013", "MA1AS2ZA7S3L00014", "MA1AR2ZA7S3L00006",
                        "MA1AR2ZA7S3L00007", "MA1AS2ZA7S3L00015", "MA1AR2ZA7S3L00008", "MA1AR2ZA7S3L00009",
                        "MA1AR2ZA7S3L00010"));

        public DataHandler(LibConfig config, Logger log, Metrics metrics) {
                this.config = config;
                this.log = log;
                this.metrics = metrics;
                this.clickhPusher = new ClickhPusher(this.config.getApplicationName(),
                                this.config.getClickhousePubProps(), this.log, this.metrics);
                this.vehicleParamFetch = new VehicleParamFetch(this.config.getMetaDbProps(), this.log);
                this.checkpoint = new DataCheckp();
                this.absrpLtstPublisher = new AbsrpLatestPublisher(this.config.getApplicationName(),
                                this.config.getRedisPubProps().get("host").asText(),
                                this.config.getRedisPubProps().get("port").asInt(), this.clickhPusher, this.checkpoint,
                                this.log, this.metrics, this.vehicleParamFetch);
                // Set up callbacks
                this.absrpLtstPublisher.connect(new AbsrpLatestPublisher.ConnectionCallback() {
                        @Override
                        public void onConnect() {
                                log.info("Connected to Redis");
                        }

                        @Override
                        public void onDisconnect() {
                                log.info("Disconnected from Redis");
                        }

                        @Override
                        public void onConnectionFailure(Throwable cause) {
                                log.error("Failed to connect: " + cause.getMessage());
                        }
                });
        }

        public boolean initialize() {
                // this.shardId = shardId;
                if (this.checkpoint.getCanDataRecord() == null) {
                        this.checkpoint.setCanDataRecord(new HashMap<String, DataCheckp.CanDataRecord>());
                }
                if (this.checkpoint.getCanCheckpoints() == null) {
                        this.checkpoint.setCanCheckpoints(new ConcurrentHashMap<String, DataCheckp.CanData>());
                }
                if (!this.vehicleParamFetch.init()) {
                        this.log.error("Failed to initialize vehicleParamFetch");
                        return false;
                }
                if (!this.vehicleParamFetch.fetchParamMetaNow()) {
                        this.log.error("Failed to fetch param meta now");
                        return false;
                }
                this.absrpLtstPublisher.startThread(this);
                if (!this.absrpLtstPublisher.getAndUpdateCheckp("initialization")) {
                        this.log.error("Failed to initialize absrpLtstPublisher and getAndUpdateCheckp");
                        return false;
                }
                if (!this.absrpLtstPublisher.getAndUpdateCheckpAllvin()) {
                        log.error("Failed to initialize absrpLtstPublisher and getAndUpdateCheckpAllvin");
                        return false;
                }
                this.vehicleParamFetch.start();
                return this.clickhPusher.init(this);
        }

        public void setKinesisCheckpointer(String shardid,
                        software.amazon.kinesis.processor.RecordProcessorCheckpointer checkpointer) {
                this.clickhPusher.setKinesisCheckpointer(shardid, checkpointer);
        }

        public boolean processRecord(String shardid, String partitionKey, String sequenceNumber, ByteBuffer databuff,
                        Instant approximateArrivalTimestamp) {
                try {
                        nMsgsReceived.incrementAndGet();

                        // Check if ByteBuffer has any data
                        if (databuff == null ) {
                                nMsgsNull.incrementAndGet();
                                this.log.warn("Empty or null data received; partitionKey: " + partitionKey
                                                + " sequenceNumber: " + sequenceNumber );
                                return false;
                        }

                        ByteBuffer data = databuff.duplicate();

                        // Check if ByteBuffer has any data
                        if (data.remaining() == 0 && data.capacity() > 0) {
                                data.rewind();
                                nMsgsBufferRewound.incrementAndGet();
                                this.log.warn("Buffer is consumed. Rewond. partitionKey: " + partitionKey
                                                + " sequenceNumber: " + sequenceNumber);
                                // return false; // we are rewinding the buffer to avoid losing data
                        }

                        if (data.remaining() == 0) {
                                nMsgsZeroDataBuf.incrementAndGet();
                                this.log.warn("Buffer is actually empty. partitionKey: " + partitionKey
                                                + " sequenceNumber: " + sequenceNumber);
                                return false;
                        }

                        byte[] bytes = new byte[data.remaining()];
                        data.get(bytes);
                        String dataString = new String(bytes, StandardCharsets.UTF_8);

                        // Check if dataString is empty or null before parsing
                        if (dataString == null || dataString.trim().isEmpty()) {
                                nMsgsEmptyData.incrementAndGet();
                                this.log.warn("Empty data string after conversion; partitionKey: " + partitionKey
                                                + " sequenceNumber: " + sequenceNumber);
                                return false;
                        }

                        JsonNode dataNode = new ObjectMapper().readTree(dataString);

                        if (dataNode.has("vin")) {
                                if (dataNode.get("vin").asText().equals("") || partitionKey.equals("")) {
                                        // log.info("vin or partitionKey is empty");
                                        nMsgsInvalidVin.incrementAndGet();
                                        return false;
                                }

                                if (!dataNode.get("vin").asText().equals(partitionKey)) {
                                        // log.info("partitionKey: {}, vin: {} mismatch", partitionKey,
                                        // dataNode.get("vin").asText());
                                        nMsgsVinMismatch.incrementAndGet();
                                        return false;
                                }
                                if (T380checkvins.contains(partitionKey)) {
                                        nMsgsT380Check.incrementAndGet();
                                        // this.log.info("T380 check VIN in data: " + partitionKey + ", data: "
                                        //                 + dataString);
                                }
                        } else {
                                this.log.info("vin not found in data");
                                nMsgsVinNotFound.incrementAndGet();
                                return false;
                        }

                        // Persist the raw beacon before any validation so it is never lost to a
                        // downstream processing error. Failures here must never block processing.
                        writeRawData(shardid, sequenceNumber, partitionKey, dataNode, dataString.length(),
                                        approximateArrivalTimestamp);

                        // {regulardata: {v_mode: "ess_bcm_vehicle_mode_act_enum"}} or null
                        HashMap<String, HashMap<String, String>> vehicleParamMeta = this.vehicleParamFetch
                                        .getVehicleParamMeta(partitionKey);
                        if (vehicleParamMeta == null) {
                                // this.log.warn("Vehicle param meta is null for vin: " + partitionKey);
                                nMsgsNullVehicleParamMeta.incrementAndGet();
                                return false;
                        } else if (!vehicleParamMeta.containsKey("regulardata")) {
                                // this.log.warn("Vehicle param meta is not found for vin: " + partitionKey);
                                nMsgsVehicleParamMetaNotFound.incrementAndGet();
                                return false;
                        }
                        // {regulardata: {v_mode: true}} or null
                        HashMap<String, HashMap<String, Boolean>> vehicleOdoProcMeta = this.vehicleParamFetch
                                        .getVehicleOdoProcMeta(partitionKey);
                        if (vehicleOdoProcMeta == null) {
                                this.log.warn("Vehicle odo proc meta is null for vin: " + partitionKey);
                                nMsgsNullVehicleOdoProcMeta.incrementAndGet();
                                return false;
                        } else if (!vehicleOdoProcMeta.containsKey("regulardata")) {
                                this.log.warn("Vehicle odo proc meta is not found for vin: " + partitionKey);
                                nMsgsVehicleOdoProcMetaNotFound.incrementAndGet();
                                return false;
                        }

                        return parseData(shardid, sequenceNumber, partitionKey, dataNode, vehicleParamMeta,
                                        vehicleOdoProcMeta,
                                        approximateArrivalTimestamp);
                } catch (Exception e) {
                        this.log.error("Error parsing data; partitionKey: " + partitionKey + " sequenceNumber: "
                                        + sequenceNumber + " error: " + e);
                        nMsgsProcessingError.incrementAndGet();
                        return false;
                }
        }

        private void writeRawData(String shardid, String sequenceNumber, String vin, JsonNode dataNode,
                        int recordsize, Instant approximateArrivalTimestamp) {
                try {
                        String recordType = dataNode.has("record_type") ? dataNode.get("record_type").asText() : "";
                        String recordTypeLower = recordType.toLowerCase();
                        String model = dataNode.has("model") ? dataNode.get("model").asText() : "";
                        String modelLower = model.toLowerCase();

                        ObjectNode dstNode = new ObjectMapper().createObjectNode();
                        if (recordTypeLower.equals("regulardata")) {
                                if (modelLower.equals("ejeetop2")) {
                                        // Stream model eJeetoP2 -> rawdata.regulardata_zeop2
                                        int preparedRecordSize = DataPrepare.prepareRegularDataZeoP2Data(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataZeoP2(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataZeoP2.incrementAndGet();
                                } else if (modelLower.equals("ejeeto")) {
                                        int preparedRecordSize = DataPrepare.prepareRegularDataZeoData(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataZeo(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataZeo.incrementAndGet();
                                } else if (modelLower.equals("treo2_163ah")) {
                                        // Stream model Treo2_163ah -> rawdata.regulardata_treo2163ah
                                        int preparedRecordSize = DataPrepare.prepareRegularDataTreo2163ahData(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataTreo2163ah(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataTreo2163ah.incrementAndGet();
                                } else if (modelLower.equals("treo2")) {
                                        // Beacon model is treo2; maps to rawdata.regulardata_treo
                                        int preparedRecordSize = DataPrepare.prepareRegularDataTreoData(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataTreo(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataTreo.incrementAndGet();
                                } else if (modelLower.equals("a301")) {
                                        int preparedRecordSize = DataPrepare.prepareRegularDataA301Data(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataA301(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataA301.incrementAndGet();
                                } else if (modelLower.equals("t305")) {
                                        int preparedRecordSize = DataPrepare.prepareRegularDataT305Data(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataT305(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataT305.incrementAndGet();
                                } else if (modelLower.equals("t380")) {
                                        int preparedRecordSize = DataPrepare.prepareRegularDataT380Data(dataNode, dstNode);
                                        this.clickhPusher.sendRawRegularDataT380(shardid, sequenceNumber, preparedRecordSize,
                                                        dstNode, approximateArrivalTimestamp);
                                        nSendClickhRawRegularDataT380.incrementAndGet();
                                }
                        } else if (recordTypeLower.equals("gpsdata")) {
                                int preparedRecordSize = DataPrepare.prepareGpsData(dataNode, dstNode);
                                this.clickhPusher.sendRawGpsData(shardid, sequenceNumber, preparedRecordSize, dstNode,
                                                approximateArrivalTimestamp);
                                nSendClickhRawGpsData.incrementAndGet();
                        } else if (recordTypeLower.equals("summarydata_a301")) {
                                int preparedRecordSize = DataPrepare.prepareSummaryDataA301(dataNode, dstNode);
                                this.clickhPusher.sendRawSummaryDataA301(shardid, sequenceNumber, preparedRecordSize,
                                                dstNode, approximateArrivalTimestamp);
                                nSendClickhRawSummaryDataA301.incrementAndGet();
                        } else if (recordTypeLower.equals("summarydata")) {
                                int preparedRecordSize = DataPrepare.prepareSummaryDataCommon(dataNode, dstNode);
                                this.clickhPusher.sendRawSummaryDataCommon(shardid, sequenceNumber, preparedRecordSize,
                                                dstNode, approximateArrivalTimestamp);
                                nSendClickhRawSummaryDataCommon.incrementAndGet();
                        } else if (recordTypeLower.equals("alertdata")) {
                                int preparedRecordSize = DataPrepare.prepareAlertData(dataNode, dstNode);
                                this.clickhPusher.sendRawAlertData(shardid, sequenceNumber, preparedRecordSize, dstNode,
                                                approximateArrivalTimestamp);
                                nSendClickhRawAlertData.incrementAndGet();
                        } else if (recordTypeLower.equals("trip_alert")) {
                                int preparedRecordSize = DataPrepare.prepareTripAlertData(dataNode, dstNode);
                                this.clickhPusher.sendRawTripAlert(shardid, sequenceNumber, preparedRecordSize, dstNode,
                                                approximateArrivalTimestamp);
                                nSendClickhRawTripAlert.incrementAndGet();
                        } else if (recordTypeLower.equals("charge_insight")) {
                                int preparedRecordSize = DataPrepare.prepareChargeInsightData(dataNode, dstNode);
                                this.clickhPusher.sendRawChargeInsight(shardid, sequenceNumber, preparedRecordSize,
                                                dstNode, approximateArrivalTimestamp);
                                nSendClickhRawChargeInsight.incrementAndGet();
                        } else if (recordTypeLower.equals("trip_insight")) {
                                int preparedRecordSize = DataPrepare.prepareTripInsightData(dataNode, dstNode);
                                this.clickhPusher.sendRawTripInsight(shardid, sequenceNumber, preparedRecordSize,
                                                dstNode, approximateArrivalTimestamp);
                                nSendClickhRawTripInsight.incrementAndGet();
                        }
                } catch (Exception e) {
                        nRawDataWriteError.incrementAndGet();
                        this.log.error("Error writing raw data to clickhouse; vin: " + vin
                                        + " sequenceNumber: " + sequenceNumber + " error: " + e);
                }
        }

        public long convertToEpochMillis(String dateTimeStr) {
                try {
                        // First check if we have a simple time format like "0:14:21"
                        if (dateTimeStr.matches("\\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                                DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("H:m:s");
                                LocalTime time = LocalTime.parse(dateTimeStr, timeFormatter);
                                LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), time);
                                return dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                        }

                        // Default format for full datetime strings
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, formatter);
                        return dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                } catch (Exception e) {
                        this.log.error("Error parsing datetime string: " + dateTimeStr + " " + e);
                        // log.error("Error parsing datetime string: " + dateTimeStr, e);
                        return 0;
                }
        }

        private boolean parseData(String shardid, String sequenceNumber, String partitionKey, JsonNode dataNode,
                        HashMap<String, HashMap<String, String>> vehicleParamMeta,
                        HashMap<String, HashMap<String, Boolean>> vehicleOdoProcMeta,
                        Instant approximateArrivalTimestamp) {
                // log.info("shardId: {}, partitionKey: {}, data: {}", shardId, partitionKey,
                // dataNode);
                if (dataNode.has("record_type")) {
                        String recordType = dataNode.get("record_type").asText();
                        if (recordType.equals("Gpsdata")) {
                                DataCheckp.GpsData gpsData = checkpoint.getGpsCheckpoint(partitionKey);
                                gpsData.setVin(partitionKey);
                                gpsData.setPackettype(dataNode.has("packet_type") ? dataNode.get("packet_type").asText()
                                                : "");
                                if (convertToEpochMillis(dataNode.get("tgu_timestamp").asText()) == 0
                                                || convertToEpochMillis(dataNode.get("intime").asText()) == 0
                                                || convertToEpochMillis(dataNode.get("gps_timestamp").asText()) == 0) {
                                        this.log.info("Invalid timestamp");
                                        nGpsMsgsInvalidTimestamp.incrementAndGet();
                                        return false;
                                }
                                gpsData.setUtctime(convertToEpochMillis(dataNode.get("tgu_timestamp").asText()));
                                gpsData.setIntime(convertToEpochMillis(dataNode.get("intime").asText()));
                                gpsData.setProctime(System.currentTimeMillis());
                                gpsData.setGpstime(convertToEpochMillis(dataNode.get("gps_timestamp").asText()));

                                if (gpsData.getUtctime() < (gpsData.getProctime() - (30 * 24 * 60 * 60 * 1000L))
                                                || gpsData.getUtctime() > (gpsData.getProctime() + 600000L)) {
                                        // log.info("gpsData.utctime is out of range");
                                        nGpsMsgsOutOfRange.incrementAndGet();
                                        return false;
                                }

                                boolean isGpsProcessedForLiveness = parseGpsDataForLiveness(shardid, sequenceNumber,
                                                partitionKey, dataNode, approximateArrivalTimestamp);

                                boolean isGpsProcessed = parseGpsData(shardid, sequenceNumber, partitionKey, dataNode,
                                                gpsData, approximateArrivalTimestamp);

                                return isGpsProcessedForLiveness && isGpsProcessed;

                        } else if (recordType.equals("Regulardata")) {
                                HashMap<String, String> regulardataParamMeta = vehicleParamMeta.get("regulardata");
                                HashMap<String, Boolean> regulardataOdoProcMeta = vehicleOdoProcMeta.get("regulardata");
                                if (regulardataParamMeta == null) {
                                        // log.info("regulardata param meta not found for vin: {}", partitionKey);
                                        nRegulardataParamMetaNotFound.incrementAndGet();
                                        return false;
                                }

                                DataCheckp.CanData canData = checkpoint.getCanCheckpoint(partitionKey);
                                canData.setVin(partitionKey);
                                canData.setPackettype(dataNode.has("packet_type") ? dataNode.get("packet_type").asText()
                                                : "");
                                if (convertToEpochMillis(dataNode.get("tgu_timestamp").asText()) == 0
                                                || convertToEpochMillis(dataNode.get("intime").asText()) == 0) {
                                        // log.info("Invalid timestamp");
                                        nCanMsgsInvalidTimestamp.incrementAndGet();
                                        return false;
                                }
                                canData.setUtctime(convertToEpochMillis(dataNode.get("tgu_timestamp").asText()));
                                canData.setIntime(convertToEpochMillis(dataNode.get("intime").asText()));
                                canData.setProctime(System.currentTimeMillis());

                                if (canData.getUtctime() < (canData.getProctime() - (30 * 24 * 60 * 60 * 1000L))
                                                || canData.getUtctime() > (canData.getProctime() + 600000L)) {
                                        // log.info("canData.utctime is out of range");
                                        nCanMsgsOutOfRange.incrementAndGet();
                                        return false;
                                }

                                if (checkpoint.getCanDataRecord().get(partitionKey) != null
                                                && checkpoint.getCanDataRecord().get(partitionKey).getLastData() != null
                                                && checkpoint.getCanDataRecord().get(partitionKey).getLastData()
                                                                .getUtctime() > 0
                                                && canData.getUtctime() < checkpoint.getCanDataRecord()
                                                                .get(partitionKey)
                                                                .getLastData().getUtctime()) {
                                        // log.info("gpsData.utctime is less than lastData.utctime");
                                        nMsgsCanOutOfOrder.incrementAndGet();
                                        return false;
                                }

                                boolean isCanProcessedForLiveness = parseCanDataForLiveness(shardid, sequenceNumber,
                                                partitionKey, dataNode, approximateArrivalTimestamp);

                                boolean isCanProcessed = parseCanData(shardid, sequenceNumber, partitionKey, dataNode,
                                                canData, regulardataParamMeta, regulardataOdoProcMeta,
                                                approximateArrivalTimestamp);

                                return isCanProcessedForLiveness && isCanProcessed;

                        } else {
                                nMsgsInvalidRecordType.incrementAndGet();
                                // TODO: we still have to insert into respective rawdata tables
                                // this.log.info("Invalid record type: " + recordType);
                                return false;
                        }
                }
                nMsgsNoRecordType.incrementAndGet();
                this.log.info("No record type found");
                return false;
        }

        private boolean parseGpsData(String shardid, String sequenceNumber, String partitionKey, JsonNode dataNode,
                        DataCheckp.GpsData gpsData, Instant approximateArrivalTimestamp) {
                nMsgsProcessed.incrementAndGet();
                nMsgsGps.incrementAndGet();
                gpsData.setImei(dataNode.has("imei") ? dataNode.get("imei").asText() : "");
                gpsData.setModel(dataNode.has("model") ? dataNode.get("model").asText() : "");
                gpsData.setValid(dataNode.has("valid") ? dataNode.get("valid").asText() : "");
                gpsData.setSpeed(dataNode.has("speed") ? dataNode.get("speed").asLong() : 0);
                if (gpsData.getSpeed() < 0 || gpsData.getSpeed() > 100) {
                        gpsData.setSpeed(100);
                }
                // if (dataNode.has("latitude") && dataNode.has("longitude") &&
                // dataNode.get("latitude").asDouble() != 0.0
                // && dataNode.get("longitude").asDouble() != 0.0) {
                gpsData.setLatitude(dataNode.has("latitude") ? dataNode.get("latitude").asDouble() : 0.0);
                gpsData.setLongitude(dataNode.has("longitude") ? dataNode.get("longitude").asDouble() : 0.0);
                gpsData.setAltitude(dataNode.has("altitude") ? dataNode.get("altitude").asLong() : 0);
                gpsData.setLngdirection(dataNode.has("long_dir") ? dataNode.get("long_dir").asText() : "");
                gpsData.setLatdirection(dataNode.has("lat_dir") ? dataNode.get("lat_dir").asText() : "");
                // }

                checkpoint.saveGpsCheckpoint(partitionKey, gpsData);
                try {
                        int recordsize = partitionKey.length() + sizeOf(gpsData.getProctime())
                                        + sizeOf(gpsData.getIntime())
                                        + sizeOf(gpsData.getAltitude()) + sizeOf(gpsData.getGpstime())
                                        + gpsData.getLatdirection().length()
                                        + gpsData.getLngdirection().length() + sizeOf(gpsData.getSpeed())
                                        + sizeOf(gpsData.getLatitude())
                                        + sizeOf(gpsData.getLongitude()) + sizeOf(gpsData.getUtctime())
                                        + sizeOf(gpsData.getPackettype().length())
                                        + sizeOf(gpsData.getValid().length());
                        ObjectNode valnode = new ObjectMapper().createObjectNode();
                        valnode.put("vin", partitionKey);
                        valnode.put("utctime", gpsData.getUtctime());
                        valnode.put("gpstime", gpsData.getGpstime());
                        valnode.put("intime", gpsData.getIntime());
                        valnode.put("altitude", gpsData.getAltitude());
                        valnode.put("latdir", gpsData.getLatdirection());
                        valnode.put("latitude", gpsData.getLatitude());
                        valnode.put("longdir", gpsData.getLngdirection());
                        valnode.put("longitude", gpsData.getLongitude());
                        valnode.put("speed", gpsData.getSpeed());
                        valnode.put("valid", gpsData.getValid());
                        valnode.put("proctime", gpsData.getProctime());
                        valnode.put("packettype", gpsData.getPackettype());
                        this.clickhPusher.sendGpsHistory(shardid, sequenceNumber, recordsize,
                                        valnode, approximateArrivalTimestamp);
                        nSendClickh.incrementAndGet();
                        this.absrpLtstPublisher.publish("gpsinfo." + partitionKey, shardid,
                                        sequenceNumber, gpsData.getUtctime(), recordsize, valnode,
                                        approximateArrivalTimestamp);
                } catch (Exception e) {
                        this.log.error("Error converting GPS data to JSON: " + e);
                        return false;
                }
                return true;
        }

        private boolean parseCanData(String shardid, String sequenceNumber, String partitionKey, JsonNode dataNode,
                        DataCheckp.CanData canData, HashMap<String, String> regulardataParamMeta,
                        HashMap<String, Boolean> regulardataOdoProcMeta,
                        Instant approximateArrivalTimestamp) {

                if (canData.getProctime() == 0 || canData.getProctime() < (System.currentTimeMillis()
                                - VEH_DATA_CHECKP_FETCH_INTERVAL)) {
                        if (!this.absrpLtstPublisher.getAndUpdateCheckpVin(partitionKey)) {
                                nCanMsgsCheckpUpdateFailed.incrementAndGet();
                                this.log.error("Failed to getAndUpdateCheckpVin for vin: " + partitionKey);
                                return false;
                        }
                }

                nMsgsProcessed.incrementAndGet();
                nMsgsCan.incrementAndGet();
                canData.setImei(dataNode.has("imei") ? dataNode.get("imei").asText() : "");
                canData.setModel(dataNode.has("model") ? dataNode.get("model").asText() : "");
                canData.setValid(dataNode.has("valid") ? dataNode.get("valid").asText() : "");
                canData.setTstamp(dataNode.has("tstamp") ? dataNode.get("tstamp").asText() : "");
                canData.setVmodelType(dataNode.has("vmodeltype") ? dataNode.get("vmodeltype").asDouble() : 0.0);

                // Use regulardataParamMeta map to dynamically fetch field values with null
                // checks
                try {
                        boolean odoProc = regulardataOdoProcMeta.getOrDefault("odometer", false);
                        boolean junkCompute = false;
                        String odometerKey = regulardataParamMeta.getOrDefault("odometer", "");
                        float odometer = !odometerKey.equals("") && dataNode.has(odometerKey)
                                        ? (float) dataNode.get(odometerKey).asDouble()
                                        : 0.0f;
                        String vModeKey = regulardataParamMeta.getOrDefault("v_mode", "");
                        int vModeState = !vModeKey.equals("") && dataNode.has(vModeKey) ? dataNode.get(vModeKey).asInt()
                                        : 0;
                        if (vModeState < 0) {
                                vModeState = 0;
                        }
                        boolean isCharging = (vModeState > 0 && vModeState < 5 && vModeState % 2 == 0) ? true
                                        : false;

                        if (odometer > 1.0f) {
                                canData.setOdometer(odometer);
                        } else if (odometer <= 1.0f && odoProc) { //removed && isCharging to add beacons to history data even if odo is zero and charging is false
                                nMsgsCanZeroOdo.incrementAndGet();
                                junkCompute = true;
                        } else {
                                nMsgsCanZeroOdo.incrementAndGet(); 
                                // return false; // attempting to add beacons to history data even if odo is zero and charging is false
                        }

                        String cycletimeKey = regulardataParamMeta.getOrDefault("cycletime", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("cycletime", false))) {
                                canData.setCycletime(
                                                !cycletimeKey.equals("") && dataNode.has(cycletimeKey)
                                                                ? dataNode.get(cycletimeKey).asText()
                                                                : "");
                        }

                        String chargerStsLiveKey = regulardataParamMeta.getOrDefault("charger_sts_live", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("charger_sts_live", false))) {
                                canData.setCharger_sts_live(
                                                !chargerStsLiveKey.equals("") && dataNode.has(chargerStsLiveKey)
                                                                ? dataNode.get(chargerStsLiveKey).asText()
                                                                : "");
                        }

                        String bmsChargerPlugInStsKey = regulardataParamMeta.getOrDefault("bms_charger_plug_in_sts",
                                        "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_charger_plug_in_sts", false))) {
                                canData.setBms_charger_plug_in_sts(
                                                !bmsChargerPlugInStsKey.equals("")
                                                                && dataNode.has(bmsChargerPlugInStsKey)
                                                                                ? dataNode.get(bmsChargerPlugInStsKey)
                                                                                                .asText()
                                                                                : "");
                        }

                        String driveEnableKey = regulardataParamMeta.getOrDefault("drive_enable", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("drive_enable", false))) {
                                canData.setDrive_enable(!driveEnableKey.equals("") && dataNode.has(driveEnableKey)
                                                ? dataNode.get(driveEnableKey).asText()
                                                : "");
                        }

                        String bmsHvbattConnectStsKey = regulardataParamMeta.getOrDefault("bms_hvbatt_connect_sts",
                                        "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_hvbatt_connect_sts", false))) {
                                canData.setBms_hvbatt_connect_sts(
                                                !bmsHvbattConnectStsKey.equals("")
                                                                && dataNode.has(bmsHvbattConnectStsKey)
                                                                                ? dataNode.get(bmsHvbattConnectStsKey)
                                                                                                .asText()
                                                                                : "");
                        }

                        String bmsHvbattDisconnectStsKey = regulardataParamMeta.getOrDefault(
                                        "bms_hvbatt_disconnect_sts",
                                        "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_hvbatt_disconnect_sts", false))) {
                                canData.setBms_hvbatt_disconnect_sts(
                                                !bmsHvbattDisconnectStsKey.equals("")
                                                                && dataNode.has(bmsHvbattDisconnectStsKey)
                                                                                ? dataNode.get(bmsHvbattDisconnectStsKey)
                                                                                                .asText()
                                                                                : "");
                        }

                        String bmsInKeyonKey = regulardataParamMeta.getOrDefault("bms_in_keyon", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_in_keyon", false))) {
                                canData.setBms_in_keyon(!bmsInKeyonKey.equals("") && dataNode.has(bmsInKeyonKey)
                                                ? dataNode.get(bmsInKeyonKey).asText()
                                                : "");
                        }

                        String bmsCyclenumKey = regulardataParamMeta.getOrDefault("bms_cyclenum", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_cyclenum", false))) {
                                canData.setBms_cyclenum(!bmsCyclenumKey.equals("") && dataNode.has(bmsCyclenumKey)
                                                ? dataNode.get(bmsCyclenumKey).asInt()
                                                : 0);
                        }

                        if (canData.getBms_cyclenum() < 0) {
                                canData.setBms_cyclenum(0);
                        }

                        String chargerTempLiveKey = regulardataParamMeta.getOrDefault("charger_temp_live", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("charger_temp_live", false))) {
                                canData.setCharger_temp_live(
                                                !chargerTempLiveKey.equals("") && dataNode.has(chargerTempLiveKey)
                                                                ? (float) dataNode.get(chargerTempLiveKey).asDouble()
                                                                : 0.0f);
                        }

                        String chargertempKey = regulardataParamMeta.getOrDefault("chargertemp", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("chargertemp", false))) {
                                canData.setChargertemp(!chargertempKey.equals("") && dataNode.has(chargertempKey)
                                                ? (float) dataNode.get(chargertempKey).asDouble()
                                                : 0.0f);
                        }

                        String socKey = regulardataParamMeta.getOrDefault("soc", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("soc", false))) {
                                canData.setSoc(!socKey.equals("") && dataNode.has(socKey)
                                                ? (float) dataNode.get(socKey).asDouble()
                                                : 0.0f);
                        }

                        String sohKey = regulardataParamMeta.getOrDefault("soh", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("soh", false))) {
                                canData.setSoh(!sohKey.equals("") && dataNode.has(sohKey) ? dataNode.get(sohKey).asInt()
                                                : 0);
                        }

                        if (canData.getSoh() < 0) {
                                canData.setSoh(0);
                        }

                        String bmsCellAvgVoltKey = regulardataParamMeta.getOrDefault("bms_cell_avg_volt", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_cell_avg_volt", false))) {
                                canData.setBms_cell_avg_volt(
                                                !bmsCellAvgVoltKey.equals("") && dataNode.has(bmsCellAvgVoltKey)
                                                                ? (float) dataNode.get(bmsCellAvgVoltKey).asDouble()
                                                                : 0.0f);
                        }

                        String bmsCellAvgTempKey = regulardataParamMeta.getOrDefault("bms_cell_avg_temp", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_cell_avg_temp", false))) {
                                canData.setBms_cell_avg_temp(
                                                !bmsCellAvgTempKey.equals("") && dataNode.has(bmsCellAvgTempKey)
                                                                ? (float) dataNode.get(bmsCellAvgTempKey).asDouble()
                                                                : 0.0f);
                        }

                        String motortempKey = regulardataParamMeta.getOrDefault("motortemp", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("motortemp", false))) {
                                canData.setMotortemp(!motortempKey.equals("") && dataNode.has(motortempKey)
                                                ? (float) dataNode.get(motortempKey).asDouble()
                                                : 0.0f);
                        }

                        String bmsBattPackTempKey = regulardataParamMeta.getOrDefault("bms_batt_pack_temp", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bms_batt_pack_temp", false))) {
                                canData.setBms_batt_pack_temp(
                                                !bmsBattPackTempKey.equals("") && dataNode.has(bmsBattPackTempKey)
                                                                ? (float) dataNode.get(bmsBattPackTempKey).asDouble()
                                                                : 0.0f);
                        }

                        String brakeSwitchStatusKey = regulardataParamMeta.getOrDefault("brake_switch_status", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("brake_switch_status", false))) {
                                canData.setBrake_switch_status(
                                                !brakeSwitchStatusKey.equals("") && dataNode.has(brakeSwitchStatusKey)
                                                                ? dataNode.get(brakeSwitchStatusKey).asInt()
                                                                : 0);
                        }

                        if (canData.getBrake_switch_status() < 0) {
                                canData.setBrake_switch_status(0);
                        }

                        String vehiclespeedKey = regulardataParamMeta.getOrDefault("vehiclespeed", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("vehiclespeed", false))) {
                                canData.setVehiclespeed(!vehiclespeedKey.equals("") && dataNode.has(vehiclespeedKey)
                                                ? (float) dataNode.get(vehiclespeedKey).asDouble()
                                                : 0.0f);
                        }

                        if (canData.getVehiclespeed() < 0 || canData.getVehiclespeed() > 100) {
                                canData.setVehiclespeed(100.0f);
                        }

                        String motorpowerKey = regulardataParamMeta.getOrDefault("motorpower", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("motorpower", false))) {
                                canData.setMotorpower(!motorpowerKey.equals("") && dataNode.has(motorpowerKey)
                                                ? (float) dataNode.get(motorpowerKey).asDouble()
                                                : 0.0f);
                        }

                        String eMotorRpmKey = regulardataParamMeta.getOrDefault("e_motor_rpm", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("e_motor_rpm", false))) {
                                canData.setE_motor_rpm(!eMotorRpmKey.equals("") && dataNode.has(eMotorRpmKey)
                                                ? dataNode.get(eMotorRpmKey).asInt()
                                                : 0);
                        }

                        if (canData.getE_motor_rpm() < 0) {
                                canData.setE_motor_rpm(0);
                        }

                        String invDrivemodeShiftKey = regulardataParamMeta.getOrDefault("inv_drivemode_shift", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("inv_drivemode_shift", false))) {
                                canData.setInv_drivemode_shift(
                                                !invDrivemodeShiftKey.equals("") && dataNode.has(invDrivemodeShiftKey)
                                                                ? dataNode.get(invDrivemodeShiftKey).asInt()
                                                                : 0);
                        }

                        if (canData.getInv_drivemode_shift() < 0) {
                                canData.setInv_drivemode_shift(0);
                        }

                        String vehiclePhaseKey = regulardataParamMeta.getOrDefault("vehicle_phase", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("vehicle_phase", false))) {
                                canData.setVehicle_phase(!vehiclePhaseKey.equals("") && dataNode.has(vehiclePhaseKey)
                                                ? dataNode.get(vehiclePhaseKey).asInt()
                                                : 0);
                        }

                        if (canData.getVehicle_phase() < 0) {
                                canData.setVehicle_phase(0);
                        }

                        // String vModeKey = regulardataParamMeta.getOrDefault("v_mode", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("v_mode", false))) {
                                canData.setV_mode(
                                                !vModeKey.equals("") && dataNode.has(vModeKey)
                                                                ? dataNode.get(vModeKey).asInt()
                                                                : 0);
                        }

                        if (canData.getV_mode() < 0) {
                                canData.setV_mode(0);
                        }

                        String batteryTtcKey = regulardataParamMeta.getOrDefault("battery_ttc", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("battery_ttc", false))) {
                                canData.setBattery_ttc(!batteryTtcKey.equals("") && dataNode.has(batteryTtcKey)
                                                ? dataNode.get(batteryTtcKey).asInt()
                                                : 0);
                        }

                        if (canData.getBattery_ttc() < 0) {
                                canData.setBattery_ttc(0);
                        }

                        String percthrottleKey = regulardataParamMeta.getOrDefault("percthrottle", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("percthrottle", false))) {
                                canData.setPercthrottle(!percthrottleKey.equals("") && dataNode.has(percthrottleKey)
                                                ? (float) dataNode.get(percthrottleKey).asDouble()
                                                : 0.0f);
                        }

                        String kwhKey = regulardataParamMeta.getOrDefault("kwh", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("kwh", false))) {
                                canData.setKwh(!kwhKey.equals("") && dataNode.has(kwhKey)
                                                ? (float) dataNode.get(kwhKey).asDouble()
                                                : 0.0f);
                        }

                        String battCurrentKey = regulardataParamMeta.getOrDefault("batt_current", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("batt_current", false))) {
                                canData.setBatt_current(!battCurrentKey.equals("") && dataNode.has(battCurrentKey)
                                                ? (float) dataNode.get(battCurrentKey).asDouble()
                                                : 0.0f);
                        }

                        String auxBattVoltKey = regulardataParamMeta.getOrDefault("aux_batt_volt", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("aux_batt_volt", false))) {
                                canData.setAux_batt_volt(!auxBattVoltKey.equals("") && dataNode.has(auxBattVoltKey)
                                                ? (float) dataNode.get(auxBattVoltKey).asDouble()
                                                : 0.0f);
                        }

                        String batVoltageKey = regulardataParamMeta.getOrDefault("bat_voltage", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bat_voltage", false))) {
                                canData.setBat_voltage(!batVoltageKey.equals("") && dataNode.has(batVoltageKey)
                                                ? (float) dataNode.get(batVoltageKey).asDouble()
                                                : 0.0f);
                        }

                        String busVoltageKey = regulardataParamMeta.getOrDefault("bus_voltage", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("bus_voltage", false))) {
                                canData.setBus_voltage(!busVoltageKey.equals("") && dataNode.has(busVoltageKey)
                                                ? (float) dataNode.get(busVoltageKey).asDouble()
                                                : 0.0f);
                        }

                        String dteKey = regulardataParamMeta.getOrDefault("dte", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("dte", false))) {
                                canData.setDte(!dteKey.equals("") && dataNode.has(dteKey)
                                                ? (float) dataNode.get(dteKey).asDouble()
                                                : 0.0f);
                        }

                        String canThrottleKey = regulardataParamMeta.getOrDefault("can_throttle", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("can_throttle", false))) {
                                canData.setCan_throttle(!canThrottleKey.equals("") && dataNode.has(canThrottleKey)
                                                ? dataNode.get(canThrottleKey).asInt()
                                                : 0);
                        }

                        if (canData.getCan_throttle() < 0) {
                                canData.setCan_throttle(0);
                        }

                        String wakeupCommandKey = regulardataParamMeta.getOrDefault("wakeup_command", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("wakeup_command", false))) {
                                canData.setWakeup_command(!wakeupCommandKey.equals("") && dataNode.has(wakeupCommandKey)
                                                ? (float) dataNode.get(wakeupCommandKey).asDouble()
                                                : 0.0f);
                        }

                        String vehImmoRespKey = regulardataParamMeta.getOrDefault("veh_immo_resp", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("veh_immo_resp", false))) {
                                canData.setVeh_immo_resp(!vehImmoRespKey.equals("") && dataNode.has(vehImmoRespKey)
                                                ? dataNode.get(vehImmoRespKey).asInt()
                                                : 0);
                        }

                        if (canData.getVeh_immo_resp() < 0) {
                                canData.setVeh_immo_resp(0);
                        }

                        String serviceTtKey = regulardataParamMeta.getOrDefault("service_tt", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("service_tt", false))) {
                                canData.setService_tt(!serviceTtKey.equals("") && dataNode.has(serviceTtKey)
                                                ? dataNode.get(serviceTtKey).asInt()
                                                : 0);
                        }

                        if (canData.getService_tt() < 0) {
                                canData.setService_tt(0);
                        }

                        String chargettKey = regulardataParamMeta.getOrDefault("chargett", "");
                        if (!junkCompute || (junkCompute && regulardataOdoProcMeta.getOrDefault("chargett", false))) {
                                canData.setChargett(!chargettKey.equals("") && dataNode.has(chargettKey)
                                                ? dataNode.get(chargettKey).asInt()
                                                : 0);
                        }

                        if (canData.getChargett() < 0) {
                                canData.setChargett(0);
                        }

                        String connectionhealthttKey = regulardataParamMeta.getOrDefault("connectionhealthtt", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("connectionhealthtt", false))) {
                                canData.setConnectionhealthtt(
                                                !connectionhealthttKey.equals("") && dataNode.has(connectionhealthttKey)
                                                                ? dataNode.get(connectionhealthttKey).asInt()
                                                                : 0);
                        }

                        if (canData.getConnectionhealthtt() < 0) {
                                canData.setConnectionhealthtt(0);
                        }

                        String tcuSpeedmodeKey = regulardataParamMeta.getOrDefault("tcu_speedmode", "");
                        if (!junkCompute || (junkCompute
                                        && regulardataOdoProcMeta.getOrDefault("tcu_speedmode", false))) {
                                canData.setTcu_speedmode(!tcuSpeedmodeKey.equals("") && dataNode.has(tcuSpeedmodeKey)
                                                ? dataNode.get(tcuSpeedmodeKey).asInt()
                                                : 0);
                        }

                        if (canData.getTcu_speedmode() < 0) {
                                canData.setTcu_speedmode(0);
                        }
                } catch (Exception e) {
                        nCanMsgsProcessingError.incrementAndGet();
                        this.log.error("Error getting param values " + e);
                        return false;
                }

                checkpoint.saveCanCheckpoint(partitionKey, canData);

                if (checkpoint.getCanDataRecord().get(partitionKey) == null) {
                        ArrayList<DataCheckp.CanData> canDataInmemList = new ArrayList<>();
                        ArrayList<String> sequenceNumberList = new ArrayList<>();
                        ArrayList<Instant> approximateArrivalTimestampList = new ArrayList<>();
                        String lastSequenceNumber = "";
                        Instant lastApproximateArrivalTimestamp = Instant.now();
                        int checkpCounter = 0;

                        // Create separate instances to avoid shared references
                        DataCheckp.CanData lastData = createDeepCopy(checkpoint.getCanCheckpoint(partitionKey));

                        checkpoint.getCanDataRecord().put(partitionKey, new DataCheckp.CanDataRecord(canDataInmemList,
                                        sequenceNumberList, approximateArrivalTimestampList, lastData,
                                        lastSequenceNumber,
                                        lastApproximateArrivalTimestamp, checkpCounter));

                        checkpoint.saveCanDataRecordCheckpoint(partitionKey,
                                        checkpoint.getCanDataRecord().get(partitionKey));
                }

                // check if the sequence list is not empty and if its less than or equal to
                // the 1st one in list then skip the record
                if (checkpoint.getCanDataRecord().get(partitionKey).getLastSequenceNumber().equals("")
                                || checkpoint.getCanDataRecord().get(partitionKey).getLastSequenceNumber() == null) {
                        if (checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList().size() > 0
                                        && sequenceNumber.compareTo(checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getSequenceNumberList().get(0)) <= 0) {
                                // log.info("Sequence number is less than the earliest one in list, skipping");
                                nCanMsgsSeqNumOutOfOrder.incrementAndGet();
                                return false;
                        }
                } else if (sequenceNumber.compareTo(checkpoint.getCanDataRecord().get(partitionKey)
                                .getLastSequenceNumber()) <= 0) {
                        nCanMsgsSeqNumOutOfOrder.incrementAndGet();
                        return false;
                }

                checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList().add(createDeepCopy(canData));
                checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList().add(sequenceNumber);
                checkpoint.getCanDataRecord().get(partitionKey).getApproximateArrivalTimestampList()
                                .add(approximateArrivalTimestamp);

                return handleBufferCheck(shardid, partitionKey);
        }

        public boolean handleBufferCheck(String shardid, String partitionKey) {
                // 1. Keep the inmem list in sorted order
                for (int i = checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList().size()
                                - 1; i > 0; i--) {
                        if (checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList().get(i)
                                        .getUtctime() >= checkpoint
                                                        .getCanDataRecord().get(partitionKey).getCanDataInmemList()
                                                        .get(i - 1)
                                                        .getUtctime()) {
                                break;
                        }
                        // Swap elements
                        DataCheckp.CanData temp = checkpoint.getCanDataRecord().get(partitionKey)
                                        .getCanDataInmemList().get(i);
                        String tempSqNum = checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList()
                                        .get(i);
                        checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList().set(i, checkpoint
                                        .getCanDataRecord().get(partitionKey).getCanDataInmemList().get(i - 1));
                        checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList().set(i - 1, temp);
                        checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList().set(i,
                                        checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList()
                                                        .get(i - 1));
                        checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList().set(i - 1, tempSqNum);
                        Instant tempApproximateArrivalTimestamp = checkpoint.getCanDataRecord().get(partitionKey)
                                        .getApproximateArrivalTimestampList().get(i);
                        checkpoint.getCanDataRecord().get(partitionKey).getApproximateArrivalTimestampList().set(i,
                                        checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getApproximateArrivalTimestampList()
                                                        .get(i - 1));
                        checkpoint.getCanDataRecord().get(partitionKey).getApproximateArrivalTimestampList().set(i - 1,
                                        tempApproximateArrivalTimestamp);
                }

                if (checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList()
                                .size() <= MAX_BEACON_TO_REMOVE_OUT_OF_ORDER) {
                        nMsgsMaxOOOBeacons.incrementAndGet();
                        return true;
                }

                // 2. Pickup the next record to process
                DataCheckp.CanData nextRecord = checkpoint.getCanDataRecord().get(partitionKey)
                                .getCanDataInmemList().get(0);
                checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList().remove(0);
                String lastSequenceNumber = checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList()
                                .get(0);
                checkpoint.getCanDataRecord().get(partitionKey).getSequenceNumberList().remove(0);
                Instant tempApproximateArrivalTimestamp = checkpoint.getCanDataRecord().get(partitionKey)
                                .getApproximateArrivalTimestampList().get(0);
                checkpoint.getCanDataRecord().get(partitionKey).getApproximateArrivalTimestampList().remove(0);

                // 3. Merge it to the last record
                if (checkpoint.getCanDataRecord().get(partitionKey).getLastData() == null) {
                        // Create a deep copy to avoid shared references
                        // Sort data by utctime before processing
                        checkpoint.getCanDataRecord().get(partitionKey).getCanDataInmemList()
                                        .sort((a, b) -> Long.compare(a.getUtctime(), b.getUtctime()));
                        checkpoint.getCanDataRecord().get(partitionKey).setLastData(createDeepCopy(nextRecord));
                        checkpoint.getCanDataRecord().get(partitionKey).setLastSequenceNumber(lastSequenceNumber);
                        checkpoint.getCanDataRecord().get(partitionKey).setLastApproximateArrivalTimestamp(
                                        tempApproximateArrivalTimestamp);
                } else {
                        // Backup important metrics
                        float odoF64 = checkpoint.getCanDataRecord().get(partitionKey).getLastData().getOdometer();
                        // double kwhF64 = lastCheck.getLastData().getFloatVals().getOrDefault("kwh",
                        // 0.0);

                        // Create a deep copy to avoid shared references
                        checkpoint.getCanDataRecord().get(partitionKey).setLastData(createDeepCopy(nextRecord));
                        checkpoint.getCanDataRecord().get(partitionKey).setLastSequenceNumber(lastSequenceNumber);
                        checkpoint.getCanDataRecord().get(partitionKey).setLastApproximateArrivalTimestamp(
                                        tempApproximateArrivalTimestamp);
                        checkpoint.getCanDataRecord().get(partitionKey).setCheckpCounter(
                                        checkpoint.getCanDataRecord().get(partitionKey).getCheckpCounter() + 1);

                        double odoAfterUpdatef64 = checkpoint.getCanDataRecord().get(partitionKey).getLastData()
                                        .getOdometer();
                        // kwhUpdatef64 := lastCheckI.LastData.FloatVals["kwh"]
                        if (odoAfterUpdatef64 <= 1.0f) {
                                checkpoint.getCanDataRecord().get(partitionKey).getLastData().setOdometer(odoF64);
                        }
                        // if (kwhUpdateF64 == 0) {
                        // lastCheck.getLastData().getFloatVals().put("kwh", kwhF64);
                        // }
                }

                DataCheckp.CanData canData = checkpoint.getCanDataRecord().get(partitionKey).getLastData();
                String sequenceNumber = checkpoint.getCanDataRecord().get(partitionKey).getLastSequenceNumber();
                Instant approximateArrivalTimestamp = checkpoint.getCanDataRecord().get(partitionKey)
                                .getLastApproximateArrivalTimestamp();

                try {
                        int recordsize = partitionKey.length() + sizeOf(canData.getProctime())
                                        + sizeOf(canData.getIntime())
                                        + sizeOf(canData.getUtctime()) + canData.getImei().length()
                                        + canData.getModel().length() + canData.getValid().length()
                                        + canData.getPackettype().length()
                                        + canData.getBms_hvbatt_connect_sts().length()
                                        + canData.getCycletime().length()
                                        + canData.getBms_charger_plug_in_sts().length()
                                        + canData.getDrive_enable().length()
                                        + canData.getCharger_sts_live().length()
                                        + canData.getBms_hvbatt_disconnect_sts().length()
                                        + canData.getBms_in_keyon().length()
                                        + sizeOf(canData.getBms_cyclenum()) + sizeOf(canData.getCharger_temp_live())
                                        + sizeOf(canData.getChargertemp()) + sizeOf(canData.getSoc())
                                        + sizeOf(canData.getSoh()) + sizeOf(canData.getBms_cell_avg_volt())
                                        + sizeOf(canData.getBms_cell_avg_temp()) + sizeOf(canData.getMotortemp())
                                        + sizeOf(canData.getBms_batt_pack_temp())
                                        + sizeOf(canData.getBrake_switch_status())
                                        + sizeOf(canData.getOdometer()) + sizeOf(canData.getVehiclespeed())
                                        + sizeOf(canData.getMotorpower()) + sizeOf(canData.getE_motor_rpm())
                                        + sizeOf(canData.getInv_drivemode_shift()) + sizeOf(canData.getV_mode())
                                        + sizeOf(canData.getBattery_ttc()) + sizeOf(canData.getPercthrottle())
                                        + sizeOf(canData.getKwh()) + sizeOf(canData.getBatt_current())
                                        + sizeOf(canData.getAux_batt_volt()) + sizeOf(canData.getBat_voltage())
                                        + sizeOf(canData.getBus_voltage())
                                        + sizeOf(canData.getDte()) + sizeOf(canData.getCan_throttle())
                                        + sizeOf(canData.getWakeup_command()) + sizeOf(canData.getVeh_immo_resp())
                                        + sizeOf(canData.getService_tt()) + sizeOf(canData.getChargett())
                                        + sizeOf(canData.getConnectionhealthtt()) + sizeOf(canData.getTcu_speedmode());

                        ObjectNode valnode = new ObjectMapper().createObjectNode();
                        valnode.put("vin", partitionKey);
                        valnode.put("imei", canData.getImei());
                        valnode.put("model", canData.getModel());
                        valnode.put("utctime", canData.getUtctime());
                        valnode.put("intime", canData.getIntime());
                        valnode.put("tstamp", canData.getTstamp());
                        valnode.put("cycletime", canData.getCycletime());
                        valnode.put("charger_sts_live", canData.getCharger_sts_live());
                        valnode.put("bms_charger_plug_in_sts", canData.getBms_charger_plug_in_sts());
                        valnode.put("drive_enable", canData.getDrive_enable());
                        valnode.put("bms_hvbatt_connect_sts", canData.getBms_hvbatt_connect_sts());
                        valnode.put("bms_hvbatt_disconnect_sts", canData.getBms_hvbatt_disconnect_sts());
                        valnode.put("bms_in_keyon", canData.getBms_in_keyon());
                        valnode.put("bms_cyclenum", canData.getBms_cyclenum());
                        valnode.put("charger_temp_live", canData.getCharger_temp_live());
                        valnode.put("chargertemp", canData.getChargertemp());
                        valnode.put("soc", canData.getSoc());
                        valnode.put("soh", canData.getSoh());
                        valnode.put("bms_cell_avg_volt", canData.getBms_cell_avg_volt());
                        valnode.put("bms_cell_avg_temp", canData.getBms_cell_avg_temp());
                        valnode.put("motortemp", canData.getMotortemp());
                        valnode.put("bms_batt_pack_temp", canData.getBms_batt_pack_temp());
                        valnode.put("brake_switch_status", canData.getBrake_switch_status());
                        valnode.put("odometer", canData.getOdometer());
                        valnode.put("vehiclespeed", canData.getVehiclespeed());
                        valnode.put("motorpower", canData.getMotorpower());
                        valnode.put("e_motor_rpm", canData.getE_motor_rpm());
                        valnode.put("inv_drivemode_shift", canData.getInv_drivemode_shift());
                        valnode.put("v_mode", canData.getV_mode());
                        valnode.put("vehicle_phase", canData.getVehicle_phase());
                        valnode.put("battery_ttc", canData.getBattery_ttc());
                        valnode.put("percthrottle", canData.getPercthrottle());
                        valnode.put("kwh", canData.getKwh());
                        valnode.put("batt_current", canData.getBatt_current());
                        valnode.put("aux_batt_volt", canData.getAux_batt_volt());
                        valnode.put("bat_voltage", canData.getBat_voltage());
                        valnode.put("bus_voltage", canData.getBus_voltage());
                        valnode.put("dte", canData.getDte());
                        valnode.put("can_throttle", canData.getCan_throttle());
                        valnode.put("wakeup_command", canData.getWakeup_command());
                        valnode.put("veh_immo_resp", canData.getVeh_immo_resp());
                        valnode.put("service_tt", canData.getService_tt());
                        valnode.put("chargett", canData.getChargett());
                        valnode.put("connectionhealthtt", canData.getConnectionhealthtt());
                        valnode.put("tcu_speedmode", canData.getTcu_speedmode());
                        valnode.put("valid", canData.getValid());
                        valnode.put("proctime", canData.getProctime());
                        valnode.put("packettype", canData.getPackettype());
                        this.clickhPusher.sendCanHistory(shardid, sequenceNumber,
                                        recordsize, valnode, approximateArrivalTimestamp);
                        nSendClickh.incrementAndGet();
                        // Latest Data send
                        // ObjectMapper mapper = new ObjectMapper();
                        // String canJsonString = mapper.writeValueAsString(canData);
                        this.absrpLtstPublisher.publish("caninfo." + partitionKey, shardid,
                                        sequenceNumber,
                                        canData.getUtctime(),
                                        recordsize, valnode, approximateArrivalTimestamp);
                } catch (Exception e) {
                        nCanMsgsBufferCheckError.incrementAndGet();
                        this.log.error("Error converting CAN data to JSON: " + e);
                        return false;
                }

                return checkpToRedis(partitionKey);
        }

        public boolean checkpToRedis(String partitionKey) {
                try {
                        if (this.isStopped.get()) {
                                this.log.info("Statsfeeder is stopped, skipping checkpToRedis");
                                nCheckpToRedisStopped.incrementAndGet();
                                return true;
                        }

                        if (checkpoint.getCanDataRecord().get(partitionKey).getCheckpCounter() < 10) {
                                nCheckpToRedisSkipped.incrementAndGet();
                                return true;
                        }
                        checkpoint.getCanDataRecord().get(partitionKey).setCheckpCounter(0);

                        ObjectMapper mapper = new ObjectMapper();
                        ObjectNode checkpNode = mapper.createObjectNode();
                        checkpNode.put("canDataInmemList",
                                        mapper.valueToTree(checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getCanDataInmemList()));
                        checkpNode.put("sequenceNumberList",
                                        mapper.valueToTree(checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getSequenceNumberList()));
                        checkpNode.put("approximateArrivalTimestampList",
                                        mapper.valueToTree(checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getApproximateArrivalTimestampList()));
                        checkpNode.put("canlastData",
                                        mapper.valueToTree(checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getLastData()));
                        checkpNode.put("lastSequenceNumber",
                                        checkpoint.getCanDataRecord().get(partitionKey).getLastSequenceNumber());
                        checkpNode.put("lastApproximateArrivalTimestamp",
                                        checkpoint.getCanDataRecord().get(partitionKey)
                                                        .getLastApproximateArrivalTimestamp().toString());

                        String dataRecordJsonString = mapper.writeValueAsString(checkpNode);
                        this.absrpLtstPublisher.publishOnlyRedis("checkp.gcfeeder.candatarecord." + partitionKey,
                                        dataRecordJsonString);
                } catch (Exception e) {
                        nCheckpToRedisError.incrementAndGet();
                        this.log.error("Error publishing can data to redis: " + e, e);
                        return false;
                }
                return true;
        }

        private boolean parseGpsDataForLiveness(String shardid, String sequenceNumber, String vin,
                        JsonNode dataNode,
                        Instant approximateArrivalTimestamp) {
                try {
                        if (convertToEpochMillis(dataNode.get("tgu_timestamp").asText()) == 0) {
                                nGpsLivenessInvalidTimestamp.incrementAndGet();
                                return false;
                        }
                        long utctime = convertToEpochMillis(dataNode.get("tgu_timestamp").asText());
                        int istOffset = 330 * 60 * 1000;
                        int utc_day_b = (int) Math.floor((utctime + istOffset) / (24 * 60 * 60 * 1000));
                        String livenessPartitionKey = vin + "_" + String.valueOf(utc_day_b);

                        DataCheckp.LivenessData livenessData = checkpoint.getLivenessCheckpoint(livenessPartitionKey);
                        if (livenessData.getVin().equals("") || (livenessData.getProctime() == 0
                                        || livenessData.getProctime() < (System.currentTimeMillis()
                                                        - VEH_DATA_CHECKP_FETCH_INTERVAL))) {
                                // this vin doesn't exist in cache. see if it exists in redis
                                nGpsLivenessOutOfDate.incrementAndGet();
                                this.absrpLtstPublisher.getAndUpdateLivenessCheckp(livenessPartitionKey);
                        }

                        livenessData.setVin(vin);
                        livenessData.setUtc_day_b(utc_day_b);
                        livenessData.setGps_last(""); // TODO: temporary fix
                        livenessData.setGps_cnt(livenessData.getGps_cnt() + 1);
                        livenessData.setProctime(System.currentTimeMillis());

                        if (livenessData.getGps_cnt() == 1) {
                                livenessData.setGps_first(""); // TODO: temporary fix
                        }

                        checkpoint.saveLivenessCheckpoint(livenessPartitionKey, livenessData);
                        int recordsize = vin.length() + sizeOf(livenessData.getUtc_day_b())
                                        + sizeOf(livenessData.getGps_cnt())
                                        + sizeOf(livenessData.getCan_cnt())
                                        + livenessData.getGps_last().length()
                                        + livenessData.getGps_first().length()
                                        + livenessData.getCan_first().length()
                                        + livenessData.getCan_last().length();

                        ObjectNode valnode = new ObjectMapper().createObjectNode();
                        valnode.put("vin", vin);
                        valnode.put("utc_day_b", livenessData.getUtc_day_b());
                        valnode.put("gps_cnt", livenessData.getGps_cnt());
                        valnode.put("can_cnt", livenessData.getCan_cnt());
                        valnode.put("gps_last", livenessData.getGps_last());
                        valnode.put("gps_first", livenessData.getGps_first());
                        valnode.put("can_first", livenessData.getCan_first());
                        valnode.put("can_last", livenessData.getCan_last());
                        valnode.put("proctime", livenessData.getProctime());

                        this.absrpLtstPublisher.publish(
                                        AbsrpLatestPublisher.LIVENESS_CHECKPOINT_KEY + livenessPartitionKey, shardid,
                                        sequenceNumber, utctime, recordsize, valnode, approximateArrivalTimestamp);

                        nSendClickh.incrementAndGet();

                } catch (Exception e) {
                        nGpsLivenessProcessingError.incrementAndGet();
                        this.log.error("Error parsing gps data for liveness " + e);
                        return false;
                }

                return true;
        }

        private boolean parseCanDataForLiveness(String shardid, String sequenceNumber, String vin,
                        JsonNode dataNode,
                        Instant approximateArrivalTimestamp) {
                try {
                        if (convertToEpochMillis(dataNode.get("tgu_timestamp").asText()) == 0) {
                                nCanLivenessInvalidTimestamp.incrementAndGet();
                                return false;
                        }
                        long utctime = convertToEpochMillis(dataNode.get("tgu_timestamp").asText());
                        int istOffset = 330 * 60 * 1000;
                        int utc_day_b = (int) Math.floor((utctime + istOffset) / (24 * 60 * 60 * 1000));
                        String livenessPartitionKey = vin + "_" + String.valueOf(utc_day_b);

                        DataCheckp.LivenessData livenessData = checkpoint.getLivenessCheckpoint(livenessPartitionKey);
                        if (livenessData.getVin().equals("") || (livenessData.getProctime() == 0
                                        || livenessData.getProctime() < (System.currentTimeMillis()
                                                        - VEH_DATA_CHECKP_FETCH_INTERVAL))) {
                                // this vin doesn't exist in cache. see if it exists in redis
                                this.absrpLtstPublisher.getAndUpdateLivenessCheckp(livenessPartitionKey);
                        }

                        livenessData.setVin(vin);
                        livenessData.setUtc_day_b(utc_day_b);
                        livenessData.setCan_last(""); // TODO: temporary fix
                        livenessData.setCan_cnt(livenessData.getCan_cnt() + 1);
                        livenessData.setProctime(System.currentTimeMillis());

                        if (livenessData.getCan_cnt() == 1) {
                                livenessData.setCan_first(""); // TODO: temporary fix
                        }

                        checkpoint.saveLivenessCheckpoint(livenessPartitionKey, livenessData);
                        int recordsize = vin.length() + sizeOf(livenessData.getUtc_day_b())
                                        + sizeOf(livenessData.getGps_cnt())
                                        + sizeOf(livenessData.getCan_cnt())
                                        + livenessData.getGps_last().length()
                                        + livenessData.getGps_first().length()
                                        + livenessData.getCan_first().length()
                                        + livenessData.getCan_last().length();

                        ObjectNode valnode = new ObjectMapper().createObjectNode();
                        valnode.put("vin", vin);
                        valnode.put("utc_day_b", livenessData.getUtc_day_b());
                        valnode.put("gps_cnt", livenessData.getGps_cnt());
                        valnode.put("can_cnt", livenessData.getCan_cnt());
                        valnode.put("last_gps", livenessData.getGps_last());
                        valnode.put("first_gps", livenessData.getGps_first());
                        valnode.put("first_can", livenessData.getCan_first());
                        valnode.put("last_can", livenessData.getCan_last());
                        valnode.put("proctime", livenessData.getProctime());

                        this.absrpLtstPublisher.publish(
                                        AbsrpLatestPublisher.LIVENESS_CHECKPOINT_KEY + livenessPartitionKey, shardid,
                                        sequenceNumber, utctime, recordsize, valnode, approximateArrivalTimestamp);

                        nSendClickh.incrementAndGet();

                } catch (Exception e) {
                        nCanLivenessProcessingError.incrementAndGet();
                        this.log.error("Error parsing can data for liveness " + e);
                        return false;
                }

                return true;
        }

        public void publishCheckpoint() {
                try {
                        if (!this.absrpLtstPublisher.getAndUpdateCheckp("regular")) {
                                this.log.error("Failed to getAndUpdateCheckp");
                                return;
                        }

                        this.cleanupShardCheckpoints();
                        // this.absrpLtstPublisher.publishOnlyRedis(
                        // "checkp." + this.config.getApplicationName() + ".appver",
                        // String.valueOf(this.appVersion));

                        ObjectMapper mapper = new ObjectMapper();
                        if (this.lastShardIdCheckpointTime.isEmpty()) {
                                this.log.error("No checkpoint times found, skipping publish");
                                return;
                        }
                        String checkpJsonString = mapper.writeValueAsString(this.lastShardIdCheckpointTime);
                        this.absrpLtstPublisher.publishOnlyRedis("checkp." + this.config.getApplicationName(),
                                        checkpJsonString);
                } catch (Exception e) {
                        this.log.error("Error publishing checkpoint: " + e);
                        this.log.error("Error publishing checkpoint: " + e.getMessage());
                        this.log.error("Error publishing checkpoint: " + e.getStackTrace());
                        this.log.error("Error publishing checkpoint: " + e.toString());
                }
        }

        public void cleanupShardCheckpoints() {
                Optional<Instant> latestCheckpointOpt = getLatestShardCheckpointTime();
                if (!latestCheckpointOpt.isPresent()) {
                        return;
                }
                Instant thresholdTime = latestCheckpointOpt.get().minusSeconds(7200);
                this.lastShardIdCheckpointTime.entrySet()
                                .removeIf(entry -> entry.getValue().isBefore(thresholdTime));
        }

        public Map<String, Object> createMap(String... keyValues) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < keyValues.length; i += 2) {
                        if (i + 1 < keyValues.length) {
                                map.put(keyValues[i], keyValues[i + 1]);
                        }
                }
                return map;
        }

        public boolean stopHandler() {
                this.publishCheckpoint();
                try {
                        Thread.sleep(5000);
                } catch (InterruptedException e) {
                        this.log.warn("Thread interrupted during stopHandler sleep " + e);
                        Thread.currentThread().interrupt();
                }
                this.isStopped.set(true);
                this.clickhPusher.stop(60000);
                this.absrpLtstPublisher.stopThread();
                return true;
        }

        public boolean updateMetrics(String shardId) {
                // ObjectMapper objectMapper = new ObjectMapper();
                // ObjectNode objectNode = objectMapper.createObjectNode();
                // objectNode.put("nMsgsReceived", nMsgsReceived.get());
                this.metrics.gauge("datahandler.nMsgsReceived", createMap("help", "total_msgs_received"))
                                .set(nMsgsReceived.get(), new HashMap<>());
                // objectNode.put("nMsgsProcessed", nMsgsProcessed.get());
                this.metrics.gauge("datahandler.nMsgsProcessed", createMap("help", "total_msgs_processed"))
                                .set(nMsgsProcessed.get(), new HashMap<>());
                // objectNode.put("nMsgsGps", nMsgsGps.get());

                this.metrics.gauge("datahandler.nMsgsNull", createMap("help", "total_msgs_null"))
                                .set(nMsgsNull.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsBufferRewound", createMap("help", "total_msgs_buffer_rewound"))
                                .set(nMsgsBufferRewound.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsZeroDataBuf", createMap("help", "total_msgs_zero_data_buf"))
                                .set(nMsgsZeroDataBuf.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsEmptyData", createMap("help", "total_msgs_empty_data"))
                                .set(nMsgsEmptyData.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsInvalidVin", createMap("help", "total_msgs_invalid_vin"))
                                .set(nMsgsInvalidVin.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsVinMismatch", createMap("help", "total_msgs_vin_mismatch"))
                                .set(nMsgsVinMismatch.get(), new HashMap<>());
                // this.metrics.gauge("datahandler.nMsgsT380Check", createMap("help", "total_msgs_t380_check"))
                //                 .set(nMsgsT380Check.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsVinNotFound", createMap("help", "total_msgs_vin_not_found"))
                                .set(nMsgsVinNotFound.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsNullVehicleParamMeta", createMap("help", "total_msgs_null_vehicle_param_meta"))
                                .set(nMsgsNullVehicleParamMeta.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsVehicleParamMetaNotFound", createMap("help", "total_msgs_vehicle_param_meta_not_found"))
                                .set(nMsgsVehicleParamMetaNotFound.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsNullVehicleOdoProcMeta", createMap("help", "total_msgs_null_vehicle_odo_proc_meta"))
                                .set(nMsgsNullVehicleOdoProcMeta.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsVehicleOdoProcMetaNotFound", createMap("help", "total_msgs_vehicle_odo_proc_meta_not_found"))
                                .set(nMsgsVehicleOdoProcMetaNotFound.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsProcessingError", createMap("help", "total_msgs_processing_error"))
                                .set(nMsgsProcessingError.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nGpsMsgsInvalidTimestamp", createMap("help", "total_gps_msgs_invalid_timestamp"))
                                .set(nGpsMsgsInvalidTimestamp.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nGpsMsgsOutOfRange", createMap("help", "total_gps_msgs_out_of_range"))
                                .set(nGpsMsgsOutOfRange.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nRegulardataParamMetaNotFound", createMap("help", "total_regulardata_param_meta_not_found"))
                                .set(nRegulardataParamMetaNotFound.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanMsgsInvalidTimestamp", createMap("help", "total_can_msgs_invalid_timestamp"))
                                .set(nCanMsgsInvalidTimestamp.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanMsgsOutOfRange", createMap("help", "total_can_msgs_out_of_range"))
                                .set(nCanMsgsOutOfRange.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsCanOutOfOrder", createMap("help", "total_msgs_can_out_of_order"))
                                .set(nMsgsCanOutOfOrder.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsInvalidRecordType", createMap("help", "total_msgs_invalid_record_type"))
                                .set(nMsgsInvalidRecordType.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsNoRecordType", createMap("help", "total_msgs_no_record_type"))
                                .set(nMsgsNoRecordType.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanMsgsCheckpUpdateFailed", createMap("help", "total_can_msgs_checkp_update_failed"))
                                .set(nCanMsgsCheckpUpdateFailed.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsCanZeroOdo", createMap("help", "total_msgs_can_zero_odo"))
                                .set(nMsgsCanZeroOdo.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanMsgsProcessingError", createMap("help", "total_can_msgs_processing_error"))
                                .set(nCanMsgsProcessingError.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanMsgsSeqNumOutOfOrder", createMap("help", "total_can_msgs_seq_num_out_of_order"))
                                .set(nCanMsgsSeqNumOutOfOrder.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsMaxOOOBeacons", createMap("help", "total_msgs_max_ooo_beacons"))
                                .set(nMsgsMaxOOOBeacons.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanMsgsBufferCheckError", createMap("help", "total_can_msgs_buffer_check_error"))
                                .set(nCanMsgsBufferCheckError.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCheckpToRedisStopped", createMap("help", "total_checkp_to_redis_stopped"))
                                .set(nCheckpToRedisStopped.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCheckpToRedisSkipped", createMap("help", "total_checkp_to_redis_skipped"))
                                .set(nCheckpToRedisSkipped.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCheckpToRedisError", createMap("help", "total_checkp_to_redis_error"))
                                .set(nCheckpToRedisError.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nGpsLivenessInvalidTimestamp", createMap("help", "total_gps_liveness_invalid_timestamp"))
                                .set(nGpsLivenessInvalidTimestamp.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nGpsLivenessOutOfDate", createMap("help", "total_gps_liveness_out_of_date"))
                                .set(nGpsLivenessOutOfDate.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nGpsLivenessProcessingError", createMap("help", "total_gps_liveness_processing_error"))
                                .set(nGpsLivenessProcessingError.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanLivenessInvalidTimestamp", createMap("help", "total_can_liveness_invalid_timestamp"))
                                .set(nCanLivenessInvalidTimestamp.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nCanLivenessProcessingError", createMap("help", "total_can_liveness_processing_error"))
                                .set(nCanLivenessProcessingError.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nMsgsGps", createMap("help", "total_msgs_gps"))
                                .set(nMsgsGps.get(), new HashMap<>());
                // objectNode.put("nMsgsCan", nMsgsCan.get());
                this.metrics.gauge("datahandler.nMsgsCan", createMap("help", "total_msgs_can"))
                                .set(nMsgsCan.get(), new HashMap<>());
                // objectNode.put("nSendClickh", nSendClickh.get());
                this.metrics.gauge("datahandler.nSendClickh", createMap("help", "total_send_clickh"))
                                .set(nSendClickh.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataZeo", createMap("help", "total_send_clickh_raw_regular_data_zeo"))
                                .set(nSendClickhRawRegularDataZeo.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataZeoP2", createMap("help", "total_send_clickh_raw_regular_data_zeop2"))
                                .set(nSendClickhRawRegularDataZeoP2.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataTreo", createMap("help", "total_send_clickh_raw_regular_data_treo"))
                                .set(nSendClickhRawRegularDataTreo.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataTreo2163ah", createMap("help", "total_send_clickh_raw_regular_data_treo2163ah"))
                                .set(nSendClickhRawRegularDataTreo2163ah.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataA301", createMap("help", "total_send_clickh_raw_regular_data_a301"))
                                .set(nSendClickhRawRegularDataA301.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataT305", createMap("help", "total_send_clickh_raw_regular_data_t305"))
                                .set(nSendClickhRawRegularDataT305.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawRegularDataT380", createMap("help", "total_send_clickh_raw_regular_data_t380"))
                                .set(nSendClickhRawRegularDataT380.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawGpsData", createMap("help", "total_send_clickh_raw_gpsdata"))
                                .set(nSendClickhRawGpsData.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawSummaryDataA301", createMap("help", "total_send_clickh_raw_summarydata_a301"))
                                .set(nSendClickhRawSummaryDataA301.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawSummaryDataCommon", createMap("help", "total_send_clickh_raw_summarydata_common"))
                                .set(nSendClickhRawSummaryDataCommon.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawAlertData", createMap("help", "total_send_clickh_raw_alertdata"))
                                .set(nSendClickhRawAlertData.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawTripAlert", createMap("help", "total_send_clickh_raw_tripalert"))
                                .set(nSendClickhRawTripAlert.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawChargeInsight", createMap("help", "total_send_clickh_raw_chargeinsight"))
                                .set(nSendClickhRawChargeInsight.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nSendClickhRawTripInsight", createMap("help", "total_send_clickh_raw_tripinsight"))
                                .set(nSendClickhRawTripInsight.get(), new HashMap<>());
                this.metrics.gauge("datahandler.nRawDataWriteError", createMap("help", "total_raw_data_write_error"))
                                .set(nRawDataWriteError.get(), new HashMap<>());


                return true;
        }

        public boolean getMetrics(String shardId) {
                this.updateMetrics(shardId);
                this.absrpLtstPublisher.getMetrics(shardId);
                this.clickhPusher.getMetrics(shardId);
                if (this.metrics != null) {
                        this.metrics.setComponentHealth("clickhouse", this.isPusherAlive.get());
                        this.metrics.setComponentHealth("postgres",
                                        this.vehicleParamFetch != null && this.vehicleParamFetch.isConnectionOpen());
                }
                return true;
        }

        public void recordShardCheckpointTime(String shardId, Instant checkpointTime) {
                if (shardId == null || checkpointTime == null) {
                        return;
                }
                this.lastShardIdCheckpointTime.compute(shardId,
                                (key, existing) -> (existing == null || checkpointTime.isAfter(existing))
                                                ? checkpointTime
                                                : existing);
        }

        public Optional<Instant> getLatestShardCheckpointTime() {
                return this.lastShardIdCheckpointTime.values().stream().max(Instant::compareTo);
        }

        public Optional<Instant> getOldestShardCheckpointTime() {
                return this.lastShardIdCheckpointTime.values().stream().min(Instant::compareTo);
        }

        public Map<String, Instant> getShardCheckpointSnapshot() {
                return new HashMap<>(this.lastShardIdCheckpointTime);
        }

        public boolean hasShardCheckpointTimes() {
                return !this.lastShardIdCheckpointTime.isEmpty();
        }

        public static int sizeOf(byte element) {
                return 1;
        }

        public static int sizeOf(short element) {
                return 2;
        }

        public static int sizeOf(int element) {
                return 4;
        }

        public static int sizeOf(long element) {
                return 8;
        }

        public static int sizeOf(float element) {
                return 4;
        }

        public static int sizeOf(double element) {
                return 8;
        }

        public static int sizeOf(boolean element) {
                return 10;
        }

        /**
         * Creates a deep copy of an AllData object to prevent shared references
         */
        private DataCheckp.CanData createDeepCopy(DataCheckp.CanData original) {
                if (original == null) {
                        return new DataCheckp.CanData();
                }

                DataCheckp.CanData copy = new DataCheckp.CanData();
                copy.setVin(original.getVin());
                copy.setImei(original.getImei());
                copy.setModel(original.getModel());
                copy.setValid(original.getValid());
                copy.setPackettype(original.getPackettype());
                copy.setUtctime(original.getUtctime());
                copy.setIntime(original.getIntime());
                copy.setProctime(original.getProctime());
                copy.setTstamp(original.getTstamp());
                copy.setCycletime(original.getCycletime());
                copy.setCharger_sts_live(original.getCharger_sts_live());
                copy.setBms_charger_plug_in_sts(original.getBms_charger_plug_in_sts());
                copy.setDrive_enable(original.getDrive_enable());
                copy.setBms_hvbatt_connect_sts(original.getBms_hvbatt_connect_sts());
                copy.setBms_hvbatt_disconnect_sts(original.getBms_hvbatt_disconnect_sts());
                copy.setBms_in_keyon(original.getBms_in_keyon());
                copy.setBms_cyclenum(original.getBms_cyclenum());
                copy.setCharger_temp_live(original.getCharger_temp_live());
                copy.setChargertemp(original.getChargertemp());
                copy.setSoc(original.getSoc());
                copy.setSoh(original.getSoh());
                copy.setBms_cell_avg_volt(original.getBms_cell_avg_volt());
                copy.setBms_cell_avg_temp(original.getBms_cell_avg_temp());
                copy.setMotortemp(original.getMotortemp());
                copy.setBms_batt_pack_temp(original.getBms_batt_pack_temp());
                copy.setBrake_switch_status(original.getBrake_switch_status());
                copy.setOdometer(original.getOdometer());
                copy.setVehiclespeed(original.getVehiclespeed());
                copy.setMotorpower(original.getMotorpower());
                copy.setE_motor_rpm(original.getE_motor_rpm());
                copy.setInv_drivemode_shift(original.getInv_drivemode_shift());
                copy.setV_mode(original.getV_mode());
                copy.setBattery_ttc(original.getBattery_ttc());
                copy.setPercthrottle(original.getPercthrottle());
                copy.setKwh(original.getKwh());
                copy.setBatt_current(original.getBatt_current());
                copy.setAux_batt_volt(original.getAux_batt_volt());
                copy.setBat_voltage(original.getBat_voltage());
                copy.setBus_voltage(original.getBus_voltage());
                copy.setDte(original.getDte());
                copy.setCan_throttle(original.getCan_throttle());
                copy.setWakeup_command(original.getWakeup_command());
                copy.setVeh_immo_resp(original.getVeh_immo_resp());
                copy.setService_tt(original.getService_tt());
                copy.setChargett(original.getChargett());
                copy.setConnectionhealthtt(original.getConnectionhealthtt());
                copy.setTcu_speedmode(original.getTcu_speedmode());

                return copy;
        }
}
