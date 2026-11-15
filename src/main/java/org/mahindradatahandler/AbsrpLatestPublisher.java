package org.mahindradatahandler;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.mahindradatahandler.DataCheckp.LivenessData;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import in.intellicar.infra.lognmetricswriter.observability.*;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import java.util.concurrent.ExecutorService;
// import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Connection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
// import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.stream.Collectors;

/* Created By
 * Omkar G
 * Intellicar
 * 1 July 2025
 */

public class AbsrpLatestPublisher extends Thread {
    private Logger log;
    private Metrics metrics;

    public static final String LIVENESS_CHECKPOINT_KEY = "checkp.liveness.";

    // private final Object lock = new Object();
    private final Object msgLock = new Object();
    private String clientId;
    private String applicationName;
    private JedisCluster jedisCluster;
    private boolean connected;
    private long lastSuccessfulConnectAt;

    // Metrics
    private int deliveryComplete;
    private int noOfConnect;
    private int noOfExceptions;
    private int noOfMsgSent;
    private int noOfSubRecv;

    private DataCheckp checkpoint;

    private AtomicBoolean isStopped = new AtomicBoolean(false);
    private AtomicInteger nMsgsPublished = new AtomicInteger(0);
    private AtomicInteger nMsgsInFlight = new AtomicInteger(0);

    // private ConcurrentHashMap<String, String> redisMsgMap = new
    // ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ClickhMsg> clickhValnodeMap = new ConcurrentHashMap<>();
    private ArrayList<String> redisLRSList = new ArrayList<>();

    // Callback interfaces
    public interface PublishCallback {
        void onSuccess(String channelName);

        void onFailure(Throwable cause);
    }

    public interface ConnectionCallback {
        void onConnect();

        void onDisconnect();

        void onConnectionFailure(Throwable cause);
    }

    private ConnectionCallback connectionCallback;
    private PublishCallback publishCallback;
    private ClickhPusher clickhPusher;
    private DataHandler dataHandler;
    private VehicleParamFetch vehicleParamFetch;
    private int LIVENESS_CHCKP_REDIS_KEY_TTL = 172800;

    public AbsrpLatestPublisher(String applicationName, String redisHost, int redisPort, ClickhPusher clickhPusher,
            DataCheckp checkpoint, Logger log, Metrics metrics, VehicleParamFetch vehicleParamFetch) {
        this.log = log;
        this.metrics = metrics;
        try {
            this.clientId = "redispublisher_" + applicationName;
            this.applicationName = applicationName;
            this.connected = false;
            this.clickhPusher = clickhPusher;
            this.checkpoint = checkpoint;
            this.vehicleParamFetch = vehicleParamFetch;

            // Configure JedisCluster
            Set<HostAndPort> jedisClusterNodes = new HashSet<>();
            jedisClusterNodes.add(new HostAndPort(redisHost, redisPort));

            // Improved connection pool configuration
            GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(20); // Increased pool size
            poolConfig.setMaxIdle(10); // More idle connections
            poolConfig.setMinIdle(2); // Always keep some connections ready
            poolConfig.setMaxWaitMillis(5000); // Wait timeout for getting connection
            poolConfig.setTestOnBorrow(true); // Test connection before use
            poolConfig.setTestOnReturn(true); // Test connection before return
            poolConfig.setTestWhileIdle(true); // Test idle connections
            poolConfig.setTimeBetweenEvictionRunsMillis(30000); // Check every 30 seconds
            poolConfig.setMinEvictableIdleTimeMillis(60000); // Evict after 1 minute idle
            poolConfig.setNumTestsPerEvictionRun(3); // Test 3 connections per run
            poolConfig.setBlockWhenExhausted(true); // Block when pool exhausted

            // Initialize JedisCluster with improved configuration
            this.jedisCluster = new JedisCluster(
                    jedisClusterNodes,
                    10000, // connection timeout (reduced from 30s to 10s)
                    10000, // socket timeout (reduced from 30s to 10s)
                    5, // max attempts (increased from 3 to 5)
                    poolConfig);

            initMetrics();
        } catch (Exception e) {
            this.log.error("Error initializing AbsrpLatestPublisher", e);
            throw new RuntimeException("Failed to initialize AbsrpLatestPublisher");
        }
    }

    public void initMetrics() {
        deliveryComplete = 0;
        noOfConnect = 0;
        noOfExceptions = 0;
        noOfMsgSent = 0;
        noOfSubRecv = 0;
    }

    public void connect(ConnectionCallback callback) {
        this.connectionCallback = callback;
        try {
            // Test connection by simple operation
            this.jedisCluster.ping();
            this.connected = true;
            noOfConnect++;
            if (connectionCallback != null) {
                connectionCallback.onConnect();
            }
            this.lastSuccessfulConnectAt = System.currentTimeMillis();
            this.log.info("Successfully connected to Redis cluster");
        } catch (Exception e) {
            this.connected = false;
            noOfExceptions++;
            if (connectionCallback != null) {
                connectionCallback.onConnectionFailure(e);
            }
            this.log.error("Failed to connect to Redis Cluster", e);
        }
    }

    // New method to validate connection health
    private boolean isConnectionHealthy() {
        if (!connected || jedisCluster == null) {
            return false;
        }

        try {
            // Quick ping to verify connection is alive
            String pong = jedisCluster.ping();
            return "PONG".equals(pong);
        } catch (Exception e) {
            this.log.warn("Connection health check failed " + e);
            connected = false;
            return false;
        }
    }

    // Improved connection recovery with exponential backoff
    private boolean attemptReconnection() {
        if (connected && isConnectionHealthy()) {
            return true;
        }

        try {
            if (jedisCluster != null && !connected) {
                // Try to reconnect using existing cluster
                connect(connectionCallback);
                return connected;
            }
        } catch (Exception e) {
            this.log.warn("Reconnection attempt failed " + e);
        }

        return false;
    }

    public void disconnect() {
        try {
            if (this.jedisCluster != null) {
                this.jedisCluster.close();
            }
            this.connected = false;
            if (connectionCallback != null) {
                connectionCallback.onDisconnect();
            }
        } catch (Exception e) {
            noOfExceptions++;
            this.log.error("Error disconnecting from Redis Cluster", e);
        }
    }

    public void startThread(DataHandler dataHandler) {
        this.dataHandler = dataHandler;
        this.start();
    }

    public void stopThread() {
        this.isStopped.set(true);
        disconnect();
    }

    public boolean publish(String channel, String shardid, String sequenceNumber, long utctime,
            int recordsize,
            ObjectNode valnode, Instant approximateArrivalTimestamp) {
        synchronized (msgLock) {
            if (!clickhValnodeMap.containsKey(channel)) {
                this.redisLRSList.add(channel);
            }
            // redisMsgMap.put(channel, message);
            clickhValnodeMap.put(channel,
                    new ClickhMsg(shardid, sequenceNumber, utctime, recordsize, valnode, approximateArrivalTimestamp));
        }
        nMsgsPublished.incrementAndGet();
        return true;
    }

    public boolean publishOnlyRedis(String channel, String message) {
        publishToRedis(channel, message);
        nMsgsPublished.incrementAndGet();
        return true;
    }

    private boolean publishToRedis(String channel, String message) {
        try {
            // Check connection health before attempting to publish
            if (!isConnectionHealthy()) {
                this.log.warn("Connection not healthy, attempting reconnection");
                if (!attemptReconnection()) {
                    this.log.error("Failed to establish healthy connection for publishing");
                    return false;

                }

            }
            nMsgsInFlight.incrementAndGet();

            // set and publish for persistence and realtime
            String result = jedisCluster.setex(channel, LIVENESS_CHCKP_REDIS_KEY_TTL, message);
            long publishResult = jedisCluster.publish(channel, message);
            // Update metrics
            noOfSubRecv = (int) publishResult;
            noOfMsgSent++;
            deliveryComplete++;
            nMsgsInFlight.decrementAndGet();

            if (publishCallback != null) {
                publishCallback.onSuccess(channel);
            }
            // Update last successful operation time
            this.lastSuccessfulConnectAt = System.currentTimeMillis();
            return "OK".equals(result);
        } catch (Exception e) {
            nMsgsInFlight.decrementAndGet();
            noOfExceptions++;
            connected = false; // Mark connection as bad

            if (publishCallback != null) {
                publishCallback.onFailure(e);
            }
            this.log.error("Error publishing to Redis channel: " + channel, e);
            return false;
        }
    }

    private boolean publishToClickh(String channel, ClickhMsg clickhMsg) {
        try {
            if (!this.clickhPusher.isClickhInit) {
                this.log.error("Clickh is not initialized, skipping latest data publish");
                return false;
            }

            if (channel.contains("gpsinfo")) {
                this.clickhPusher.sendGlobal(clickhMsg.shardid, clickhMsg.sequenceNumber, clickhMsg.recordsize,
                        clickhMsg.valnode, clickhMsg.approximateArrivalTimestamp);
            } else if (channel.contains("caninfo")) {
                this.clickhPusher.sendCanGlobal(clickhMsg.shardid, clickhMsg.sequenceNumber, clickhMsg.recordsize,
                        clickhMsg.valnode, clickhMsg.approximateArrivalTimestamp);
            } else if (channel.startsWith(LIVENESS_CHECKPOINT_KEY)) {
                this.clickhPusher.sendLivenessData(clickhMsg.shardid, clickhMsg.sequenceNumber, clickhMsg.recordsize,
                        clickhMsg.valnode, clickhMsg.approximateArrivalTimestamp);
                this.publishOnlyRedis(channel, clickhMsg.valnode.toString());
            }
        } catch (Exception e) {
            // noOfExceptions++;
            this.log.error("Error publishing latest data to Clickh " + e);
        }
        return true;
    }

    private boolean sendNextRedisMsg() {
        if (nMsgsInFlight.get() > 300) {
            return false;
        }

        String nextChannel = null;
        // String value = null;
        ClickhMsg clickhMsg = null;

        synchronized (msgLock) {
            if (redisLRSList.size() > 0) {
                nextChannel = redisLRSList.remove(0);
            }

            if (nextChannel != null && clickhValnodeMap.containsKey(nextChannel)) {
                clickhMsg = clickhValnodeMap.remove(nextChannel);
            }
        }

        if (nextChannel != null && clickhMsg != null) {
            return publishToClickh(nextChannel, clickhMsg);
        }
        return false;
    }

    @Override
    public void run() {
        this.log.info("Absrp Latest Publisher Thread started");
        // long lastSuccessfulMsgAt = System.currentTimeMillis();

        while (!this.isStopped.get()) {
            try {

                boolean isSuccess = sendNextRedisMsg();
                if (!isSuccess) {
                    Thread.sleep(500);
                }
                Thread.sleep(1); // induced delay to avoid high writes
            } catch (Exception e) {
                this.log.error("Exception while handling Absrp Latest message " + e);
                // noOfExceptions++;
            }
        }

        // disconnect();
        this.log.info("Absrp Latest Publisher Thread stopped");
    }

    public boolean getMetrics(String shardId) {
        // ObjectMapper objectMapper = new ObjectMapper();
        // ObjectNode objectNode = objectMapper.createObjectNode();
        // objectNode.put("nMsgsInFlight", nMsgsInFlight.get());
        this.metrics.gauge("redispub.nMsgsInFlight", dataHandler.createMap("help", "total_msgs_in_flight"))
                               .set(nMsgsInFlight.get(), dataHandler.createMap("shardid", shardId));
        // objectNode.put("deliveryComplete", deliveryComplete);
        this.metrics.gauge("redispub.deliveryComplete", dataHandler.createMap("help", "total_delivery_complete"))
                               .set(deliveryComplete, dataHandler.createMap("shardid", shardId));
        this.metrics.gauge("redispub.noOfConnect", dataHandler.createMap("help", "total_connect"))
                               .set(noOfConnect, dataHandler.createMap("shardid", shardId));
        this.metrics.gauge("redispub.noOfExceptions", dataHandler.createMap("help", "total_exceptions"))
                               .set(noOfExceptions, dataHandler.createMap("shardid", shardId));
        this.metrics.gauge("redispub.noOfMsgSent", dataHandler.createMap("help", "total_msgs_sent"))
                               .set(noOfMsgSent, dataHandler.createMap("shardid", shardId));
        // objectNode.put("noOfMsgPublished", nMsgsPublished.get());
        this.metrics.gauge("redispub.noOfMsgPublished", dataHandler.createMap("help", "total_msgs_published"))
                               .set(nMsgsPublished.get(), dataHandler.createMap("shardid", shardId));
        // objectNode.put("noOfSubRecv", noOfSubRecv);
        this.metrics.gauge("redispub.noOfSubRecv", dataHandler.createMap("help", "total_sub_recv"))
                               .set(noOfSubRecv, dataHandler.createMap("shardid", shardId));

        // Add connection and queue status
        // objectNode.put("connected", connected);
        // objectNode.put("lastSuccessfulConnectAt", lastSuccessfulConnectAt);

        // Add queue metrics
        synchronized (msgLock) {
            // objectNode.put("pendingMessages", redisLRSList.size());
            this.metrics.gauge("redispub.clickhPendingMessages", dataHandler.createMap("help", "clickh_pending_messages"))
                               .set(redisLRSList.size(), dataHandler.createMap("shardid", shardId));
            // objectNode.put("messageMapSize", redisMsgMap.size());
            this.metrics.gauge("redispub.clickhValnodeMapSize", dataHandler.createMap("help", "clickh_valnode_map_size"))
                               .set(clickhValnodeMap.size(), dataHandler.createMap("shardid", shardId));
        }

        return true;
    }

    public void setPublishCallback(PublishCallback callback) {
        this.publishCallback = callback;
    }

    public static class ClickhMsg {
        String sequenceNumber;
        long utctime;
        int recordsize;
        ObjectNode valnode;
        String shardid;
        Instant approximateArrivalTimestamp;

        public ClickhMsg(String shardid, String sequenceNumber, long utctime, int recordsize, ObjectNode valnode,
                Instant approximateArrivalTimestamp) {
            this.sequenceNumber = sequenceNumber;
            this.utctime = utctime;
            this.recordsize = recordsize;
            this.valnode = valnode;
            this.shardid = shardid;
            this.approximateArrivalTimestamp = approximateArrivalTimestamp;
        }
    }

    public boolean getAndUpdateLivenessCheckp(String partitionKey) {
        while (true) {
            if (!connected || jedisCluster == null) {
                try {
                    connect(connectionCallback);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    this.log.error("getAndUpdateCheckpVin: Error getting and updating checkpoint " + e);
                }
                continue;
            }
            break;
        }

        String data = jedisCluster.get(LIVENESS_CHECKPOINT_KEY + partitionKey);
        if (data != null && !data.isEmpty()) {
            try {
                // LivenessData livenessData = new ObjectMapper().readValue(data, LivenessData.class);
                LivenessData livenessData = new LivenessData();
                // parse redis liveness data to get all fields
                JsonNode dataNode = new ObjectMapper().readTree(data);
                if (dataNode.isObject()) {
                    ObjectNode objectNode = (ObjectNode) dataNode;
                    livenessData.setVin(objectNode.has("vin") ? objectNode.get("vin").asText() : "");
                    livenessData.setUtc_day_b(objectNode.has("utc_day_b") ? objectNode.get("utc_day_b").asInt() : 0);
                    livenessData.setGps_cnt(objectNode.has("gps_cnt") ? objectNode.get("gps_cnt").asInt() : 0);
                    livenessData.setCan_cnt(objectNode.has("can_cnt") ? objectNode.get("can_cnt").asInt() : 0);
                    livenessData.setGps_first(objectNode.has("gps_first") ? objectNode.get("gps_first").asText() : "");
                    livenessData.setGps_last(objectNode.has("gps_last") ? objectNode.get("gps_last").asText() : "");
                    livenessData.setCan_first(objectNode.has("can_first") ? objectNode.get("can_first").asText() : "");
                    livenessData.setCan_last(objectNode.has("can_last") ? objectNode.get("can_last").asText() : "");
                    livenessData.setProctime(objectNode.has("proctime") ? objectNode.get("proctime").asLong() : 0L);
                }
                synchronized (this.checkpoint) {
                    this.checkpoint.saveLivenessCheckpoint(partitionKey, livenessData);
                }
            } catch (Exception e) {
                this.log.error("getAndUpdateLivenessCheckp: Error getting and updating checkpoint " + e);
                return false;
            }
        }
        return true;
    }

    public boolean getAndUpdateCheckp(String mode) {
        while (true) {
            if (!connected || jedisCluster == null) {
                try {
                    connect(connectionCallback);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    this.log.error("Error getting and updating checkpoint " + e);
                }
                continue;
            }
            break;
        }

        // this.log.info("Getting and updating feeders checkpoint in mode: " + mode);
        try {
            // if (mode.equals("initialization")) {
            //     String appver = jedisCluster.get("checkp." + this.applicationName + ".appver");
            //     if (appver != null && !appver.isEmpty()) {
            //         this.dataHandler.appVersion = Integer.parseInt(appver) + 1;
            //     }
            // }

            String data = jedisCluster.get("checkp." + this.applicationName);
            if (data != null && !data.isEmpty()) {
                JsonNode dataNode = new ObjectMapper().readTree(data);
                if (dataNode.isObject()) {
                    ObjectNode objectNode = (ObjectNode) dataNode;
                    Iterator<String> fieldNames = objectNode.getFieldNames();
                    while (fieldNames.hasNext()) {
                        String shardid = fieldNames.next();
                        JsonNode timestampNode = objectNode.get(shardid);
                        if (timestampNode.isObject()) {
                            try {
                                long epochSecond = timestampNode.get("epochSecond").asLong();
                                long nano = timestampNode.get("nano").asLong();
                                Instant approximateArrivalTimestamp = Instant.ofEpochSecond(epochSecond, nano);
                                // compare with available checkpoint time and update checkpoint map
                                // if approximateArrivalTimestamp is greater than available checkpoint time
                                this.dataHandler.recordShardCheckpointTime(shardid, approximateArrivalTimestamp);
                                this.log.info("Shardid: " + shardid + " Approximate Arrival Timestamp: " + approximateArrivalTimestamp);
                            } catch (Exception e) {
                                this.log.warn("Invalid timestamp format for shard " + shardid + ": " + timestampNode.toString());
                            }
                        } else {
                            this.log.warn("Unexpected timestamp format for shard: " + shardid);
                        }
                    }
                }
            }
        } catch (Exception e) {
            this.log.error("Error getting and updating checkpoint " + e);
        }

        return true;
    }

    public boolean getAndUpdateCheckpAllvin() {
        while (true) {
                if (!connected || jedisCluster == null) {
                        try {
                                connect(connectionCallback);
                                Thread.sleep(10000);
                        } catch (InterruptedException e) {
                                this.log.error("Error getting and updating checkpoint", e);
                        }
                        continue;
                }
                break;
        }

        // log.info("Getting and updating checkpoint for {} vehicles",
        // this.vehicleParamFetch.vinList.size());
        // Use a reasonable thread pool size (max 20 threads or number of vehicles,
        // whichever is smaller)
        int threadPoolSize = Math.min(20, this.vehicleParamFetch.vinList.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        try {
                final AtomicInteger totalNoMismatch = new AtomicInteger(0);
                List<CompletableFuture<Void>> futures = this.vehicleParamFetch.vinList.stream()
                                .map(vin -> CompletableFuture.runAsync(() -> {
                                        try {
                                                String canData = jedisCluster
                                                                .get("checkp.gcfeeder.candatarecord." + vin);
                                                if (canData != null && !canData.isEmpty()) {
                                                        JsonNode dataRecordNode = new ObjectMapper()
                                                                        .readTree(canData);
                                                        DataCheckp.CanDataRecord canDataRecord = getAndUpdateCanDataRecord(
                                                                        vin, dataRecordNode);
                                                        if (canDataRecord != null) {
                                                                if (canDataRecord.canDataInmemList
                                                                                .size() == 0) {
                                                                        totalNoMismatch.incrementAndGet();
                                                                }
                                                        }
                                                        synchronized (this.checkpoint) {
                                                                this.checkpoint.saveCanDataRecordCheckpoint(vin,
                                                                                canDataRecord);
                                                        }
                                                }
                                        } catch (Exception e) {
                                                this.log.error("Error getting and updating checkpoint for vin: " + vin, e);
                                        }
                                }, executor))
                                .collect(Collectors.toList());

                // Wait for all tasks to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                this.log.info("Completed processing checkpoints for " + this.vehicleParamFetch.vinList.size() + " vehicles, total no of mismatch: " + totalNoMismatch.get());
        } finally {
                // Shutdown the executor service
                executor.shutdown();
        }

        return true;
    }


    public boolean getAndUpdateCheckpVin(String vin) {
        while (true) {
                if (!connected || jedisCluster == null) {
                        try {
                                connect(connectionCallback);
                                Thread.sleep(10000);
                        } catch (InterruptedException e) {
                                this.log.error("Error getting and updating checkpoint", e);
                        }
                        continue;
                }
                break;
        }

        try {
                String dataRecord = jedisCluster
                                .get("checkp.gcfeeder.candatarecord." + vin);
                if (dataRecord != null && !dataRecord.isEmpty()) {
                        JsonNode dataRecordNode = new ObjectMapper()
                                        .readTree(dataRecord);
                        DataCheckp.CanDataRecord canDataRecord = getAndUpdateCanDataRecord(
                                        vin, dataRecordNode);
                        synchronized (this.checkpoint) {
                                this.checkpoint.saveCanDataRecordCheckpoint(vin,
                                                canDataRecord);
                        }
                }
        } catch (Exception e) {
                this.log.error("Error getting and updating checkpoint for vin: " + vin, e);
        }

        return true;
    }

    public DataCheckp.CanDataRecord getAndUpdateCanDataRecord(String vin, JsonNode canDataNode) {
        ArrayList<DataCheckp.CanData> canDataInmemList = new ArrayList<>();
        ArrayList<String> sequenceNumberList = new ArrayList<>();
        ArrayList<Instant> approximateArrivalTimestampList = new ArrayList<>();
        Instant lastApproximateArrivalTimestamp = canDataNode.has("lastApproximateArrivalTimestamp")
                        && !canDataNode.get("lastApproximateArrivalTimestamp").asText().isEmpty()
                                        ? Instant.parse(canDataNode.get("lastApproximateArrivalTimestamp")
                                                        .asText())
                                        : Instant.now();
        DataCheckp.CanData lastData = canDataNode.has("canlastData")
                        ? getAndUpdateCanData(vin, canDataNode.get("canlastData"))
                        : new DataCheckp.CanData();
        String lastSequenceNumber = canDataNode.has("lastSequenceNumber")
                        ? canDataNode.get("lastSequenceNumber").asText()
                        : "";

        JsonNode inmemList = canDataNode.get("canDataInmemList");
        JsonNode sequenceList = canDataNode.get("sequenceNumberList");
        JsonNode approximateArrivalTimestampListNode = canDataNode.get("approximateArrivalTimestampList");
        int checkpCounter = 0;

        // Handle allDataInmemList array
        if (inmemList != null && inmemList.isArray()) {
                for (JsonNode node : inmemList) {
                        canDataInmemList.add(getAndUpdateCanData(vin, node));
                }
                canDataInmemList.sort((a, b) -> Long.compare(a.getUtctime(), b.getUtctime()));
        }

        // Handle sequenceNumberList array
        if (sequenceList != null && sequenceList.isArray()) {
                for (JsonNode node : sequenceList) {
                        sequenceNumberList.add(node.asText());
                }
        }

        // Handle approximateArrivalTimestampList array
        if (approximateArrivalTimestampListNode != null && approximateArrivalTimestampListNode.isArray()) {
                for (JsonNode timestampNode : approximateArrivalTimestampListNode) {
                        if (timestampNode.isObject()) {
                                try {
                                        long epochSecond = timestampNode.get("epochSecond")
                                                        .asLong();
                                        long nano = timestampNode.get("nano").asLong();
                                        Instant approximateArrivalTimestamp = Instant
                                                        .ofEpochSecond(epochSecond, nano);
                                        approximateArrivalTimestampList.add(approximateArrivalTimestamp);
                                } catch (Exception e) {
                                        this.log.warn("Invalid timestamp format for shard " + timestampNode.toString());
                                }
                        } else {
                                this.log.warn("Unexpected timestamp format for shard: " + timestampNode.toString());
                        }
                }
        }

        if (canDataInmemList.size() != sequenceNumberList.size()
                        || canDataInmemList.size() != approximateArrivalTimestampList.size()
                        || sequenceNumberList.size() != approximateArrivalTimestampList.size()) {
                this.log.error("canDataInmemList " + canDataInmemList.size() + ", sequenceNumberList " + sequenceNumberList.size() + ", approximateArrivalTimestampList " + approximateArrivalTimestampList.size() + " size mismatch for vin: " + vin);
                canDataInmemList.clear();
                sequenceNumberList.clear();
                approximateArrivalTimestampList.clear();
        }

        DataCheckp.CanDataRecord canDataRecord = new DataCheckp.CanDataRecord(canDataInmemList, sequenceNumberList,
                        approximateArrivalTimestampList, lastData,
                        lastSequenceNumber, lastApproximateArrivalTimestamp, checkpCounter);

        return canDataRecord;
    }

    public DataCheckp.CanData getAndUpdateCanData(String vin, JsonNode dataNode) {
        DataCheckp.CanData canData = new DataCheckp.CanData();
        canData.setVin(vin);
        canData.setImei(dataNode.has("imei") ? dataNode.get("imei").asText() : "");
        canData.setModel(dataNode.has("model") ? dataNode.get("model").asText() : "");
        canData.setValid(dataNode.has("valid") ? dataNode.get("valid").asText() : "");
        canData.setPackettype(dataNode.has("packettype") ? dataNode.get("packettype").asText() : "");
        canData.setUtctime(dataNode.has("utctime") ? dataNode.get("utctime").asLong() : 0L);
        canData.setIntime(dataNode.has("intime") ? dataNode.get("intime").asLong() : 0L);
        canData.setProctime(dataNode.has("proctime") ? dataNode.get("proctime").asLong() : 0L);
        canData.setTstamp(dataNode.has("tstamp") ? dataNode.get("tstamp").asText() : "");
        canData.setCycletime(dataNode.has("cycletime") ? dataNode.get("cycletime").asText() : "");
        canData.setCharger_sts_live(dataNode.has("charger_sts_live") ? dataNode.get("charger_sts_live").asText() : "");
        canData.setBms_charger_plug_in_sts(dataNode.has("bms_charger_plug_in_sts") ? dataNode.get("bms_charger_plug_in_sts").asText() : "");
        canData.setDrive_enable(dataNode.has("drive_enable") ? dataNode.get("drive_enable").asText() : "");
        canData.setBms_hvbatt_connect_sts(dataNode.has("bms_hvbatt_connect_sts") ? dataNode.get("bms_hvbatt_connect_sts").asText() : "");
        canData.setBms_hvbatt_disconnect_sts(dataNode.has("bms_hvbatt_disconnect_sts") ? dataNode.get("bms_hvbatt_disconnect_sts").asText() : "");
        canData.setBms_in_keyon(dataNode.has("bms_in_keyon") ? dataNode.get("bms_in_keyon").asText() : "");
        canData.setBms_cyclenum(dataNode.has("bms_cyclenum") ? dataNode.get("bms_cyclenum").asInt() : 0);
        canData.setCharger_temp_live(dataNode.has("charger_temp_live") ? (float) dataNode.get("charger_temp_live").asDouble() : 0.0f);
        canData.setChargertemp(dataNode.has("chargertemp") ? (float) dataNode.get("chargertemp").asDouble() : 0.0f);
        canData.setSoc(dataNode.has("soc") ? (float) dataNode.get("soc").asDouble() : 0.0f);
        canData.setSoh(dataNode.has("soh") ? dataNode.get("soh").asInt() : 0);
        canData.setBms_cell_avg_volt(dataNode.has("bms_cell_avg_volt") ? (float) dataNode.get("bms_cell_avg_volt").asDouble() : 0.0f);
        canData.setBms_cell_avg_temp(dataNode.has("bms_cell_avg_temp") ? (float) dataNode.get("bms_cell_avg_temp").asDouble() : 0.0f);
        canData.setMotortemp(dataNode.has("motortemp") ? (float) dataNode.get("motortemp").asDouble() : 0.0f);
        canData.setBms_batt_pack_temp(dataNode.has("bms_batt_pack_temp") ? (float) dataNode.get("bms_batt_pack_temp").asDouble() : 0.0f);
        canData.setBrake_switch_status(dataNode.has("brake_switch_status") ? dataNode.get("brake_switch_status").asInt() : 0);
        canData.setOdometer(dataNode.has("odometer") ? (float) dataNode.get("odometer").asDouble() : 0.0f);
        canData.setVehiclespeed(dataNode.has("vehiclespeed") ? (float) dataNode.get("vehiclespeed").asDouble() : 0.0f);
        canData.setMotorpower(dataNode.has("motorpower") ? (float) dataNode.get("motorpower").asDouble() : 0.0f);
        canData.setE_motor_rpm(dataNode.has("e_motor_rpm") ? dataNode.get("e_motor_rpm").asInt() : 0);
        canData.setInv_drivemode_shift(dataNode.has("inv_drivemode_shift") ? dataNode.get("inv_drivemode_shift").asInt() : 0);
        canData.setV_mode(dataNode.has("v_mode") ? dataNode.get("v_mode").asInt() : 0);
        canData.setVehicle_phase(dataNode.has("vehicle_phase") ? dataNode.get("vehicle_phase").asInt() : 0);
        canData.setBattery_ttc(dataNode.has("battery_ttc") ? dataNode.get("battery_ttc").asInt() : 0);
        canData.setPercthrottle(dataNode.has("percthrottle") ? (float) dataNode.get("percthrottle").asDouble() : 0.0f);
        canData.setKwh(dataNode.has("kwh") ? (float) dataNode.get("kwh").asDouble() : 0.0f);
        canData.setBatt_current(dataNode.has("batt_current") ? (float) dataNode.get("batt_current").asDouble() : 0.0f);
        canData.setAux_batt_volt(dataNode.has("aux_batt_volt") ? (float) dataNode.get("aux_batt_volt").asDouble() : 0.0f);
        canData.setBat_voltage(dataNode.has("bat_voltage") ? (float) dataNode.get("bat_voltage").asDouble() : 0.0f);
        canData.setBus_voltage(dataNode.has("bus_voltage") ? (float) dataNode.get("bus_voltage").asDouble() : 0.0f);
        canData.setDte(dataNode.has("dte") ? (float) dataNode.get("dte").asDouble() : 0.0f);
        canData.setCan_throttle(dataNode.has("can_throttle") ? dataNode.get("can_throttle").asInt() : 0);
        canData.setWakeup_command(dataNode.has("wakeup_command") ? (float) dataNode.get("wakeup_command").asDouble() : 0.0f);
        canData.setVeh_immo_resp(dataNode.has("veh_immo_resp") ? dataNode.get("veh_immo_resp").asInt() : 0);
        canData.setService_tt(dataNode.has("service_tt") ? dataNode.get("service_tt").asInt() : 0);
        canData.setChargett(dataNode.has("chargett") ? dataNode.get("chargett").asInt() : 0);
        canData.setConnectionhealthtt(dataNode.has("connectionhealthtt") ? dataNode.get("connectionhealthtt").asInt() : 0);
        canData.setTcu_speedmode(dataNode.has("tcu_speedmode") ? dataNode.get("tcu_speedmode").asInt() : 0);

        return canData;
}
}