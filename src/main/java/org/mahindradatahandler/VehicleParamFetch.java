package org.mahindradatahandler;

import org.codehaus.jackson.JsonNode;
import java.sql.*;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.HashSet;
import in.intellicar.infra.lognmetricswriter.observability.*;

public class VehicleParamFetch extends Thread {
    private Logger log;
    final Object lock = new Object();
    private long lastParamRefreshTime;
    private long lastVehicleRefreshTime;
    private long paramRefreshInterval;
    private long vehicleRefreshInterval;
    private boolean stopped;
    private String vmquery;
    private String paramquery;
    private String username;
    private String password;
    private String url;
    private PreparedStatement vehicleStatement = null;
    private PreparedStatement paramStatement = null;
    private int timeout = 0;
    private Connection connection = null;
    private JsonNode properties;
    private String querydbschema;

    public HashSet<String> vinList = new HashSet<>();
    public HashMap<String, HashMap<String, HashMap<String, String>>> modelParamMetaMap = new HashMap<>();
    // model -> paramfamilycode -> paramcode -> paramvalue
    public HashMap<String, HashMap<String, HashMap<String, Boolean>>> modelOdoProcMap = new HashMap<>();
    // model -> paramfamilycode -> paramcode -> odozeroprocessing
    public HashMap<String, String> vinModelMap = new HashMap<>();
    // vin -> model

    public VehicleParamFetch(JsonNode properties, Logger log) {
        this.properties = properties;
        this.log = log;
        this.stopped = false;
        this.lastParamRefreshTime = 0;
        this.paramRefreshInterval = properties.has("refreshInterval") ? properties.get("refreshInterval").asInt()
                : 15 * 60 * 1000;
        this.lastVehicleRefreshTime = 0;
        this.vehicleRefreshInterval = properties.has("refreshInterval") ? properties.get("refreshInterval").asInt()
                : 15 * 60 * 1000;
    }

    public boolean init() {
        if (!this.properties.has("host")) {
            this.log.error("Failed to initialize VehicleParamFetch: missing host in meta db properties");
            return false;
        }
        this.url = this.properties.get("host").asText();
        this.username = this.properties.has("username") ? this.properties.get("username").asText() : "username";
        this.password = this.properties.has("password") ? this.properties.get("password").asText() : "password";
        this.querydbschema = this.properties.has("querydbschema") ? this.properties.get("querydbschema").asText()
                : null;
        this.timeout = this.properties.has("timeout") ? this.properties.get("timeout").asInt() : 30;
        this.connection = getConnection();

        this.vmquery = "select v.vinno, vm.modelfamilycode from " + this.querydbschema + ".vehicle v join "
                + this.querydbschema + ".vehicle_model vm on v.modelcode = vm.modelcode";
        this.paramquery = "select paramfamilycode, modelfamilycode, paramcode, paramvalue, odozeroprocessing from "
                + this.querydbschema + ".vehicle_modelfamily_param";

        return connection != null && this.querydbschema != null;
    }

    private Connection getConnection() {
        this.log.info("Getting the new connection");

        Connection new_connection = null;
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            if (this.timeout > 0)
                DriverManager.setLoginTimeout(this.timeout);
            new_connection = DriverManager.getConnection(this.url, this.username, this.password);
            new_connection.setAutoCommit(true);
        } catch (Exception e) {
            this.log.error("Not able to create connection", e);
        }

        return new_connection;
    }


    public boolean isConnectionOpen() {
        try {
            return this.connection != null && !this.connection.isClosed();
        } catch (Exception e) {
            this.log.error("Postgres connection check failed", e);
            return false;
        }
    }

    public void stopMapper() {
        this.stopped = true;
    }

    private PreparedStatement prepareStatements(String writeQuery) {
        PreparedStatement writestsmt = null;
        if (connection == null) {
            connection = getConnection();
        }

        try {
            if (connection != null) {
                writestsmt = connection.prepareStatement(writeQuery);
            }
        } catch (Exception e) {
            this.log.error("Failed to prepare the Statement", e);
        }
        return writestsmt;
    }

    public boolean refreshParamMeta() {
        if (System.currentTimeMillis() - lastParamRefreshTime > paramRefreshInterval) {
            try {
                log.info("Refreshing param meta");
                HashMap<String, HashMap<String, HashMap<String, String>>> newParamMetaMap = new HashMap<>();
                HashMap<String, HashMap<String, HashMap<String, Boolean>>> newOdoProcMap = new HashMap<>();
                this.paramStatement = prepareStatements(this.paramquery);
                if (this.paramStatement == null) {
                    this.log.error("Failed to prepare the Statement");
                    return false;
                }
                ResultSet rs = this.paramStatement.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        try {
                            String paramfamilycode = rs.getString("paramfamilycode");
                            String modelfamilycode = rs.getString("modelfamilycode");
                            String paramcode = rs.getString("paramcode");
                            String paramvalue = rs.getString("paramvalue");
                            Boolean odozeroprocessing = rs.getBoolean("odozeroprocessing");

                            // model family code -> param family code -> param code -> param value
                            // t305 -> regulardata -> v_mode -> ess_bcm_vehicle_mode_act_enum
                            if (!newParamMetaMap.containsKey(modelfamilycode))
                                newParamMetaMap.put(modelfamilycode, new HashMap<>());
                            if (!newParamMetaMap.get(modelfamilycode).containsKey(paramfamilycode))
                                newParamMetaMap.get(modelfamilycode).put(paramfamilycode,
                                        new HashMap<>());
                            newParamMetaMap.get(modelfamilycode).get(paramfamilycode).put(paramcode, paramvalue);

                            // model family code -> param family code -> param code -> odo zero processing
                            // t305 -> regulardata -> v_mode -> true
                            if (!newOdoProcMap.containsKey(modelfamilycode))
                                newOdoProcMap.put(modelfamilycode, new HashMap<>());
                            if (!newOdoProcMap.get(modelfamilycode).containsKey(paramfamilycode))
                                newOdoProcMap.get(modelfamilycode).put(paramfamilycode,
                                        new HashMap<>());
                            newOdoProcMap.get(modelfamilycode).get(paramfamilycode).put(paramcode, odozeroprocessing);
                        } catch (Exception e) {
                            this.log.error("Failed to update the param meta", e);
                            return false;
                        }
                    }

                    synchronized (lock) {
                        modelParamMetaMap = newParamMetaMap;
                        modelOdoProcMap = newOdoProcMap;
                        // log.info("Param meta: {}", paramMetaMap.toString());
                    }
                    this.paramStatement.close();
                    rs.close();
                    log.info("Param meta refreshed for " + modelParamMetaMap.size() + " models");
                }
            } catch (Exception e) {
                this.log.error("Exception while refreshing param meta", e);
                try {
                    this.connection.close();
                    this.connection = getConnection();
                } catch (Exception e1) {
                    this.log.error("Exception while closing the connection", e1);
                }
                return false;
            }
            lastParamRefreshTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public boolean refreshVehicleMeta() {
        if (System.currentTimeMillis() - lastVehicleRefreshTime > vehicleRefreshInterval) {
            try {
                this.log.info("Refreshing vehicle meta");
                HashMap<String, String> newVinModelMap = new HashMap<>();
                HashSet<String> newVinList = new HashSet<>();
                this.vehicleStatement = prepareStatements(this.vmquery);
                if (this.vehicleStatement == null) {
                    this.log.error("Failed to prepare the Statement");
                    return false;
                }
                ResultSet rs = this.vehicleStatement.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        try {
                            String vin = rs.getString("vinno");
                            String model = rs.getString("modelfamilycode");

                            newVinList.add(vin);
                            if (!newVinModelMap.containsKey(vin))
                                newVinModelMap.put(vin, model);
                        } catch (Exception e) {
                            this.log.error("Failed to update the vehicle meta", e);
                            return false;
                        }
                    }

                    synchronized (lock) {
                        vinModelMap = newVinModelMap;
                        vinList = newVinList;
                        // log.info("Vehicle Param meta: {}", vinModelMap.toString());
                        // this.log.info("Vehicle Param meta size: " + vinModelMap.size());
                    }
                    this.vehicleStatement.close();
                    rs.close();
                    this.log.info("Vehicle meta refreshed for " + newVinModelMap.size() + " vehicles");
                }
            } catch (Exception e) {
                this.log.error("Exception while refreshing vehicle meta", e);
                try {
                    this.connection.close();
                    this.connection = getConnection();
                } catch (Exception e1) {
                    this.log.error("Exception while closing the connection", e1);
                }
                return false;
            }
            lastVehicleRefreshTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public void run() {
        while (!this.stopped) {
            refreshParamMeta();
            refreshVehicleMeta();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                this.log.error("Exception while sleeping", e);
            }
        }
    }

    public boolean fetchParamMetaNow() {
        if (!refreshParamMeta())
            return false;
        return refreshVehicleMeta();
    }

    /**
     * 
     * @param vin
     * @return {"regulardata": {"v_mode": "ess_bcm_vehicle_mode_act_enum"}}
     */
    public HashMap<String, HashMap<String, String>> getVehicleParamMeta(String vin) {
        synchronized (lock) {
            String model = this.vinModelMap.getOrDefault(vin, null);
            if (model == null)
                return null;
            return this.modelParamMetaMap.getOrDefault(model, null);
        }
    }

    /**
     * 
     * @param vin
     * @return {"regulardata": {"v_mode": true}}
     */
    public HashMap<String, HashMap<String, Boolean>> getVehicleOdoProcMeta(String vin) {
        synchronized (lock) {
            String model = this.vinModelMap.getOrDefault(vin, null);
            if (model == null)
                return null;
            return this.modelOdoProcMap.getOrDefault(model, null);
        }
    }
}