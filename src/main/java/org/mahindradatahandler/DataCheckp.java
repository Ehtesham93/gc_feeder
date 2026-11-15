package org.mahindradatahandler;

import lombok.Data;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.time.Instant;

@Data
public class DataCheckp implements Serializable {
    private Map<String, GpsData> gpsCheckpoints = new ConcurrentHashMap<>(); // vin -> GpsData
    private Map<String, CanData> canCheckpoints = new ConcurrentHashMap<>(); // vin -> CanData
    private Map<String, LivenessData> livenessCheckpoints = new ConcurrentHashMap<>(); // vin_utcdayb -> LivenessData
    private Map<String, CanDataRecord> canDataRecord = new ConcurrentHashMap<>(); // vin -> CanDataRecord

    @Data
    public static class GpsData implements Serializable {
        // Vehicle data
        private String vin;
        private String imei;
        private String model;
        private String valid;
        private String packettype;

        // Time data
        private long utctime;
        private long intime;
        private long proctime;
        private long gpstime;

        // Location
        private double latitude;
        private double longitude;
        private long altitude;
        private long speed;
        private String lngdirection;
        private String latdirection;
    }

    @Data
    public static class CanData implements Serializable {
        // Vehicle data
        private String vin;
        private String imei;
        private String model;
        private String valid;
        private String packettype;
        private double vmodelType;

        // Time data
        private long utctime;
        private long proctime;
        private long intime;
        private String tstamp;

        private String cycletime;
        private String charger_sts_live;
        private String bms_charger_plug_in_sts;
        private String drive_enable;
        private String bms_hvbatt_connect_sts;
        private String bms_hvbatt_disconnect_sts;
        private String bms_in_keyon;
        private int bms_cyclenum;
        private float charger_temp_live;
        private float chargertemp;
        private float soc;
        private int soh;
        private float bms_cell_avg_volt;
        private float bms_cell_avg_temp;
        private float motortemp;
        private float bms_batt_pack_temp;
        private int brake_switch_status;
        private float odometer;
        private float vehiclespeed;
        private float motorpower;
        private int e_motor_rpm;
        private int inv_drivemode_shift;
        private int v_mode;
        private int vehicle_phase;
        private int battery_ttc;
        private float percthrottle;
        private float kwh;
        private float batt_current;
        private float aux_batt_volt;
        private float bat_voltage;
        private float bus_voltage;
        private float dte;
        private int can_throttle;
        private float wakeup_command;
        private int veh_immo_resp;
        private int service_tt;
        private int chargett;
        private int connectionhealthtt;
        private int tcu_speedmode;
    }

    @Data
    public static class LivenessData implements Serializable {
        private String vin;
        private int utc_day_b;
        private int gps_cnt;
        private int can_cnt;
        private String gps_first;
        private String gps_last;
        private String can_first;
        private String can_last;
        private long proctime;
    }

    // Methods to store checkpoints
    public void saveGpsCheckpoint(String partitionKey, GpsData data) {
        gpsCheckpoints.put(partitionKey, data);
    }

    public void saveCanCheckpoint(String partitionKey, CanData data) {
        canCheckpoints.put(partitionKey, data);
    }

    public void saveLivenessCheckpoint(String partitionKey, LivenessData data) {
        livenessCheckpoints.put(partitionKey, data);
    }
    
    public void saveCanDataRecordCheckpoint(String vin, CanDataRecord data) {
        canDataRecord.put(vin, data);
    }

    // Methods to retrieve checkpoints
    public GpsData getGpsCheckpoint(String partitionKey) {
        GpsData data = gpsCheckpoints.get(partitionKey);
        if (data == null) {
            GpsData defaultData = new GpsData();
            defaultData.setVin("");
            defaultData.setImei("");
            defaultData.setModel("");
            defaultData.setValid("");
            defaultData.setPackettype("");

            defaultData.setUtctime(0L);
            defaultData.setIntime(0L);
            defaultData.setProctime(0L);
            defaultData.setGpstime(0L);

            defaultData.setLatitude(0.0);
            defaultData.setLongitude(0.0);
            defaultData.setAltitude(0);
            defaultData.setSpeed(0);
            defaultData.setLngdirection("");
            defaultData.setLatdirection("");

            return defaultData;
        }
        return data;
    }

    public CanData getCanCheckpoint(String partitionKey) {
        CanData data = canCheckpoints.get(partitionKey);
        if (data == null) {
            CanData defaultData = new CanData();
            defaultData.setVin("");
            defaultData.setImei("");
            defaultData.setModel("");
            defaultData.setValid("");
            defaultData.setPackettype("");
            defaultData.setVmodelType(0.0);

            defaultData.setUtctime(0L);
            defaultData.setIntime(0L);
            defaultData.setProctime(0L);
            defaultData.setTstamp("");

            defaultData.setCycletime("");
            defaultData.setCharger_sts_live("");
            defaultData.setBms_charger_plug_in_sts("");
            defaultData.setDrive_enable("");
            defaultData.setBms_hvbatt_connect_sts("");
            defaultData.setBms_hvbatt_disconnect_sts("");
            defaultData.setBms_in_keyon("");

            defaultData.setBms_cyclenum(0);
            defaultData.setCharger_temp_live(0.0f);
            defaultData.setChargertemp(0.0f);
            defaultData.setSoc(0.0f);
            defaultData.setSoh(0);
            defaultData.setBms_cell_avg_volt(0.0f);
            defaultData.setBms_cell_avg_temp(0.0f);
            defaultData.setMotortemp(0.0f);
            defaultData.setBms_batt_pack_temp(0.0f);
            defaultData.setBrake_switch_status(0);
            defaultData.setOdometer(0.0f);
            defaultData.setVehiclespeed(0.0f);
            defaultData.setMotorpower(0.0f);
            defaultData.setE_motor_rpm(0);
            defaultData.setInv_drivemode_shift(0);
            defaultData.setV_mode(0);
            defaultData.setVehicle_phase(0);
            defaultData.setBattery_ttc(0);
            defaultData.setPercthrottle(0.0f);
            defaultData.setKwh(0.0f);
            defaultData.setBatt_current(0.0f);
            defaultData.setAux_batt_volt(0.0f);
            defaultData.setBat_voltage(0.0f);
            defaultData.setBus_voltage(0.0f);
            defaultData.setDte(0.0f);
            defaultData.setCan_throttle(0);
            defaultData.setWakeup_command(0.0f);
            defaultData.setVeh_immo_resp(0);
            defaultData.setService_tt(0);
            defaultData.setChargett(0);
            defaultData.setConnectionhealthtt(0);
            defaultData.setTcu_speedmode(0);

            return defaultData;
        }
        return data;
    }

    public LivenessData getLivenessCheckpoint(String partitionKey) {
        LivenessData data = livenessCheckpoints.get(partitionKey);
        if (data == null) {
            LivenessData defaultData = new LivenessData();
            defaultData.setVin("");
            defaultData.setUtc_day_b(0);
            defaultData.setGps_cnt(0);
            defaultData.setCan_cnt(0);
            defaultData.setGps_first("");
            defaultData.setGps_last("");
            defaultData.setCan_first("");
            defaultData.setCan_last("");
            defaultData.setProctime(0L);
            return defaultData;
        }
        return data;
    }

    @Data
    public static class CanDataRecord implements Serializable {
        ArrayList<CanData> canDataInmemList;
        ArrayList<String> sequenceNumberList;
        ArrayList<Instant> approximateArrivalTimestampList;
        CanData lastData;
        String lastSequenceNumber;
        Instant lastApproximateArrivalTimestamp;
        int checkpCounter;

        public CanDataRecord(ArrayList<CanData> canDataInmemList, ArrayList<String> sequenceNumberList,
                ArrayList<Instant> approximateArrivalTimestampList, CanData lastData, String lastSequenceNumber,
                Instant lastApproximateArrivalTimestamp, int checkpCounter) {
            this.canDataInmemList = canDataInmemList;
            this.sequenceNumberList = sequenceNumberList;
            this.approximateArrivalTimestampList = approximateArrivalTimestampList;
            this.lastData = lastData;
            this.lastSequenceNumber = lastSequenceNumber;
            this.lastApproximateArrivalTimestamp = lastApproximateArrivalTimestamp;
            this.checkpCounter = checkpCounter;
        }
    }
}