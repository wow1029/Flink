package com.ncut.wc.entity;

/**
 * 船舶数据结构，用于封装 Kafka AIS JSON 数据
 */
public class ShipData {
    public long mmsi;
    public double lat;
    public double lon;
    public String timeStr;
    public long timestamp;

    public ShipData() {
    }

    public ShipData(long mmsi, double lat, double lon, String timeStr, long timestamp) {
        this.mmsi = mmsi;
        this.lat = lat;
        this.lon = lon;
        this.timeStr = timeStr;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ShipData{" +
                "mmsi=" + mmsi +
                ", lat=" + lat +
                ", lon=" + lon +
                ", timeStr='" + timeStr + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
