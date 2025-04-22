package com.ncut.wc.config;

public class Constant {
    /** 区域边界配置（使用经纬度矩形框） */
    public static final double MIN_LAT = -33.31;
    public static final double MIN_LON = 24.89;
    public static final double MAX_LAT = -30.00;
    public static final double MAX_LON = 30.00;

    /** 统计窗口时间间隔（单位：毫秒） */
    public static final long WINDOW_INTERVAL = 15 * 60 * 1000;

    public static final String TOPIC = "rightais";
    public static final String BROKER = "192.168.157.132:9092";
    public static final String CONSUME = "192.168.157.133:9092";
    public static final int INTERVAL_SECONDS = 60;
}
