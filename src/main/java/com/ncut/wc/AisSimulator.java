package com.ncut.wc;

import com.ncut.wc.config.Constant;
import com.ncut.wc.utils.GeoHash;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class AisSimulator {

    private static final long[] MMSI_LIST = {100000001L, 100000002L, 100000003L, 100000004L};

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            long timestamp = System.currentTimeMillis();
            String timeStr = new SimpleDateFormat("yyyy/M/d HH:mm").format(new Date());

            for (int idx = 0; idx < MMSI_LIST.length; idx++) {
                long mmsi = MMSI_LIST[idx];
                int phase = (int) ((System.currentTimeMillis() / 60000) % 15);
                int inStart = idx * 2;
                int inEnd = inStart + 6;

                double lat, lon;
                if (phase >= inStart && phase <= inEnd) {
                    lat = randomBetween(Constant.MIN_LAT, Constant.MAX_LAT);
                    lon = randomBetween(Constant.MIN_LON, Constant.MAX_LON);
                } else {
                    lat = randomBetween(-90, 90);
                    lon = randomBetween(-180, 180);
                }

                String geohash = GeoHash.encode(lat, lon, 12);

                double speed = randomBetween(0, 30);
                double heading = randomBetween(0, 360);
                double course = randomBetween(0, 360);
                int rot = new Random().nextInt(10);

                String json = String.format(Locale.US,
                        "{\"mmsi\":%d,\"heading\":%.5f,\"rot\":%d,\"geohash\":\"%s\",\"course\":%.2f,\"lon\":%.5f,\"timeStr\":\"%s\",\"lat\":%.5f,\"navStatus\":\"在航(主机推动)\",\"speed\":%.6f,\"timestamp\":%d}",
                        mmsi, heading, rot, geohash, course, lon, timeStr, lat, speed, timestamp
                );

                producer.send(new ProducerRecord<>(Constant.TOPIC, json));
                System.out.printf("MMSI=%d 位置=(%.5f, %.5f) 时间=%s\n", mmsi, lat, lon, timeStr);
            }
        }, 0, Constant.INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private static double randomBetween(double min, double max) {
        return min + new Random().nextDouble() * (max - min);
    }
}
