package com.ncut.wc;

import com.ncut.wc.config.Constant;
import com.ncut.wc.entity.ShipData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;



/**
 * 实时船舶进出区域统计程序
 * 功能说明：
 * - 监听 Kafka 船舶定位数据流
 * - 判断船舶是否进入/离开指定区域
 * - 每个时间窗口内统计离开区域的船只数量（定时器触发）
 * - 不去重，允许重复船只多次统计
 */
public class ShipCount {
    /**
     * 判断给定经纬度是否在区域内
     */
    private static boolean isInRegion(double lat, double lon) {
        return lat >= Constant.MIN_LAT && lat <= Constant.MAX_LAT && lon >= Constant.MIN_LON && lon <= Constant.MAX_LON;
    }
    public static void main(String[] args) throws Exception {
        // 设置 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka 消费者配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.CONSUME);
        props.setProperty("group.id", "ship-group");

        // 创建 Kafka source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                Constant.TOPIC,
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                props
        );

        //只消费“实时数据”，忽略历史消息
        kafkaSource.setStartFromLatest();

        // 原始数据打印输出
        DataStream<String> rawStream = env.addSource(kafkaSource);
        rawStream.print("Kafka原始数据");

        // JSON 转换为 ShipData 对象
        DataStream<ShipData> ships = rawStream.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(json);
            long mmsi = node.get("mmsi").asLong();
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();
            String timeStr = node.get("timeStr").asText();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm");
            long timestamp = sdf.parse(timeStr).getTime();
            return new ShipData(mmsi, lat, lon, timeStr, timestamp);
        });

        ships.keyBy(ship -> "region") // 所有船只共用一个 key
             .process(new KeyedProcessFunction<String, ShipData, String>() {

                 // 区域内船只
                 private transient ValueState<List<Long>> inList;

                 // 已离开区域的船只
                 private transient ListState<Long> resultList;

                 // 当前定时器时间
                 private transient ValueState<Long> timer;

                 // 判断船的状态
                 private transient ValueState<Boolean> printedFlag;

                 @Override
                 public void open(Configuration parameters) {
                     inList = getRuntimeContext().getState(new ValueStateDescriptor<>("inList", Types.LIST(Types.LONG)));
                     resultList = getRuntimeContext().getListState(new ListStateDescriptor<>("resultList", Long.class));
                     timer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
                     printedFlag = getRuntimeContext().getState(new ValueStateDescriptor<>("printedFlag",Types.BOOLEAN));
                 }

                 @Override
                 public void processElement(ShipData ship, Context ctx, Collector<String> out) throws Exception {
                     List<Long> in = inList.value();
                     if (in == null) in = new ArrayList<>();

                     boolean inRegion = isInRegion(ship.lat, ship.lon);
                     boolean wasIn = in.contains(ship.mmsi);
                     Boolean hasPrinted = printedFlag.value();
                     // 进入区域
                     if (inRegion && !wasIn) {
                         in.add(ship.mmsi);
                         inList.update(in);
                         out.collect(ship.mmsi + " 进入区域！");
                     }

                     // 离开区域
                     if (!inRegion && wasIn) {
                         resultList.add(ship.mmsi);
                         in.remove(ship.mmsi);
                         inList.update(in);
                         out.collect(ship.mmsi + " 离开区域！");
                     }

                     List<Long> result = new ArrayList<>();
                     for (Long mmsi : resultList.get()) {
                         result.add(mmsi);
                     }

                     //if (hasPrinted == null || !hasPrinted) {
                         //System.out.println("当前在区域内的船只: " + in);
                         //System.out.println("已离开区域的船只: " + result);
                         //printedFlag.update(true);
                     //}

                     System.out.println("当前在区域内的船只:" + in);

                     //List<Long> result = new ArrayList<>();
                     //for (Long mmsi : resultList.get()) {
                         //result.add(mmsi);
                     //}
                     System.out.println("已离开区域的船只:" + result);

                     // 注册定时器（只注册一次）
                     Long currentTimer = timer.value();
                     long now = ctx.timerService().currentProcessingTime();
                     if (currentTimer == null || currentTimer <= now) {
                         long triggerTime = now + Constant.WINDOW_INTERVAL;
                         ctx.timerService().registerProcessingTimeTimer(triggerTime);
                         timer.update(triggerTime);
                     }
                 }

                 @Override
                 public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                     // 输出统计结果
                     List<Long> result = new ArrayList<>();
                     for (Long mmsi : resultList.get()) {
                         result.add(mmsi);
                     }

                     out.collect((Constant.WINDOW_INTERVAL / 60000) + "分钟内离开区域的船只数量：" + result.size());

                     // 清空状态，开启下一个周期
                     inList.clear();
                     resultList.clear();
                     timer.clear();

                     long nextTrigger = ctx.timerService().currentProcessingTime() + Constant.WINDOW_INTERVAL;
                     ctx.timerService().registerProcessingTimeTimer(nextTrigger);
                     timer.update(nextTrigger);
                    }
                })
                .print("统计结果");
        env.execute("Ship Count - Real-Time Region Monitor");
    }
}
