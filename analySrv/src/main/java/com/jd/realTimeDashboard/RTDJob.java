package com.jd.realTimeDashboard;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/**
 * {"userId": 234567,"orderId": 2902306918400,"subOrderId": 2902306918401,"siteId": 10219,"siteName": "site_blabla","cityId": 101,"cityName": "北京市","warehouseId": 636,"merchandiseId": 187699,"price": 100,"quantity": 2,"orderStatus": 1,"isNewOrder": 0,"timestamp": 1572963672217}
 *
 * {"userId": 234567,"orderId": 2902306918400,"subOrderId": 2902306918400,"siteId": 10219,"siteName": "site_blabla","cityId": 101,"cityName": "北京市","warehouseId": 636,"merchandiseId": 187699,"price": 100,"quantity": 1,"orderStatus": 1,"isNewOrder": 0,"timestamp": 1572963672217}
 *
 * {"userId": 234567,"orderId": 2902306918401,"subOrderId": 2902306918400,"siteId": 10219,"siteName": "site_blabla","cityId": 101,"cityName": "北京市","warehouseId": 636,"merchandiseId": 187690,"price": 100,"quantity": 4,"orderStatus": 1,"isNewOrder": 0,"timestamp": 1572963672217}
 */

public class RTDJob {

    public static void main(String[] args) throws Exception {
        args = new String[]{
                "--input-topic","tuzisir",
                "--output-topic","tuzisir",
                "--bootstrap.servers","localhost:9092",
                "--zookeeper.connect","localhost:2181",
                "--group.id","consumer"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final int PARTITION_COUNT = 1;
        final String ORDER_EXT_TOPIC_NAME = parameterTool.get("input-topic");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60*1000, CheckpointingMode.EXACTLY_ONCE);//1min
        env.getCheckpointConfig().setCheckpointTimeout(30*1000);
        // kafka
//        DataStream<String> sourceStream = env.addSource(
//                new FlinkKafkaConsumer010<>(
//                        ORDER_EXT_TOPIC_NAME,
//                        new SimpleStringSchema(),
//                        parameterTool.getProperties()))
//                .setParallelism(PARTITION_COUNT)
//                .name("source_kafka_" + ORDER_EXT_TOPIC_NAME)
//                .uid("source_kafka_" + ORDER_EXT_TOPIC_NAME);

        DataStream<String> sourceStream = env.socketTextStream("localhost",9999);
        // JSON
        DataStream<SubOrderDetail> orderStream = sourceStream
                .map(message -> JSON.parseObject(message, SubOrderDetail.class))
                .name("map_sub_order_detail").uid("map_sub_order_detail");
        // window
        WindowedStream<SubOrderDetail, Tuple, TimeWindow> siteDayWinStream = orderStream
                .keyBy("siteId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(16)))//处理时间的时区问题
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));//1s为周期触发计算
        // aggregate
        DataStream<OrderAccumulator> siteAggStream = siteDayWinStream
                .aggregate(new OrderGmvAggFunc())
                .name("agg_site_order_gmv").uid("agg_site_order_gmv");
        // process
        DataStream<Tuple2<Long, String>> siteResStream = siteAggStream
                .keyBy("siteId")
                .process(new OutputGmvProcessFunc(), TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}))
                .name("process_site_gmv").uid("process_site_gmv");

        siteResStream.print();


        // top N
        SingleOutputStreamOperator<Tuple2<Long, Long>> merchandiseAggStream = orderStream
                .keyBy("merchandiseId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(16)))//处理时间的时区问题
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))//10s为周期触发计算
                .aggregate(new MerchandiseAggFunc(),new MerchandiseWinFunc());
        SingleOutputStreamOperator<Tuple2<Long, Long>> resStream = merchandiseAggStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new OutputRankProcessFunc(2))
                .name("merchandise_rank_output").uid("merchandise_rank_output");

        resStream.print();

        env.execute("job");

    }
}
