package com.jd.kafka;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WatermarksTest {



    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> input = env.socketTextStream("localhost",8888);
        DataStream<Tuple4<String, Long, String, String>> res = input
                .map(s -> new Tuple2<String,Long>(
                        s.split("\\W+")[0], Long.valueOf(s.split("\\W+")[1])))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new TestWatermarks())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunctionTest())
                ;

        res.print();
        env.execute();

    }

    private static class TestWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>>{
        private static Long currentMaxTimeStamp = 0L;
        final private static Long maxOutOfOrderness = 10000L; //10s
        private static Watermark watermark = null;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            this.watermark = new Watermark(currentMaxTimeStamp - maxOutOfOrderness);
            return watermark;
        }


        @Override
        public long extractTimestamp(Tuple2<String, Long> t, long l) {
            this.currentMaxTimeStamp = Math.max(t.f1, currentMaxTimeStamp);
            System.out.println("timestamp:"+t.f0+","+t.f1+"|"+stampToDate(t.f1)+","+currentMaxTimeStamp+"|"+stampToDate(currentMaxTimeStamp)+","+ watermark.toString());
            return t.f1;
        }
    }

    private static class WindowFunctionTest implements WindowFunction<Tuple2<String, Long>, Tuple4<String, Long, String, String>, Tuple, TimeWindow>{
        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple4<String, Long, String, String>> collector) throws Exception {
            collector.collect(new Tuple4<String, Long, String, String>(
                    key.getField(0),
                    iterable.spliterator().getExactSizeIfKnown(),
                    format.format(new Date(timeWindow.getStart())),
                    format.format(new Date(timeWindow.getEnd()))
                    )
            );

        }
    }

    private static String stampToDate(Long s){
        String res ;
        if(s == null){
            return null;
        }else {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date(s);
            res = simpleDateFormat.format(date);
            return res;
        }
    }
}

