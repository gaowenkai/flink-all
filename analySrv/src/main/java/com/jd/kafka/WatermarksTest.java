package com.jd.kafka;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

public class WatermarksTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<Tuple2<String,Long>> outputTag = new OutputTag<Tuple2<String,Long>>("Too-Late-Data") {};

        DataStream<String> input = env.socketTextStream("localhost",8888);

        SingleOutputStreamOperator<Tuple6<String, Long, String, String, String, String>> res = input
                .map(s -> new Tuple2<String,Long>(
                        s.split("\\W+")[0], Long.valueOf(s.split("\\W+")[1])))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new TestWatermarks())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                // 允许数据迟到3S(因为输入的数据所在的窗口已经执行过了，flink 默认对这些迟到的数据的处理方案就是丢弃)
                .allowedLateness(Time.seconds(3))
                // 丢弃的数据使用side output输出
                .sideOutputLateData(outputTag)
                .apply(new WindowFunctionTest())
                ;

        DataStream<Tuple2<String, Long>> lateData = res.getSideOutput(outputTag);
        DataStream<String> lateDataString = lateData
                .map(t -> "id: "+t.f0+" timestamp: "+t.f1)
                .returns(Types.STRING);

        res.print();
        lateDataString.print();

        StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        tenv.registerDataStream("late",lateData,"t_id,t_time");
        String sql = "select t_id,t_time from late";
        Table table = tenv.sqlQuery(sql);

        //write to csv sink

        TableSink csvSink = new CsvTableSink("E:/work/flink-all/flink_data/late-data_"+System.currentTimeMillis(), "|");
        String[] fieldNames = {"t_id","t_time"};
        TypeInformation[] fieldTypes = {Types.STRING,Types.LONG};
        tenv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
        table.insertInto("CsvSinkTable");


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

    private static class WindowFunctionTest implements WindowFunction<Tuple2<String, Long>, Tuple6<String, Long, String, String, String, String>, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple6<String, Long, String, String, String, String>> collector) throws Exception {
            List<Long> l = new ArrayList<Long>();
            Iterator<Tuple2<String, Long>> it = iterable.iterator();
            while (it.hasNext()){
                l.add(it.next().f1);
            }

            Collections.sort(l);

            collector.collect(new Tuple6<String, Long, String, String, String, String>(
                    key.toString(),
                    iterable.spliterator().getExactSizeIfKnown(),
                    stampToDate(l.get(0)),
                    stampToDate(l.get(l.size()-1)),
                    stampToDate(timeWindow.getStart()),
                    stampToDate(timeWindow.getEnd())
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

