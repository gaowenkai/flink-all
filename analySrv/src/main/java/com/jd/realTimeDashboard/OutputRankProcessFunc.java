package com.jd.realTimeDashboard;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class OutputRankProcessFunc extends ProcessAllWindowFunction<Tuple2<Long,Long>, Tuple2<Long,Long>, TimeWindow> {

    private int topsize = 10;

    public OutputRankProcessFunc(){}

    public OutputRankProcessFunc(int topsize){
        this.topsize = topsize;
    }
    @Override
    public void process(Context context, Iterable<Tuple2<Long, Long>> in, Collector<Tuple2<Long, Long>> collector) throws Exception {
        TreeMap<Long,Tuple2<Long,Long>> treeMap = new TreeMap<>(
                new Comparator<Long>() {
                    @Override
                    public int compare(Long o1, Long o2) {
                        return (o1 > o2) ? -1 : 1;
                    }
                }
        );

        for (Tuple2<Long, Long> t:in){
            treeMap.put(t.f1,t);
            if (treeMap.size() > topsize){
                treeMap.pollLastEntry();
            }
        }

        for (Map.Entry<Long,Tuple2<Long,Long>> t:treeMap.entrySet()) {
            collector.collect(t.getValue());
        }
    }
}
