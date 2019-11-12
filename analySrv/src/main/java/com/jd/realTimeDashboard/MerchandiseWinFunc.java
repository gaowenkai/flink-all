package com.jd.realTimeDashboard;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MerchandiseWinFunc implements WindowFunction<Long, Tuple2<Long, Long>, Tuple, TimeWindow> {
    private static final long serialVersionUID = 6282293568518964567L;

    @Override
    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Long> acc, Collector<Tuple2<Long, Long>> collector) throws Exception {
        long merchandiseId = key.getField(0);
        long sum = acc.iterator().next();
        collector.collect(new Tuple2<Long, Long>(merchandiseId,sum));
    }
}
