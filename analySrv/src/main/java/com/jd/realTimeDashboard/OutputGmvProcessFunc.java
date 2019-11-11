package com.jd.realTimeDashboard;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OutputGmvProcessFunc extends KeyedProcessFunction<Tuple, OrderAccumulator, Tuple2<Long, String>> {
    private static final long serialVersionUID = -3404291136886564487L;

    private transient MapState<Long,OrderAccumulator> state;

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        state = this.getRuntimeContext().getMapState(
                new MapStateDescriptor<Long, OrderAccumulator>(
                        "state_site_gmv",
                        Long.class,
                        OrderAccumulator.class)
        );
    }


    @Override
    public void processElement(OrderAccumulator order, Context context, Collector<Tuple2<Long, String>> collector) throws Exception {
        long key = order.getSiteId();
        OrderAccumulator cachedOrder = state.get(key);

        if(cachedOrder == null || order.getSubOrderSum() != cachedOrder.getSubOrderSum()){
            JSONObject result = new JSONObject();
            result.put("site_id", order.getSiteId());
            result.put("site_name", order.getSiteName());
            result.put("quantity", order.getQuantitySum());
            result.put("orderCount", order.getOrderIds().size());
            result.put("subOrderCount", order.getSubOrderSum());
            result.put("gmv", order.getGmv());
            collector.collect(new Tuple2<>(key,result.toJSONString()));
            state.put(key,order);
        }
    }

    @Override
    public void close() throws Exception{
        state.clear();
        super.close();
    }
}
