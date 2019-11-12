package com.jd.realTimeDashboard;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MerchandiseAggFunc implements AggregateFunction<SubOrderDetail, Long, Long> {
    private static final long serialVersionUID = -8902126405407003146L;

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(SubOrderDetail subOrderDetail, Long acc) {
        return acc + subOrderDetail.getQuantity();
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
