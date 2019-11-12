package com.jd.realTimeDashboard;

import lombok.*;
import java.util.HashSet;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderAccumulator {

    private long siteId = 0;
    private String siteName;

    private long orderId;
    private long subOrderSum; //子订单数量
    private long quantitySum; //商品数量
    private long gmv; //price * quantity

    private HashSet<Long> orderIds = new HashSet<Long>();//海量数据，考虑换用HyperLogLog

    public void addOrderId(long orderId){
        this.orderIds.add(orderId);
    }

    public void addSubOrderSum(long num){
        this.subOrderSum = getSubOrderSum() + num;
    }

    public void addQuantitySum(long quantity){
        this.quantitySum = getQuantitySum() + quantity;
    }

    public void addGmv(long gmv){
        this.gmv = getGmv() + gmv;
    }

    public void addOrderIds(HashSet<Long> orderIds){
        this.orderIds.addAll(orderIds);
    }
}
