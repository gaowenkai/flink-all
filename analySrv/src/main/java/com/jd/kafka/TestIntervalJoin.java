package com.jd.kafka;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;


/**
 * order
 * 001 1545800002000
 * 002 1545800004000
 * 004 1545800018000
 *
 * payment
 * 001 1545803501000
 * 002 1545803610000
 * 004 1545803611000
 */

public class TestIntervalJoin {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<Tuple2<String, Timestamp>> order = env.socketTextStream("localhost",8888)
                .map(s -> new Tuple2<String,Timestamp>(
                        s.split("\\W+")[0], new Timestamp(Long.valueOf(s.split("\\W+")[1]))))
                .returns(Types.TUPLE(Types.STRING,Types.SQL_TIMESTAMP));

        DataStream<Tuple2<String, Timestamp>> payment = env.socketTextStream("localhost",9999)
                .map(s -> new Tuple2<String,Timestamp>(
                        s.split("\\W+")[0], new Timestamp(Long.valueOf(s.split("\\W+")[1]))))
                .returns(Types.TUPLE(Types.STRING,Types.SQL_TIMESTAMP));

        StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        tenv.registerDataStream("orders",order,"o_id,o_time");
        tenv.registerDataStream("payments",payment,"p_id,p_time");
        String sql = "SELECT o.o_id,o.o_time  as order_time,p.p_time as payment_time " +
                "FROM orders AS o JOIN payments AS p " +
                "ON o.o_id = p.p_id "
                + "and p.p_time  between o.o_time and o.o_time + INTERVAL '1' HOUR";
        Table table = tenv.sqlQuery(sql);
        DataStream<Tuple3<String, Timestamp, Timestamp>> res = tenv.toAppendStream(table, Types.TUPLE(Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP));
        res.print();

        env.execute("job");


    }
}
