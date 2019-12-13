package com.jd.broadcastJob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;


// {"userID": "user_3", "eventTime": "2019-08-17 12:19:47", "eventType": "browse", "productID": 1}
// {"userID": "user_2", "eventTime": "2019-08-17 12:19:48", "eventType": "click", "productID": 1}

@Slf4j
public class UpdateUserInfo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> eventStream = sourceStream.process(new ProcessFunction<String, Tuple4<String, String, String, Integer>>() {
            @Override
            public void processElement(String s, Context context, Collector<Tuple4<String, String, String, Integer>> collector) throws Exception {
                try {
                    JSONObject obj = JSON.parseObject(s);
                    String userID = obj.getString("userID");
                    String eventTime = obj.getString("eventTime");
                    String eventType = obj.getString("eventType");
                    int productID = obj.getInteger("productID");
                    collector.collect(new Tuple4<>(userID, eventTime, eventType, productID));
                } catch (Exception e) {
                    log.warn("异常数据:{}", s, e);
                }
            }
        });

        DataStreamSource<HashMap<String, Tuple2<String, Integer>>> configStream = env.addSource(new MysqlSource("127.0.0.1", 3306, "mydb", "root", "gwk", 1));

        MapStateDescriptor<Void, Map<String, Tuple2<String,Integer>>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        BroadcastStream<HashMap<String, Tuple2<String, Integer>>> broadcastConfigStream = configStream.broadcast(configDescriptor);

        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, HashMap<String, Tuple2<String, Integer>>> connectedStream = eventStream.connect(broadcastConfigStream);

        SingleOutputStreamOperator<Tuple6<String, String, String, Integer, String, Integer>> res = connectedStream.process(new CustomBroadcastProcessFunction());

        res.print();

        env.execute();
    }

    static class CustomBroadcastProcessFunction extends BroadcastProcessFunction<Tuple4<String, String, String, Integer>, HashMap<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>>{
        MapStateDescriptor<Void, Map<String, Tuple2<String,Integer>>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
        @Override
        public void processElement(Tuple4<String, String, String, Integer> value, ReadOnlyContext readOnlyContext, Collector<Tuple6<String, String, String, Integer, String, Integer>> collector) throws Exception {
            String userID = value.f0;
            ReadOnlyBroadcastState<Void, Map<String, Tuple2<String,Integer>>> broadcastState = readOnlyContext.getBroadcastState(configDescriptor);
            Map<String, Tuple2<String,Integer>> broadcastValue = broadcastState.get(null);
            Tuple2<String,Integer> userInfo = broadcastValue.get(userID);
            if (userInfo!=null){
                collector.collect(new Tuple6<>(value.f0,value.f1,value.f2,value.f3,userInfo.f0,userInfo.f1));
            }

        }

        @Override
        public void processBroadcastElement(HashMap<String, Tuple2<String, Integer>> value, Context context, Collector<Tuple6<String, String, String, Integer, String, Integer>> collector) throws Exception {
            BroadcastState<Void, Map<String, Tuple2<String,Integer>>> broadcastState = context.getBroadcastState(configDescriptor);

            broadcastState.clear();

            broadcastState.put(null,value);
        }
    }
    static class MysqlSource extends RichSourceFunction<HashMap<String, Tuple2<String, Integer>>> {
        private String host;
        private Integer port;
        private String db;
        private String user;
        private String passwd;
        private Integer secondInterval;

        private volatile boolean isRunning = true;

        private Connection connection;
        private PreparedStatement preparedStatement;


        MysqlSource(String host, Integer port, String db, String user, String passwd, Integer secondInterval) {
            this.host = host;
            this.port = port;
            this.db = db;
            this.user = user;
            this.passwd = passwd;
            this.secondInterval = secondInterval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=UTF-8", user, passwd);
            String sql = "select userID,userName,userAge from user_info";
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            super.close();

            if (connection != null) {
                connection.close();
            }

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }


        @Override
        public void run(SourceContext<HashMap<String, Tuple2<String, Integer>>> sourceContext) throws Exception {
            try{
                while (isRunning){
                    HashMap<String, Tuple2<String, Integer>> output = new HashMap<>();
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()){
                        String userID = resultSet.getString("userID");
                        String userName = resultSet.getString("userName");
                        int userAge = resultSet.getInt("userAge");
                        output.put(userID,new Tuple2<>(userName,userAge));
                    }

                    sourceContext.collect(output);
                    Thread.sleep(1000*secondInterval);
                }
            }catch (Exception e){
                log.error("从Mysql获取配置异常...",e);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}


