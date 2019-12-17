package com.jd.realTimeDashboard;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MapWithMerchandiseInfoFunc extends RichMapFunction<String, String> {
    private static final long serialVersionUID = 49050897922571493L;

    private transient ScheduledExecutorService dbScheduler;
    private Map<Long,MerchandiseInfo> merchandiseInfo;
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        merchandiseInfo = new HashMap<>();

        dbScheduler = new ScheduledThreadPoolExecutor(1);
        dbScheduler.scheduleWithFixedDelay(()->{
            try{
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/mydb?useUnicode=true&characterEncoding=UTF-8", "root", "gwk");
                String sql = "select merchandiseId,merchandiseName,merchandiseType from merchandise_info";
                preparedStatement = connection.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    long merchandiseId = resultSet.getLong("merchandiseId");
                    String merchandiseName = resultSet.getString("merchandiseName");
                    String merchandiseType = resultSet.getString("merchandiseType");
                    merchandiseInfo.put(merchandiseId,new MerchandiseInfo(merchandiseId,merchandiseName,merchandiseType));
                    }
                }catch (Exception e){
                    log.error("Exception occurred when querying: " + e);
                }

        },0,60, TimeUnit.SECONDS);

    }

    @Override
    public String map(String s) throws Exception {
        JSONObject json = JSON.parseObject(s);
        long merchandiseId = json.getLong("merchandiseId");

        String merchandiseName = "";
        String merchandiseType = "";
        MerchandiseInfo info = merchandiseInfo.getOrDefault(merchandiseId,null);
        if (info!=null){
            merchandiseName = info.getMerchandiseName();
            merchandiseType = info.getMerchandiseType();
        }
        json.put("merchandiseName",merchandiseName);
        json.put("merchandiseType",merchandiseType);

        return json.toString();
    }

    @Override
    public void close() throws Exception{
        merchandiseInfo.clear();
        super.close();
        if (connection != null) {
            connection.close();
        }

        if (preparedStatement != null) {
            preparedStatement.close();
        }

    }
}
