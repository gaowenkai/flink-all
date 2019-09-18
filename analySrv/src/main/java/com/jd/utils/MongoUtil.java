package com.jd.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import net.sf.json.JSONObject;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MongoUtil {

    public static MongoCollection<Document> getConnect(String tableName){
        List<ServerAddress> adds = new ArrayList<>();
        //ServerAddress()两个参数分别为 服务器地址 和 端口
        ServerAddress serverAddress = new ServerAddress("", 27017);
        adds.add(serverAddress);

        List<MongoCredential> credentials = new ArrayList<>();
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential("", "", "".toCharArray());
        credentials.add(mongoCredential);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(adds, credentials);

        //连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("");

        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

        //返回连接数据库对象
        return collection;
    }

    public static List getMongoDbJson(MongoCollection<Document> collection) {

        long length = collection.count();
        System.out.println(String.format("data count =====> %s", length));

        List<Object> docJsonList = new ArrayList<>();

        FindIterable findIterable = collection.find();

        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            Map<Object,Object> map = jsonToMap(cursor.next());
            docJsonList.add(map);
        }

        return docJsonList;
    }

    // json转map
    public static Map<Object, Object> jsonToMap(Object jsonObj) {
        JSONObject jsonObject = JSONObject.fromObject(jsonObj);
        Map<Object, Object> map = (Map) jsonObject;
        return map;
    }
}