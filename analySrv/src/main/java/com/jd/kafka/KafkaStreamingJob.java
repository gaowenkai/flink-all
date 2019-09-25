package com.jd.kafka;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import scala.Int;

import javax.annotation.Nullable;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class KafkaStreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        args = new String[]{
                "--input-topic","input",
                "--output-topic","output",
                "--bootstrap.servers","localhost:9092",
                "--zookeeper.connect","localhost:2181",
                "--group.id","consumer"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 5){
            System.out.println(
                    "Missing parameters!\n" +
                    "Please use:\n" +
                    "--input-topic <topic> " +
                    "--output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> " +
                    "--group.id <some id>");
            return;
        }
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // make parameters available in the web interface


        DataStream<KafkaEvent> input = env.addSource(
            new FlinkKafkaConsumer010<KafkaEvent>(
                    parameterTool.get("input-topic"),
                    new KafkaEventSchema(),
                    parameterTool.getProperties())
                    .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("word")
                .map(new RollingAdditionMapper());

        input.addSink(
                new FlinkKafkaProducer010<KafkaEvent>(
                        parameterTool.get("output-topic"),
                        new KafkaEventSchema(),
                        parameterTool.getProperties()));

        env.execute("kafka 0.10 job");

    }

    private static class RollingAdditionMapper extends RichMapFunction<KafkaEvent, KafkaEvent>{

        private static final long serialVersionUID = -36374891830755378L;
        private transient ValueState<Integer> currentTotalValue;


        @Override
        public KafkaEvent map(KafkaEvent kafkaEvent) throws Exception {
            Integer total = currentTotalValue.value();
            if (total == null){
                total = 0;
            }
            total += kafkaEvent.getFrequency();

            currentTotalValue.update(total);
            return new KafkaEvent(kafkaEvent.getWord(), total, kafkaEvent.getTimestamp());
        }
    }


    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent>{

        private static final long serialVersionUID = 4564078497295602259L;
        private long currentTimeStamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimeStamp-1);
        }

        @Override
        public long extractTimestamp(KafkaEvent kafkaEvent, long l) {
            this.currentTimeStamp = kafkaEvent.getTimestamp();
            return kafkaEvent.getTimestamp();
        }
    }
}
