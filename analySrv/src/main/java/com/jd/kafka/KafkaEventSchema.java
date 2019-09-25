package com.jd.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaEventSchema implements DeserializationSchema<KafkaEvent>, SerializationSchema<KafkaEvent> {

    private static final long serialVersionUID = -6315545867612146367L;

    @Override
    public KafkaEvent deserialize(byte[] bytes) throws IOException {
        return new KafkaEvent().fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(KafkaEvent kafkaEvent) {
        return false;
    }

    @Override
    public byte[] serialize(KafkaEvent kafkaEvent) {
        return kafkaEvent.toString().getBytes();
    }

    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }
}
