package com.jd.kafka;

public class KafkaEvent {

    private final static String splitString = "##";
    private String word;
    private int frequency;
    private long timestamp;

    public String getWord() {
        return word;
    }

    public int getFrequency() {
        return frequency;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public KafkaEvent(){}

    public KafkaEvent(String word, int frequency, long timestamp){
        this.word = word;
        this.frequency = frequency;
        this.timestamp = timestamp;
    }

    public KafkaEvent fromString(String str){
        String[] s = str.split(splitString);
        return new KafkaEvent(s[0], Integer.valueOf(s[1]), Long.valueOf(s[2]));
    }

    @Override
    public String toString(){
        String kafkaEventString = word + splitString + frequency +splitString + timestamp;
        return kafkaEventString;
    }


}
