package com.ocean.flink.pojo;

public class SensorReading {

    public String key;
    public Long end_of_window_timestamp;
    public Integer max_value;

    public SensorReading() {
    }

    public SensorReading(String key, Long end_of_window_timestamp, Integer max_value) {
        this.key = key;
        this.end_of_window_timestamp = end_of_window_timestamp;
        this.max_value = max_value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getEnd_of_window_timestamp() {
        return end_of_window_timestamp;
    }

    public void setEnd_of_window_timestamp(Long end_of_window_timestamp) {
        this.end_of_window_timestamp = end_of_window_timestamp;
    }

    public Integer getMax_value() {
        return max_value;
    }

    public void setMax_value(Integer max_value) {
        this.max_value = max_value;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "key='" + key + '\'' +
                ", end_of_window_timestamp=" + end_of_window_timestamp +
                ", max_value=" + max_value +
                '}';
    }
}
