package ir.example.chapter9;

public class SensorData {

    private Integer sensorId;
    private Integer value;

    public SensorData() {
    }

    public SensorData(Integer sensorId, Integer value) {
        this.sensorId = sensorId;
        this.value = value;
    }

    public Integer getSensorId() {
        return sensorId;
    }

    public void setSensorId(Integer sensorId) {
        this.sensorId = sensorId;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

}
