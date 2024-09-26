package iotanalytics;

import org.eclipse.paho.client.mqttv3.MqttException;

public class IoTSensorDataManager {

	public static void main(String[] args) throws MqttException {
		// TODO Auto-generated method stub
		MqttDataProcessor iotdatacontroller = new MqttDataProcessor();
		iotdatacontroller.connectToMqttServer();
		iotdatacontroller.mqttsubscribe("sensordata");
		
	}

}
