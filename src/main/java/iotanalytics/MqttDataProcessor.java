package iotanalytics;

import java.io.IOException;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;

public class MqttDataProcessor {

		/**
	     * Method implements connection to MQTT broker.
	     * @return
	     * @throws MqttException
	     */
		
		String broker = "tcp://mqtt-dashboard.com";
		String clientID = "client1";
		String SensorData= "/gatewayid/1234/SensorData";
		MqttAsyncClient   client  =null;
	public boolean connectToMqttServer() throws MqttException {
	    try {
	    	client = new  MqttAsyncClient(
	        		broker,
	        MqttClient.generateClientId(),
	        new MemoryPersistence());
	        // register a callback for messages that we subscribe to ...
	        MqttConnectOptions options = new MqttConnectOptions();
	        options.setMaxInflight(65000);
	        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
	     //   options.setUserName("emq");
	       // options.setPassword("public".toCharArray());
	        options.setCleanSession(false);
	        options.setAutomaticReconnect(true);
	  //	  CascadingAnomalyDetection anomaly = new CascadingAnomalyDetection();

	        client.setCallback(new MqttCallbackExtended() {
	            @Override
	            public void connectComplete(boolean reconnect, String serverURI) {
	                try {
	                    System.out.println("Connection complete \n");

	                   client.subscribe(SensorData, 0);
	                } catch (MqttException e) {
	                    e.printStackTrace();
	                }
	            }

	            @Override
	            public void connectionLost(Throwable cause) {
	                cause.printStackTrace();
	            }

	            @Override
	            public void messageArrived(String topic, MqttMessage message) throws Exception {
	            	
	            	  String  messageBody = new String(message.getPayload());
	                  
	                  System.out.println("Data Recevied \n"+messageBody);

	                  System.out.println("Topic is "+topic);

	            	 //   static String GatewayRegistrationTopic  = "/IoTRegisterGateway";
	            	  //  static String SensorRegistrationTopic  = "/IoTRegisterSensor/Include";
	            	   // static String SensorDeRegistrationTopic  = "/IoTRegisterSensor/Exclude";
	                  
	            //	  IoTGatewayConfigWizard  sensorstream = new IoTGatewayConfigWizard();

	                  if(topic.equals("/IoTRegisterGateway"))
	                  {
	                	  
	                	//  sensorstream.visualizeRegistrationData(messageBody);

	                  }
	                  
	                  if(topic.equals("/gatewayid/1234xyz76/HealthNet/SensorData"))
	                  {
	                      System.out.println("Data Recevied \n"+messageBody);

	                //      sensorstream.visualizeRegistrationData(messageBody);

	                  }

	                  if(topic.equals("/gatewayid/1234xyz76/HealthNet/SensorData"))
	                  {
	                	//  System.out.println("Vehicle Sensor Data Recevied "+messageBody);
	                //	  sensorstream.visualizeSensorStreamingData(messageBody);

	               /////   Check Automotive variable anomaly analysis 
	                	  /*
	                	  if(messageBody.contains("coolantval"))
	                	  {
	                		     anomaly.monitorVehicleSensorData(messageBody);
	                	  }    
	                	  if(messageBody.contains("SystolicBP"))
	                	  {                	 
	                		  anomaly.monitorHelathCareData(messageBody);
	                	  }
	                	  //SoilMoisture
	                	  if(messageBody.contains("SoilMoisture"))
	                	  {                	 
	                		  anomaly.monitorAgriData(messageBody);
	                	  }*/
	                  }
	                  if(topic.equals("/gatewayid/1234/BACNet/SensorData"))
	                  {
	                	  System.out.println("BACnet Data Recevied "+messageBody);
	                	//   displaySensorData
	             //   	  sensorstream.visualizeRegistrationData(messageBody);
	                //	  anomaly.vehicledataCascadingAnomaly(messageBody);
	                  }
	                  if(topic.equals("/gatewayid/1234/Energydata/PeakLoadMonitoring"))
	                  {
	                	  System.out.println("Smart Energy  Data Recevied "+messageBody);
	                //	  Energydata data = new Energydata();
	                //	  parseenergydata(messageBody);
	                	  //{"rmscurrent":"12445","reactivepower:"456"}
	                	  
	                 	//  anomaly.vehicledataCascadingAnomaly(messageBody);
	                  }
	        }

	          public void parseenergydata(String data) throws JSONException, JsonParseException, IOException
	          {
	        	
	        	  
	        	//  "{"rmscurrent":"12476","reactivepower":"458"}"
	        	//  "{\"
	        	//  String data = {"rmscurrent":"1","k2":"v2"};
	        	//  String string = "{\"name\": \"Sam Smith\", \"technology\": \"Python\"}";  
	        	//String data =  "{\"rmscurrent\": \"12345"\"}";

	        	 // String string = "{\"name\": \"Sam Smith\", \"technology\": \"Python\"}";  
	        	//  String string = "{\"rmscurrent\": \"298\", \"reactivepower\": \"4567\"}";  


	        /*	  ObjectMapper mapper = new ObjectMapper();
	        	    JsonFactory factory = mapper.getFactory();
	        	    JsonParser parser = factory.createParser(data);
	        	    JsonNode actualObj = mapper.readTree(parser);
	        	    JsonNode rmsnode = actualObj.get("name");*/

	        	  JSONObject json = new JSONObject(data);
	              // print object
	              System.out.println(json.toString());
	              // get value for a key
	              String rmsval = json.getString("rmscurrent");
	           //   json.getClass(energy)
	              
	            //  json.getClass()
	              System.out.println("current"+rmsval);
	              
	        	 	//  System.out.println("Smart Energy  Data Recevied "+rmsnode.has("name"));


	          }
	        

	            @Override
	            public void deliveryComplete(IMqttDeliveryToken token) {
	            }

				

			
	        });
	        IMqttActionListener connectionListener = new IMqttActionListener() {

	            @Override
	            public void onSuccess(IMqttToken arg0) {
	            	System.out.println("Connection successfull with broker : " +broker);
	            }

	            @Override
	            public void onFailure(IMqttToken arg0, Throwable arg1) {
	              //  connectionFailureCallback();
	                System.out.println("Failed to connect to broker ");
	             //   reconnect();
	            }

				
	        };
	        IMqttToken conToken = client.connect(options, "Connect async client to server", connectionListener);
	        conToken.waitForCompletion();
	        //connecting client to server
	        if (!client.isConnected()) {
	            //LOG.error("Onem2mMqttClient: trouble connecting to server");
	            return false;
	        }
	        return true;
	    } catch (MqttException e) {
	       // LOG.debug("Onem2mMqttClient: error occurred when connecting to server", e);
	        return false;
	    }
	}

	public void mqttsubscribe(String subtopic)
	{
		int qos=2;

	    if (client != null && client.isConnected()) {
	        try {
				client.subscribe(subtopic, qos, null);
			} catch (MqttException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    } else {
	        System.out.println("Client connected is null ");

	     }
	}

	public void mqttpublish(String pubtopic,String  pubdata)
	{
		int qos=1;
	    System.out.println("Publish data is "+pubdata);

		MqttMessage pubpayload = new MqttMessage();
		byte[] data = pubdata.getBytes();
		pubpayload.setPayload(data);
	    if (client != null && client.isConnected()) {
	        try {
				client.publish(pubtopic, pubpayload);
			} catch (MqttException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    } else {
	        System.out.println("Client connected is null ");

	     }
	}
	}

	/////////////

