package code.vikash.aera.assi.main;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class AeraKafkaProd 
{
	private final String topic;
	private final Properties props;
	public static final String fileName = "aera/test.csv";

	public AeraKafkaProd(String brokers, String username, String password)
	{
		this.topic = username + "-default";
		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, username, password);
		String serializer = StringSerializer.class.getName();
		String deserializer = StringDeserializer.class.getName();
		props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", username + "-consumer");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", deserializer);
		props.put("value.deserializer", deserializer);
		props.put("key.serializer", serializer);
		props.put("value.serializer", serializer);
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.mechanism", "SCRAM-SHA-256");
		props.put("sasl.jaas.config", jaasCfg);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String brokers = "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
		String username = "co53nqhg";
		String password = "KhY_plWJtWRI_9vqU-SimfULX-0sb_RG";
		AeraKafkaProd aera =new AeraKafkaProd(brokers, username, password);
		aera.streamDataThroughProducer();
		aera.ConsumeData();

	}

	private void ConsumeData() {
		KafkaConsumer<String, String> dataConsumer = new KafkaConsumer<>(props);
		dataConsumer.subscribe(Arrays.asList(topic));
		//dataConsumer.subscribe(Collections.singletonList("customerCountries"));
		while (true) 
		{
			ConsumerRecords<String, String> records = dataConsumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) 
			{
				//System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n", record.topic(), record.partition(),record.offset(), record.key(), record.value());
				System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
			}
		}
		
	}

	private void streamDataThroughProducer() {
		// TODO Auto-generated method stub
		int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;
        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            fis = new FileInputStream(fileName);
            br = new BufferedReader(new InputStreamReader(fis));
            int i = 0;
            String line = null;
            while ((line = br.readLine()) != null) 
            {
                lineCount++;
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), line.toString()));
            }

        } catch (Exception e) 
        {
            e.printStackTrace();
        }finally
        {
            try 
            {
                br.close();
            } catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

		
	}

}
