package code.vikash.aera.assi.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class AeraKafkaProd

{
	private final String topic;
	private final Properties props;
	public static final String fileName = "aera/test.csv";
	public static final String OutfileName = "aera/outfile.csv";

	public AeraKafkaProd(String brokers, String username, String password) {
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

		String brokers = "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
		String username = "co53nqhg";
		String password = "KhY_plWJtWRI_9vqU-SimfULX-0sb_RG";
		AeraKafkaProd aera = new AeraKafkaProd(brokers, username, password);
		aera.streamDataThroughProducer();
		// For one Consumer
		aera.consumeDataSingle();
		// For three Consumer
		// aera.consumeData();
	}

	private void streamDataThroughProducer() {
		int lineCount = 0;
		FileInputStream fis;
		BufferedReader br = null;
		Producer<String, String> producer = new KafkaProducer<>(props);
		try {
			fis = new FileInputStream(fileName);
			br = new BufferedReader(new InputStreamReader(fis));
			int i = 0;
			String line = null;
			while ((line = br.readLine()) != null) {
				lineCount++;
				producer.send(new ProducerRecord<>(topic, Integer.toString(i), line.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// For Single Consumer

	private void consumeDataSingle() {
		KafkaConsumer<String, String> dataConsumer = new KafkaConsumer<>(props);
		dataConsumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = dataConsumer.poll(1000);
			System.out.println("data inserted into file");
			for (ConsumerRecord<String, String> record : records) {
				writeIntoFile(record.value() + "\n");
			}
		}
	}

	private static void writeIntoFile(String value) {
		File file = new File(OutfileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(value);
			bw.close();

		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	// For three Consumer

	private void consumeData() {
		int numConsumers = 3;
		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		final List<KafkaConsumerRunner> consumers = new ArrayList<KafkaConsumerRunner>();
		for (int i = 0; i < numConsumers; i++) {
			KafkaConsumerRunner consumer = new KafkaConsumerRunner(topic);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (KafkaConsumerRunner consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(500, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static class KafkaConsumerRunner implements Runnable {
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private final KafkaConsumer<String, String> consumer;
		private final String topic;
		public KafkaConsumerRunner(String topic)
		{
			String username = "co53nqhg";
			Properties props = getConfig();
			consumer = new KafkaConsumer<String, String>(props);
			this.topic = username + "-default";
		}

		private Properties getConfig() {

			// TODO Auto-generated method stub
			String brokers = "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
			String username = "co53nqhg";
			String password = "KhY_plWJtWRI_9vqU-SimfULX-0sb_RG";
			String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
			String jaasCfg = String.format(jaasTemplate, username, password);
			String serializer = StringSerializer.class.getName();
			String deserializer = StringDeserializer.class.getName();
			Properties props = new Properties();
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
			return props;
		}

		public void showRecord(ConsumerRecord record) {
			System.out.println("Consumer thread name: " + Thread.currentThread().getName() + " ; topic: "
					+ record.topic() + " ; offset" + record.offset() + " ; key: " + record.key() + " ; value: "
					+ record.value());
		}

		public void run() {
			try {
				consumer.subscribe(Arrays.asList(topic));
				while (!closed.get()) {
					ConsumerRecords<String, String> records = consumer.poll(1);
					for (ConsumerRecord<String, String> record : records) {
						showRecord(record);
						writeIntoFile(record.value() + "\n");
					}
				}

			} catch (WakeupException e) {
				if (!closed.get()) {
					throw e;
				}

			} finally {
				consumer.close();
			}
		}

		public void shutdown() {
			closed.set(true);
			consumer.wakeup();
		}

	}

}
