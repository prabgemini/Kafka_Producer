package SykesDemo.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterModified {

	private static final String topic = "twitter-topic";
	private final KafkaProducer<String, String> producer;
	private final Boolean isAsync;
	String consumerKey = "Jzp0bOisECssdJs0H406uKKEW";
    String consumerSecret = "y87gTNI11AtGpjIqVrSkON5kn5IW05mutH53KqbhogsUY25W3R";
    String accessToken = "57676049-I4twV5T0bSCov74eJLIjZUcnnZc4MxGVLuLktUWPZ";
    String accessTokenSecret = "8ULFKtomSdXMT2ppO1i3VbJYfC1Q8rm4TjjqFMmopDM3H";
	
	public TwitterModified(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "52.42.244.153:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        this.isAsync = isAsync;
    }
	
	public void sendMessage(String key, String value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(new ProducerRecord<String, String>(topic, key),
                    (Callback) new DemoCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<String, String>(topic, key, value)).get();
                System.out.println("Sent message: (" + key + ", " + value + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
	
	//old twitter kafka
	public static void run() throws InterruptedException {

		String consumerKey = "Jzp0bOisECssdJs0H406uKKEW";
	    String consumerSecret = "y87gTNI11AtGpjIqVrSkON5kn5IW05mutH53KqbhogsUY25W3R";
	    String accessToken = "57676049-I4twV5T0bSCov74eJLIjZUcnnZc4MxGVLuLktUWPZ";
	    String accessTokenSecret = "8ULFKtomSdXMT2ppO1i3VbJYfC1Q8rm4TjjqFMmopDM3H";
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "52.42.244.153:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id","camus");
		
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);
		
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(Lists.newArrayList("#bigdata"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,
				accessTokenSecret);
		// Authentication auth = new BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}
		producer.close();
		client.stop();

	}

	public static void main(String[] args) {
		try {
			KafkaBroker.run();
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
