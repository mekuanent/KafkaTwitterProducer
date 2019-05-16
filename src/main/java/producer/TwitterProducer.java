package producer;

import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import model.Tweet;
import org.apache.kafka.clients.producer.Producer;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Mekuanent Kassaye on 2019-05-16.
 */
public class TwitterProducer {

    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;

    public TwitterProducer() {

        Authentication authentication = new OAuth1(
                TwitterConfiguration.CONSUMER_KEY,
                TwitterConfiguration.CONSUMER_SECRET,
                TwitterConfiguration.ACCESS_TOKEN,
                TwitterConfiguration.TOKEN_SECRET);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Arrays.asList(TwitterConfiguration.HASHTAGS));

        queue = new LinkedBlockingQueue<>(20000);

        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        gson = new Gson();

    }

    public void run(){
        client.connect();
        try {
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.printf("Fetched tweet id %d %s\n", tweet.getId(), tweet.getText());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    public static void main(String[] args) {
        TwitterProducer producer = new TwitterProducer();
        producer.run();
    }

    public static class TwitterConfiguration{
        public static final String CONSUMER_KEY = "...";
        public static final String CONSUMER_SECRET = "...";
        public static final String ACCESS_TOKEN = "...";
        public static final String TOKEN_SECRET = "...";
        public static final String[] HASHTAGS = new String[]{"election"};
    }
}
