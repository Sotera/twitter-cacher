package twitter_stream.TwitterCacher2;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class Cacher {

	public static void main(String[] args) {
		System.out.println("Starting TwitterCacher");
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		//BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StreamingEndpoint endpoint = new StatusesFilterEndpoint();
		
		endpoint.addPostParameter("locations", "-180,-90,180,90");
		endpoint.addPostParameter("stall_warnings", "true");

		Authentication hosebirdAuth = new OAuth1(
				"9HQ9STJVhX8d475AtoVNQ", 
				"jpMGT3Bs2O7TwLAvsIJWLjx4VSZGRXqGkkEyJu6oYs", 
				"2244461120-78DdQxy3TE0FMA2QW2DqyaCdA3lTcvLrxwW94Sy", 
				"pMmqEbByI78GXv08aIGDDSZaIud2OF3XQl6alCWRVKKTq");

		ClientBuilder builder = new ClientBuilder()
		  .name("Hosebird-Client-01")                              // optional: mainly for the logs
		  .hosts(hosebirdHosts)
		  .authentication(hosebirdAuth)
		  .endpoint(endpoint)
		  .processor(new StringDelimitedProcessor(msgQueue));
		  //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		//hosebirdClient.connect();
		
		try {
			Thread t = new Thread(new QueueWriter(msgQueue, hosebirdClient));
	        t.start();
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
        
		//hosebirdClient.stop();
	}

}
