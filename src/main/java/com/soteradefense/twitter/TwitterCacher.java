package com.soteradefense.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

public class TwitterCacher implements StatusListener {
	
	private static String FILE_PREFIX = "TWITTER_LOG_";
    private static String FILE_EXT = ".tsv";
    private static String PROFILE_DIRECTORY = "profiles";
    private static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy_MMM_dd_HH-mm-ss_z");
    
	private TwitterStream twitterStream;
	private BufferedWriter writer;
	private Integer statusesRecieved = 0;

    private String outputDirectory;
	
	TwitterCacher(String consumerKey, String consumerSecret, String token, String secret, String outputDirectory) throws TwitterException, IOException, LangDetectException {

        this.outputDirectory = outputDirectory;
        DetectorFactory.loadProfile(PROFILE_DIRECTORY);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(token);
        cb.setOAuthAccessTokenSecret(secret);
        
        //OAuthAuthorization auth = new OAuthAuthorization();
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        if(twitterStream.getAuthorization() != null) {

            twitterStream.addListener(this);
            writer = createNewWriter();
        }
        
    }

	public static void main(String[] args) {
		System.out.println("Starting TwitterCacher");
		
		Properties prop = new Properties();
		 
		try {
	        //load a properties file
			prop.load(new FileInputStream("config.properties"));
			
			TwitterCacher cacher = new TwitterCacher(
					prop.getProperty("oauth.consumerKey"),
					prop.getProperty("oauth.consumerSecret"),
					prop.getProperty("oauth.accessToken"),
					prop.getProperty("oauth.accessTokenSecret"),
					prop.getProperty("outputDir"));
			
			cacher.startConsuming();
	
		} catch (IOException ex) {
			ex.printStackTrace();
	    } catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LangDetectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void startConsuming() throws TwitterException {
        double[][] bounds = {{-180,-90},{180,90}};
        twitterStream.filter(new FilterQuery().locations(bounds));
    }

	private BufferedWriter createNewWriter() throws FileNotFoundException, UnsupportedEncodingException, IOException {
		  Date time = Calendar.getInstance().getTime();
		  
		  File dataDir = new File(outputDirectory);
		  // if the directory does not exist, create it
		  if (!dataDir.exists()) {
		    dataDir.mkdir();
		  }
		  
		  String filename = outputDirectory + FILE_PREFIX + TIME_FORMAT.format(time) + FILE_EXT;
		  File writerFile = new File(filename);
		  if(!writerFile.exists()) {
		  	writerFile.createNewFile();
		  } 
		  FileOutputStream fileOutputStream = new FileOutputStream(writerFile);
		  
		  OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( fileOutputStream , "UTF-8");
		
		  return new BufferedWriter(outputStreamWriter);
	}
	
	private void startNewLog() {
		System.out.println("startNewLog");
	    try {
	        writer.flush();
	        writer.close();
	        writer = createNewWriter();
	    } catch (IOException e) {
	        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
	    }
	}
	
	private String detectLanguage(String text) {
        try {

            Detector langDetector = DetectorFactory.create();
            langDetector.append(text);
            return langDetector.detect();

        } catch (LangDetectException e) {

            return "";

        }
    }

	public void onException(Exception arg0) {
		// TODO Auto-generated method stub
		
	}

	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub
		
	}

	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}

	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}

	public void onStatus(Status status) {
		
        String id = String.valueOf(status.getId());
        String text = status.getText().replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
        String timestamp = status.getCreatedAt().toString();
        String user = status.getUser().getScreenName().replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
        String userLocation = status.getUser().getLocation().replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
        String latitude = (status.getGeoLocation() != null) ? String.valueOf(status.getGeoLocation().getLatitude()) : "";
        String longitude = (status.getGeoLocation() != null) ? String.valueOf(status.getGeoLocation().getLongitude()) : "";

        String language = detectLanguage(text);
        
        String source = status.getSource().replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");

        String tweetRecord = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", id, timestamp, user, userLocation, latitude, longitude, text, language, source);

        try {
            writer.write(tweetRecord);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        statusesRecieved++;
        if(statusesRecieved%10==0) {
            System.out.print(".");
        }
        if(statusesRecieved%1000==0) {
            System.out.println();
            System.out.println( "[" + Calendar.getInstance().getTime() + "] " + statusesRecieved + " Statuses received.");
        }
    	if(statusesRecieved%500000==0) {
            startNewLog();
    	}
    }

	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub
		
	}
}
