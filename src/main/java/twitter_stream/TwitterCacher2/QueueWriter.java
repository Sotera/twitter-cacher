package twitter_stream.TwitterCacher2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import twitter4j.internal.org.json.JSONArray;
import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.twitter.hbc.core.Client;

public class QueueWriter implements Runnable {
	
	private BlockingQueue<String> msgQueue;
	private String outputDirectory = "./data/";
	private BufferedWriter writer;
	
	private static String PROFILE_DIRECTORY = "src/main/resources/profiles";
	
	private Client hosebirdClient;
	
	private static String FILE_PREFIX = "TWITTER_LOG_";
    private static String FILE_EXT = ".tsv";
	private static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy_MMM_dd_HH-mm-ss_z");
	
	public QueueWriter(BlockingQueue<String> q, Client c) throws IOException, LangDetectException {
		msgQueue = q;
		writer = createNewWriter();
		hosebirdClient = c;
		
		DetectorFactory.loadProfile(PROFILE_DIRECTORY);
	}

	public void run() {
		Date start = new Date();
		
		hosebirdClient.connect();
		
		int count = 0;
		Date oldDate = new Date();
		while(true) {
			try {
				String msg = msgQueue.take();
				Date midDate = new Date();
				double lastMsg = (midDate.getTime()-oldDate.getTime())/1000;
				if(lastMsg > 30) {
					System.out.println(lastMsg+" seconds have passed since last msg!");
				}
				oldDate = midDate;
				count++;
				if(count % 1000 == 0) {
					double seconds = (midDate.getTime()-start.getTime())/1000;
					System.out.println(count+" records found in "+seconds+" seconds. queue size: "+msgQueue.size());
				}
				//System.out.println(msg);
				try {
					JSONObject tweetJson = new JSONObject(msg);
					
					if(tweetJson.has("id_str")) {
						String id = String.valueOf(tweetJson.getString("id_str"));
				        String text = tweetJson.getString("text").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
				        String timestamp = tweetJson.getString("created_at");
				        JSONObject user = tweetJson.getJSONObject("user");
				        String screenName = user.getString("screen_name").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
				        String userLocation = user.getString("location").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
				        
				        String latitude = "";
				        String longitude = "";
				        if(tweetJson.getString("coordinates") != null) {
				        	JSONArray coords = tweetJson.getJSONObject("coordinates").getJSONArray("coordinates");
				        	longitude = coords.getString(0);
				        	latitude = coords.getString(1);
				        }
				        
				        String language = detectLanguage(text);
				        
				        String source = tweetJson.getString("source").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ");
	
				        String tweetRecord = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", id, timestamp, screenName, userLocation, latitude, longitude, text, language, source);
						writer.write(tweetRecord);
						writer.newLine();
					} else {
						System.out.println(msg);
					}
		            //writer.newLine();
				} catch(IOException ioe) {
					ioe.printStackTrace();
				} catch(JSONException je) {
					je.printStackTrace();
				}
				if(count % 50000 == 0) {
					startNewLog();
				}
			} catch(InterruptedException ie) {
				ie.printStackTrace();
			}
		}
		
		//hosebirdClient.stop();
		
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
        	e.printStackTrace();
            return "";
        }
    }
}
