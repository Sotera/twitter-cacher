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

import com.twitter.hbc.core.Client;

public class QueueWriter implements Runnable {
	
	private BlockingQueue<String> msgQueue;
	private String outputDirectory = "./data/";
	private BufferedWriter writer;
	
	private Client hosebirdClient;
	
	private static String FILE_PREFIX = "TWITTER_LOG_";
    private static String FILE_EXT = ".tsv";
	private static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy_MMM_dd_HH-mm-ss_z");
	
	public QueueWriter(BlockingQueue<String> q, Client c) throws IOException {
		msgQueue = q;
		writer = createNewWriter();
		hosebirdClient = c;
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
					writer.write(msg);
		            //writer.newLine();
				} catch(IOException ioe) {
					ioe.printStackTrace();
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
}
