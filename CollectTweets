package com;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.Calendar;
import java.util.Date;

import javax.xml.crypto.Data;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterMain {

	public static void main(String[] args) throws IOException, TwitterException {

		/*-----------------------------------Configuration-----------------------------------------*/
		 
		//Your Twitter App's Consumer Key
        String consumerKey = "LW0n4aOcS1pwT5ZmIgJhQ";
 
        //Your Twitter App's Consumer Secret
        String consumerSecret = "ddOENJjo3PJoNhclj3CdACQmFaSLL0Q9taBRXFHkZA";
 
        //Your Twitter Access Token
        String accessToken = "1285070930-QJPj4yqREn3wLHu6xnOhew6bbnUZo011myNR5kp";
 
        //Your Twitter Access Token Secret
        String accessTokenSecret = "C5xmsDUWB8xj1vaHGJWP4opQ95yU95JTjMFpvxDdlaZKW";
        
        //Instantiate a re-usable and thread-safe factory
        TwitterFactory twitterFactory = new TwitterFactory();
 
        //Instantiate a new Twitter instance
        Twitter twitter = twitterFactory.getInstance();
        
        //setup OAuth Consumer Credentials
        twitter.setOAuthConsumer(consumerKey, consumerSecret);
 
        //setup OAuth Access Token
        twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret)); 
        
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("LW0n4aOcS1pwT5ZmIgJhQ");
        cb.setOAuthConsumerSecret("ddOENJjo3PJoNhclj3CdACQmFaSLL0Q9taBRXFHkZA");
        cb.setOAuthAccessToken("1285070930-QJPj4yqREn3wLHu6xnOhew6bbnUZo011myNR5kp");
        cb.setOAuthAccessTokenSecret("C5xmsDUWB8xj1vaHGJWP4opQ95yU95JTjMFpvxDdlaZKW");

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        
        final String filepath = "D:\\Workspace\\test.csv";

/*-----------------------------------Update Status------------------------------------------*/        
/*
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        System.out.println(dateFormat.format(cal.getTime()));        
        
        //tweet or update status
        Status status = twitter.updateStatus(dateFormat.format(cal.getTime()));
 
        //response from twitter server

        if(status.getRateLimitStatus() != null)
        {
            System.out.println("status.getRateLimitStatus().getLimit() = " + status.getRateLimitStatus().getLimit());
            System.out.println("status.getRateLimitStatus().getRemaining() = " + status.getRateLimitStatus().getRemaining());
            System.out.println("status.getRateLimitStatus().getResetTimeInSeconds() = " + status.getRateLimitStatus().getResetTimeInSeconds());
            System.out.println("status.getRateLimitStatus().getSecondsUntilReset() = " + status.getRateLimitStatus().getSecondsUntilReset());
        }
        System.out.println("Successfully updated the status to [" + status.getText() + "].");

*/        
/*------------------------------Tweets Collection (Streaming)--------------------------------*/
		
        
        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onStatus(Status status) {
                
            	
            	double Lat = status.getGeoLocation().getLatitude();
                double Long = status.getGeoLocation().getLongitude();
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = status.getCreatedAt();
                
            	System.out.println("@" + status.getUser().getScreenName() + "\n" + Lat + "," + Long + "\n" + status.getText() + "\n ------------------------\n");            	            	          	
        	    
            	Connection c = null;
                Statement stmt = null;                                          
                try {
                    Class.forName("org.postgresql.Driver");
                    c = DriverManager
                       .getConnection("jdbc:postgresql://54.87.78.57:5432/Twitter",
                       "postgres", "2014dataiscool!");
                    c.setAutoCommit(false);
                    System.out.println("Opened database successfully");

                    stmt = c.createStatement();
                    String sql = "INSERT INTO tweets (latitude,longitude,date) "
                          + "VALUES (" + String.valueOf(Lat) + "," + String.valueOf(Long)
                          + "," + "'" + String.valueOf(dateFormat.format(date)) + "'" + ");";
                    System.out.println(sql);
                    stmt.executeUpdate(sql);
                    stmt.close();
                    c.commit();
                    c.close();
                     
                    FileWriter writer = new FileWriter(filepath, true);
 					
 				   	writer.append(String.valueOf(Lat));
 					writer.append(',');
 					writer.append(String.valueOf(Long));
 					writer.append(',');
 					writer.append(String.valueOf(dateFormat.format(date)));
 					writer.append("\n");
 					
 					writer.flush();
 				    writer.close();
                     
                     
                     
                } catch (Exception e) {
                     System.err.println( e.getClass().getName()+": "+ e.getMessage() );
                     System.exit(0);
                  }
            	
            }            

            @Override
            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub

            }

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}

        };
        
        FileWriter writer = new FileWriter(filepath, true);
        writer.append("lat");
		writer.append(',');
		writer.append("long");
		writer.append(',');
		writer.append("time");
		writer.append("\n");
		
		writer.flush();
	    writer.close();
        
        FilterQuery fq = new FilterQuery();
    
        String keywords[] = {"the"};

        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq);                
        
    }
}
