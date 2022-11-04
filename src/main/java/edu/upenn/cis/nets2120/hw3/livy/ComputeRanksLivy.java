package edu.upenn.cis.nets2120.hw3.livy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import edu.upenn.cis.nets2120.config.Config;

public class ComputeRanksLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-54-224-109-252.compute-1.amazonaws.com:8998/"))
				  .build();

		try {
			String jar = "target/nets2120-hw3-0.0.1-SNAPSHOT.jar";
			
			System.out.printf("Uploading %s to the Spark context...\n", jar);
			client.uploadJar(new File(jar)).get();
			
			String sourceFile = Config.SOCIAL_NET_PATH;// .BIGGER_SOCIAL_NET_PATH; // 
			
			System.out.printf("Running SocialRankJob with %s as its input...\n", sourceFile);
		  
			System.out.println("Start with Backlink");
			List<MyPair<Integer,Double>> result1 = client.submit(new SocialRankJob(true, sourceFile)).get();
			System.out.println("With backlinks: " + result1);
			
			//result1.txt from here
			System.out.println("Start without Backlink");
			List<MyPair<Integer,Double>> result2 = client.submit(new SocialRankJob(false, sourceFile)).get();
			System.out.println("With backlinks: " + result2);
			
			List<Integer> resultWBE = new ArrayList<>();
			List<Integer> resultWOBE = new ArrayList<>();
			
			result1.forEach(x -> resultWBE.add(x.getLeft()));
			result2.forEach(x -> resultWOBE.add(x.getLeft()));
		  
			List<Integer> resultIntersect = new ArrayList<>(resultWBE);
			resultIntersect.retainAll(resultWOBE);
		  
			FileWriter file = new FileWriter("results1.txt");
			
			file.write("Nodes common betweeen two top-10 lists: ");
			for(Integer t10Is : resultIntersect) {
				file.write(t10Is + ", ");
			}
			file.write("\n");
			file.write("\n");
			
			resultWBE.removeAll(resultIntersect);
			file.write("Nodes exculsive to computation with back-links: ");
			for(Integer t101 : resultWBE) {
				file.write(t101 + ", ");
			}
			file.write("\n");
			file.write("\n");
			
			resultWOBE.removeAll(resultIntersect);
			file.write("Nodes exculsive to computation without back-links: ");
			for(Integer t102 : resultWOBE) {
				file.write(t102 + ", ");
			}
			//result1.txt to here
			
			//result2.txt from here
			/*
			FileWriter file = new FileWriter("results2.txt");
			
			file.write("Nodes and their ranks lists: ");
			for(MyPair<Integer, Double> t10Is : result1) {
				file.write(t10Is + "\n");
			}
			
			//result2.txt to here
			*/
			file.close();
			
			
		  // TODO write the output to a file that you'll upload.
		} catch (IOException e) {
			System.err.println("An error occured. FILE IOException");
			e.printStackTrace();
		} finally {
		  client.stop(true);
		}
	}

}
