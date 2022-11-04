package edu.upenn.cis.nets2120.hw3;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.hw3.livy.MyPair;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;

public class ComputeRanks {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ComputeRanks.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	List<MyPair<Integer,Double>> resultList;
	
	String[] gArgs;
	
	public ComputeRanks() {
		System.setProperty("file.encoding", "UTF-8");
	}
	
	
	public void setArgs(String[] a) {
		this.gArgs = a;
	}
	
	public String[] getArgs() {
		return gArgs;
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		//Read into RDD with lines as strings. Removes whitespace through \\s
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split("\\s+"));
		
		JavaPairRDD<Integer,Integer> followingNode = file.mapToPair(line -> {
			int nodeID = Integer.parseInt(line[0]);
			int followingID = Integer.parseInt(line[1]);
			
			return new Tuple2<Integer,Integer>(followingID, nodeID);
		});
		
    return followingNode;
    // TODO Load the file filePath into an RDD (take care to handle both spaces and tab characters as separators)
	}
	/*
		Separates followed and follower into two single RDD. To find the sinks, we subtract the total
		distinct nodes from the non-(source,sink) and source nodes. The sinks are then returned as RDD.
	*/
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		
		JavaPairRDD<Integer,Integer> networkDistinctRDD = network.distinct();
		
		JavaRDD<Integer> followedRDD = networkDistinctRDD.map(line -> {
			return (line._1());
		});
		
		JavaRDD<Integer> followerRDD = networkDistinctRDD.map(line -> {
			return (line._2());
		});
		
		JavaRDD<Integer> totalNodeRDD = followedRDD.union(followerRDD).distinct();
		
		JavaRDD<Integer> sinksRDD = totalNodeRDD.subtract(followerRDD.distinct());
		
		System.out.println("This graph contains " + totalNodeRDD.count()
			+ " nodes and " + networkDistinctRDD.count() + " edges");
		
		// TODO Find the sinks in the provided graph
    return sinksRDD;
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run() throws IOException, InterruptedException {
		System.out.println("Running");

		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(Config.SOCIAL_NET_PATH).distinct();

		JavaPairRDD<Integer,Character> sinksRDD = getSinks(network).mapToPair(x -> {
			return new Tuple2<Integer,Character> (x, 'a');
		});
		
		/*
			Joined the sinksRDD with the network rdd to find the nodes I can backtrack. After that we create a
			new RDD that has the backlinks to which we combine with union. 
		*/
		JavaPairRDD<Integer,Integer> networkBERDD = network.join(sinksRDD).mapToPair(x -> {
			return new Tuple2<Integer,Integer> (x._2()._1(), x._1());
		});
		
		JavaPairRDD<Integer,Integer> fromNodeToNodeRDD = network.union(networkBERDD).mapToPair(x -> {
			return new Tuple2<Integer,Integer> (x._2(), x._1());
		});
		
		System.out.println("Added " + networkBERDD.count() + " backlinks");
	// TODO find the sinks
    // TODO add back-edges
		
		//Assign weight to backlink (b, 1/N_b)
		JavaPairRDD<Integer, Double> nodeTransferRDD = fromNodeToNodeRDD
				.mapToPair(x -> new Tuple2<Integer, Double>(x._1(), 1.0))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(x -> new Tuple2<Integer, Double>(x._1(), (1.0 / x._2())));
				
		//(b, [p, 1/N_b])
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = fromNodeToNodeRDD.join(nodeTransferRDD);

//		edgeTransferRDD.collect().stream().forEach(x -> {
//			System.out.println(x._1() + ": (" + x._2()._1() + ", " + x._2()._2() + ")");
//		});
		
		double d = 0.15;
		int iterMax = 25;
		double deltaMax = 30.0;
		double delta = Double.MAX_VALUE;
		int counter = 0;
		boolean debug = false;
		
		JavaPairRDD<Integer, Double> pageRankRDD = fromNodeToNodeRDD
				.mapToPair(x -> new Tuple2<Integer, Double>(x._1(), 1.0));
		
//		edgeTransferRDD.join(pageRankRDD).collect().stream().forEach(x -> {
//			System.out.println(x._1 + ", [(" + x._2._1._1 + ", " + x._2._1._2 + "), " +x._2._2 + "]");
//		});
		
		/*
			Configure the args, if there is 1, we update the delta condition, if there are 2 we update the
			maximum iterations, if there are 3, we enter debug mode.
		*/
		if(gArgs.length > 0) {
			deltaMax = Double.parseDouble(gArgs[0]);
		}
		if(gArgs.length > 1) {
			iterMax = Integer.parseInt(gArgs[1]);
		}
		if(gArgs.length > 2) {
			debug = true;
		}
		
		System.out.println("deltaMax: " + deltaMax);
		System.out.println("iterMax: " + iterMax);
		System.out.println("Debug: " + debug);
		while(delta >= deltaMax && counter < iterMax) {
			//(b, [(p, 1/N_b), Pr(b)])
			JavaPairRDD<Integer, Double> propagateRDD = edgeTransferRDD
					.join(pageRankRDD)
					.distinct()
					// (p, Pr(b) * 1/N_b)
					.mapToPair(x -> new Tuple2<Integer,Double>(x._2()._1()._1(), x._2()._2() * x._2()._1()._2()));
			 
			//(p, sum of propagation with decay factor)
			JavaPairRDD<Integer, Double> pageRankRDD2 = propagateRDD
					.reduceByKey((a,b) -> a + b)
					.mapToPair(x -> new Tuple2<Integer, Double>(x._1(), d + (1 - d) * x._2()));
			
			//Debug Mode:
			if(debug) {
				System.out.println("[" + counter + "]");
				pageRankRDD.collect().stream().forEach(x -> {
					System.out.println(x._1() + ": " + x._2());
				});
			}
			//Find the largest delta from the previous iteration.
			JavaPairRDD<Double, Integer> pageRanksDeltaRDD = pageRankRDD2.union(pageRankRDD)
					.reduceByKey((a,b) -> Math.abs(a - b))
					.mapToPair(x -> new Tuple2<Double, Integer>(x._2(), x._1()))
					.sortByKey(false, Config.PARTITIONS);
			
//			System.out.println("dif");
//			pageRanksDeltaRDD.collect().stream().forEach(x -> {
//				System.out.println((x._1() + ": " + x._2()));
//			});
			
			delta = pageRanksDeltaRDD.map(x -> x._1()).take(1).get(0);
			pageRankRDD = pageRankRDD2;
			counter++;
		}
		
		//Get the top ten rank
		List<Tuple2<Integer,Double>> topTenRankList = pageRankRDD
				.mapToPair(x -> new Tuple2<Double, Integer>(x._2(), x._1()))
				.sortByKey(false, Config.PARTITIONS)
				.mapToPair(x -> new Tuple2<Integer, Double>(x._2(), x._1()))
				.take(10);
		
		resultList = new ArrayList<>();
		topTenRankList.forEach(x -> resultList.add(new MyPair<Integer,Double>(x._1(), x._2())));
		//Sort page rank
		
		
		System.out.println("*** Finished social network ranking! ***");
	}
	
	public List<MyPair<Integer, Double>> getResultList() {
		return resultList;
	}

	/**
	 * Graceful shutdown
	 */
//	public void shutdown() {
//		logger.info("Shutting down");
//
//		if (spark != null)
//			spark.close();
//	}
	
	

	public static void main(String[] args) {
		final ComputeRanks cr = new ComputeRanks();

		try {
			cr.initialize();

			cr.setArgs(args);
			
			cr.run();
		} catch (final IOException ie) {
			System.err.println("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
//			cr.shutdown();
		}
	}

}
