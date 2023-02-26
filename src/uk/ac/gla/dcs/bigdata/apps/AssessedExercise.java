package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.util.CollectionAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatMap.FrequencyZeroFilterMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatMap.TermArticleMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.reducor.DocLengthSumReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencyAccumulator;




/**
 * This is the main class where your Spark topology should be specified.
 *
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {


	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[6]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();


		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}


	}



	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		
		CollectionAccumulator<String> allQueryTerms = spark.sparkContext().collectionAccumulator();
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(allQueryTerms), Encoders.bean(Query.class)); // this converts each row into a Query
		queries.show();
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		//convert query into Set, remove duplicated terms,and finally convert back to List
		System.out.println("we are converting query into query List");
		Set<String> allQueryTermsToSet = new HashSet<>();   //delete duplicate element
		allQueryTermsToSet.addAll(allQueryTerms.value());
		List<String> allQueryTermsToList = new ArrayList<String>(allQueryTermsToSet);

		
		//convert article into cleanedArticle
		Encoder<NewsArticlesCleaned> newsArticleEncoder = Encoders.bean(NewsArticlesCleaned.class);
		Dataset<NewsArticlesCleaned> articles = news.map(new NewsProcessorMap(), newsArticleEncoder);

		//get averageDocumentLengthInCorpus
		Long totalDocsInCorpus = articles.count();
		System.out.println( totalDocsInCorpus);
		Dataset<Long> docLength = articles.map(new DocLengthMap(), Encoders.LONG());
		Long docLengthSUM = docLength.reduce(new DocLengthSumReducer());
		double averageDocumentLengthInCorpus = docLengthSUM / totalDocsInCorpus;
		System.out.println(averageDocumentLengthInCorpus);


		
		//Define an accumulator to calculate the total term and frequency
		 TermFrequencyAccumulator termFrequencyAccumulator = new TermFrequencyAccumulator(new HashMap<>());
		 spark.sparkContext().register(termFrequencyAccumulator, "termFrequencyAccumulator");
		

		//Broadcast allQueryTermsToList
		System.out.println("we are doing some broadcasting");
		Broadcast<List<String>> broadcastAllQueryTermsToList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(allQueryTermsToList);

		
		//group of broadcast

		Broadcast<Long> broadcastTotalDocsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		Broadcast<Double> broadcastAverageDocumentLengthInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);

		
		//term-article-map
		System.out.println("we are maping new to termArticle");
		Encoder<TermArticle> termArticleEncoder= Encoders.bean(TermArticle.class);
		Dataset<TermArticle> termArtcles = articles.flatMap(new TermArticleMap(broadcastAllQueryTermsToList), termArticleEncoder);
		System.out.println("termArticle numbers before zero frquency filtering :" + termArtcles.count());
	
		//zero frequency filter
		System.out.println("we are filtering termArticle that has zero frequency");
		FrequencyZeroFilterMap frquencyZeroFilter = new FrequencyZeroFilterMap(termFrequencyAccumulator); 
		Dataset<TermArticle> FilteredtermArtcles = termArtcles.flatMap(frquencyZeroFilter,termArticleEncoder);
		System.out.println("TermArticle numbers after filering:" + FilteredtermArtcles.count());
	

		//broadcast Term and Frequency map
		Map<String, Integer> termFrequencyMap = termFrequencyAccumulator.value();
		System.out.println("1111111111111111111111111");
		System.out.println(termFrequencyMap);

		Broadcast<Map<String, Integer>> broadcastTermFrequencyMap = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termFrequencyMap);
		
		///DPH term-Article
		System.out.println("We are calculating DPH score");
		Encoder<TermArticleDPH> dphEncoder = Encoders.bean(TermArticleDPH.class);
		Dataset<TermArticleDPH> termArticleDPH = FilteredtermArtcles.map(new DPHcalculatorMap(broadcastTermFrequencyMap,broadcastTotalDocsInCorpus,
												broadcastAverageDocumentLengthInCorpus), dphEncoder);	
		
 		
		System.out.println("We are combining the term into query and we are ranking!!!!!!");
		List<TermArticleDPH> termarticledphlist = new ArrayList<>();
		Broadcast<List<TermArticleDPH>> termdocdphlist = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termarticledphlist);
		termArticleDPH.foreach(each ->{
			termdocdphlist.getValue().add(each);
		});
		
		
		//
		DocRankMap docrankmap = new DocRankMap(termdocdphlist);
		Dataset<DocumentRanking> docrank = queries.map(docrankmap, Encoders.bean(DocumentRanking.class)); 

		
		//
		System.out.println("We are going to rank the final result");
		Dataset<DocumentRanking> finalDocRank = docrank.map(new FinalResultMap(), Encoders.bean(DocumentRanking.class));

		List<DocumentRanking> finalDocRankList = finalDocRank.collectAsList();
		
		return finalDocRankList ; // replace this with the the list of DocumentRanking output by your topology
	}


}



