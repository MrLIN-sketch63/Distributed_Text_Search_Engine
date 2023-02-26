package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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
import uk.ac.gla.dcs.bigdata.studentstructures.DPHall;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultList;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;



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
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors

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
//		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/test.json";

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
		Set<String> allQueryTermsToSet = new HashSet<>();   //delete duplicate element
		allQueryTermsToSet.addAll(allQueryTerms.value());
		List<String> allQueryTermsToList = new ArrayList<String>(allQueryTermsToSet);
//		System.out.println(allQueryTermsToList);
		
		
		//doc to Term frequency accumulator
		CollectionAccumulator<DocTermFrequency> docTermFrequency = spark.sparkContext().collectionAccumulator();

		
		//convert article into cleanedarticle
		Encoder<NewsArticlesCleaned> newsArticleEncoder = Encoders.bean(NewsArticlesCleaned.class);
		Dataset<NewsArticlesCleaned> articles = news.map(new NewsProcessorMap(docTermFrequency), newsArticleEncoder);

		//word count
		Long totalDocsInCorpus = articles.count();

		Dataset<Long> docLength = articles.map(new DocLengthMap(), Encoders.LONG());
		Long docLengthSUM = docLength.reduce(new DocLengthSumReducer());
		double averageDocumentLengthInCorpus = docLengthSUM / totalDocsInCorpus;
//		System.out.println(averageDocumentLengthInCorpus);
//
//		System.out.println("111111111111111111111111111111111111111");
//		System.out.println(docTermFrequency.value().get(100).getId());
//		System.out.println(docTermFrequency.value().get(100).getTerm());
//		System.out.println(docTermFrequency.value().get(100).getFrequency());

		Dataset<DocTermFrequency> DocTermFrequencyDataset = spark.createDataset(docTermFrequency.value(), Encoders.bean(DocTermFrequency.class));

		DocToTerm keyFunction = new DocToTerm();
		KeyValueGroupedDataset<String, DocTermFrequency> DocByTerm = DocTermFrequencyDataset.groupByKey(keyFunction, Encoders.STRING());

		SumFrequency totalFrequency = new SumFrequency();

		Encoder<Tuple2<String,Long>> termFrequencyEncoder = Encoders.tuple(Encoders.STRING(), Encoders.LONG());
		Dataset<Tuple2<String,Long>> termAndFrequency = DocByTerm.mapGroups(totalFrequency, termFrequencyEncoder);
		termAndFrequency.show();
		//System.out.println(termAndFrequency);
		
//		//QueryTerm - Document
//		Broadcast<Dataset<NewsArticlesCleaned>> broadcastCleanedNews = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(articles);
		

	
			
	
		//broadcastAlQueryTermsToSet
		Broadcast<List<String>> broadcastAllQueryTermsToList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(allQueryTermsToList);
		
		
		//group of broadcast
		List <DocTermFrequency> DocTermFrequencyDatasetList =  DocTermFrequencyDataset.collectAsList();
		Broadcast<List<DocTermFrequency>> broadcastDocTermFrequencyDataset = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(DocTermFrequencyDatasetList);
		List <Tuple2<String, Long>> TermAndFrequencyList = termAndFrequency.collectAsList();
		Broadcast<List<Tuple2<String, Long>>> broadcastTermAndFrequency = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(TermAndFrequencyList);
		Broadcast<Long> broadcastTotalDocsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		Broadcast<Double> broadcastAverageDocumentLengthInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		

		
		//test query
//		List<String> termsList = new ArrayList<String>() ;
//		termsList.add("boykin");
//		termsList.add("green");
//		termsList.add("big");
//		System.out.println(termsList);
//		Broadcast<List<String>> broadcastTermsList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termsList);
		
		
		//map news to termArticle
//		Encoder<TermArticle> termArticleEncoder= Encoders.bean(TermArticle.class);
//		Dataset<TermArticle> termArtcles = news.flatMap(new TermArticleMap(broadcastTermsList), termArticleEncoder);
//		System.out.println("termArticle:" + termArtcles.count());
		Encoder<TermArticle> termArticleEncoder= Encoders.bean(TermArticle.class);
		Dataset<TermArticle> termArtcles = news.flatMap(new TermArticleMap(broadcastAllQueryTermsToList), termArticleEncoder);
		System.out.println("termArticle:" + termArtcles.count());
		termArtcles.printSchema();
		//has dphscore
	
//	
		//zero frequency filter
		FrequencyZeroFilterMap frquencyZeroFilter = new FrequencyZeroFilterMap(broadcastDocTermFrequencyDataset); 
		Dataset<TermArticle> FilteredtermArtcles = termArtcles.flatMap(frquencyZeroFilter,termArticleEncoder);
		System.out.println("TermArticle after filering:" + FilteredtermArtcles.count());
		FilteredtermArtcles.printSchema();
		
		///DPH
		System.out.println("We are calculating DPH score");
		Encoder<DPHall> dphEncoder = Encoders.bean(DPHall.class);

		Dataset<DPHall> DPH = FilteredtermArtcles.map(new DPHcalculatorMap(broadcastTermAndFrequency,broadcastTotalDocsInCorpus,
												broadcastAverageDocumentLengthInCorpus, broadcastDocTermFrequencyDataset), dphEncoder);	
		
		DPH.printSchema();
		DPH.foreach(dphall -> {
		    // Do something with each DPHall object
		    System.out.println(dphall.getDPHscore());
		});
		
		
		TermArticleDPHMap f = new TermArticleDPHMap();
		Dataset<TermArticleDPH> termarticledph = DPH.map(f, Encoders.bean(TermArticleDPH.class));
		termarticledph.printSchema();
//		List<TermArticleDPH> termarticledphl = termarticledph.collectAsList();
//		System.out.println(termarticledphl.size());
		System.out.println(termarticledph.count());
		
		
//		termarticledph.collectAsList();
//		Broadcast<Dataset<TermArticleDPH>> dphall = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termarticledph);
//
//		
		
//		DocRankMap docrankmap = new DocRankMap(dphall);
//		Dataset<DocumentRanking> docrank = queries.map(docrankmap, Encoders.bean(DocumentRanking.class)); 
//		
//		Dataset
//		
//		System.out.println(docrank.count());
		
		List<TermArticleDPH> termarticledphlist = new ArrayList<>();
		
		
		Broadcast<List<TermArticleDPH>> termdocdphlist = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termarticledphlist);

		termarticledph.foreach(each ->{
			System.out.println(each.getDphscore());
			termdocdphlist.getValue().add(each);
			System.out.println(termdocdphlist.getValue().size());
		});
		System.out.println(termdocdphlist.getValue().size());
		
		DocRankMap docrankmap = new DocRankMap(termdocdphlist);
		Dataset<DocumentRanking> docrank = queries.map(docrankmap, Encoders.bean(DocumentRanking.class)); 
		
		System.out.println(docrank.count());
		
		
		
		
		
		
		
		
		//reduce
//		Encoder<RankedResultList> rankedResultListtEncoder = Encoders.bean(RankedResultList.class);
//		Dataset<RankedResultList> AsLists =  result.map(new RankedResultToListMap, rankedResultListtEncoder);//result是最后出现的10个rankedresult
//		
//		RankedResultList finalAsLists = AsLists.reduce(new TitleReducer());

		return null; // replace this with the the list of DocumentRanking output by your topology
	}


}



