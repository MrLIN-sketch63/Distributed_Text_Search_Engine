package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.util.CollectionAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHall;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;


public class DPHScorerForTerm implements MapFunction<NewsArticlesCleaned, DPHall> {
	
	
	Broadcast<Dataset<Tuple2<String, Long>>> broadcastTermAndFrequency;
	Broadcast<Long> broadcastTotalDocsInCorpus;
	CollectionAccumulator<DocTermFrequency> docTermFrequency;
	Broadcast<Double> broadcastAverageDocumentLengthInCorpus;
	
	
	public DPHScorerForTerm(Broadcast<Dataset<Tuple2<String, Long>>> broadcastTermAndFrequency, Broadcast<Long> broadcastTotalDocsInCorpus, Broadcast<Double> broadcastAverageDocumentLengthInCorpus, CollectionAccumulator<DocTermFrequency> docTermFrequency) {
		this.broadcastTermAndFrequency = broadcastTermAndFrequency;
		this.broadcastTotalDocsInCorpus = broadcastTotalDocsInCorpus;
		this.docTermFrequency = docTermFrequency;
		this.broadcastAverageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus;
	}
	
	
	@Override
	public DPHall call(NewsArticlesCleaned value) throws Exception {
		
		List<Tuple2<String, Double>> termdphpair;
		List<String> allterms = value.getTitleAndParagraphTerms();
		HashMap<String, Long> termfreqmap = value.getWordMap();
		
		for(String term: allterms) {
			//TODO
		}
		
		
		DPHall resdphall = new DPHall(termdphpair, value);
		
		return resdphall;
		
	}

}
