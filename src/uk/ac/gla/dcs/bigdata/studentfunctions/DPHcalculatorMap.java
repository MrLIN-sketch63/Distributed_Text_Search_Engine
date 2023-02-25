package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.util.CollectionAccumulator;

import scala.Tuple2;
import scala.collection.JavaConverters;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHall;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultList;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;

public class DPHcalculatorMap implements MapFunction<TermArticle, DPHall>{
	
	private static final long serialVersionUID = -4631167868446469099L;

//	private transient DPHScorer scorer;

	
	//Global Data
	Broadcast<List<Tuple2<String, Long>>> broadcastTermAndFrequency;
	Broadcast<Long> broadcastTotalDocsInCorpus;
	Broadcast<Double> broadcastAverageDocumentLengthInCorpus;
	Broadcast<List<DocTermFrequency>> broadcastDocTermFrequencyDataset;
	
	
	
	public DPHcalculatorMap(Broadcast<List<Tuple2<String, Long>>> broadcastTermAndFrequency,
			Broadcast<Long> broadcastTotalDocsInCorpus, Broadcast<Double> broadcastAverageDocumentLengthInCorpus, 
			Broadcast<List<DocTermFrequency>> broadcastDocTermFrequencyDataset) {
		super();
		this.broadcastTermAndFrequency = broadcastTermAndFrequency;
		this.broadcastTotalDocsInCorpus = broadcastTotalDocsInCorpus;
		this.broadcastAverageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus;
		this.broadcastDocTermFrequencyDataset = broadcastDocTermFrequencyDataset;
	}



	@Override
	public DPHall call(TermArticle value) throws Exception {
		
//		if (scorer==null) new DPHScorer();
//		
		
		
		short termFrequencyInCurrentDocument = 0;
		int totalTermFrequencyInCorpus = 0;
		int currentDocumentLength = 0;
		double averageDocumentLengthInCorpus = 0;//所有文档的平均长度
		long totalDocsInCorpus = 0;//一共有多少个文件在数据集中
		String term = "";
		String newsID = "";
		NewsArticle article = new NewsArticle();
		
		term = value.getTerm();
		article = value.getArticle();
		newsID = article.getId();
//		System.out.println(term);	
		List<DocTermFrequency> docTermFrequencyList = this.broadcastDocTermFrequencyDataset.value();
		for(DocTermFrequency docTerm:docTermFrequencyList) {
			if(docTerm.getTerm().equals(term) && docTerm.getId().equals(newsID)){
				termFrequencyInCurrentDocument = docTerm.getFrequency();
				currentDocumentLength = docTerm.getDoc_length();
//				System.out.println(termFrequencyInCurrentDocument+currentDocumentLength);
			}
		}
		
		//
		List<Tuple2<String,Long>> termAndFrequencyList = broadcastTermAndFrequency.value();
		Iterator<Tuple2<String, Long>> tupleIterator = termAndFrequencyList.iterator();
		
		while (tupleIterator.hasNext()) {
			Tuple2<String,Long> tuple = tupleIterator.next();
			if(tuple._1.equals(term)) {
			totalTermFrequencyInCorpus = tuple._2.intValue();
			}
		}
		
		
		//
//		currentDocumentLength = value.getDoc_length();
		
		//
		averageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus.value();
//		System.out.println(averageDocumentLengthInCorpus);
		//
		totalDocsInCorpus = broadcastTotalDocsInCorpus.value();
//		System.out.println(totalDocsInCorpus);
//		System.out.println(termFrequencyInCurrentDocument+" "+totalTermFrequencyInCorpus+" "+
//				currentDocumentLength+" "+averageDocumentLengthInCorpus+" "+totalDocsInCorpus);
		double DPHsocre= DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
//		System.out.println(DPHsocre);
		//
//		Dataset<NewsArticle> news = broadcastNews.value();
//		List<NewsArticle> newsList = news.collectAsList();
//		
//		for(NewsArticle newsItem: newsList) {
//			if (newsItem.getId().equals(newsID)){
//				article = newsItem;
//			}
//		}
		
		DPHall allResults = new DPHall(DPHsocre, term, article);
//		System.out.println(allResults.getDPHscore());
		return allResults;
	}



}
