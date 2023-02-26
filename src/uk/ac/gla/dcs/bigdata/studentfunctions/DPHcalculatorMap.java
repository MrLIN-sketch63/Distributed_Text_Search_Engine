package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;

public class DPHcalculatorMap implements MapFunction<TermArticle,  TermArticleDPH >{
	
	private static final long serialVersionUID = -4631167868446469099L;
	
	//Global Data
	Broadcast<Map<String, Integer>> broadcastTermFrequencyMap;
	Broadcast<Long> broadcastTotalDocsInCorpus;
	Broadcast<Double> broadcastAverageDocumentLengthInCorpus;
	

		
	public DPHcalculatorMap(Broadcast<Map<String, Integer>> broadcastTermFrequencyMap,
			Broadcast<Long> broadcastTotalDocsInCorpus, Broadcast<Double> broadcastAverageDocumentLengthInCorpus) {
		super();
		this.broadcastTermFrequencyMap = broadcastTermFrequencyMap;
		this.broadcastTotalDocsInCorpus = broadcastTotalDocsInCorpus;
		this.broadcastAverageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus;
	}


	@Override
	public  TermArticleDPH  call(TermArticle value) throws Exception {
		
			
		short termFrequencyInCurrentDocument = 0;
		int totalTermFrequencyInCorpus = 0;
		int currentDocumentLength = 0;
		double averageDocumentLengthInCorpus = 0;//所有文档的平均长度
		long totalDocsInCorpus = 0;//一共有多少个文件在数据集中
		String term = "";
		String newsID = "";
		NewsArticle article = new NewsArticle();
		
		term = value.getTerm();
		article = value.getArticle().getOriginalArticle();
		newsID = article.getId();

		//termFrequencyInCurrentDocument
		termFrequencyInCurrentDocument = value.getFrequency();
		
		//otalTermFrequencyInCorpus
		Map<String, Integer> termAndFrequencyMap = broadcastTermFrequencyMap.value();
		if(termAndFrequencyMap.get(term)!=null) totalTermFrequencyInCorpus = termAndFrequencyMap.get(term);
		
		//averageDocumentLengthInCorpus
		averageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus.value();

		//totalDocsInCorpus
		totalDocsInCorpus = broadcastTotalDocsInCorpus.value();
		
		//currentDocumentLength
		currentDocumentLength = value.getArticle().getDoc_length().intValue();
		

		double DPHsocre= DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
		
		TermArticleDPH allResults = new TermArticleDPH (DPHsocre, term, article);

		return allResults;
	}



}
