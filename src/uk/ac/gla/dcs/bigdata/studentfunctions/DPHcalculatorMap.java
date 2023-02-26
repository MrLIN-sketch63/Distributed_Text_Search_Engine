package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import java.util.List;

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
		article = value.getArticle();
		newsID = article.getId();

		List<DocTermFrequency> docTermFrequencyList = this.broadcastDocTermFrequencyDataset.value();
		for(DocTermFrequency docTerm:docTermFrequencyList) {
			if(docTerm.getTerm().equals(term) && docTerm.getId().equals(newsID)){
				termFrequencyInCurrentDocument = docTerm.getFrequency();
				currentDocumentLength = docTerm.getDoc_length();

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
		averageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus.value();

		//
		totalDocsInCorpus = broadcastTotalDocsInCorpus.value();

		double DPHsocre= DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
		
		TermArticleDPH allResults = new  TermArticleDPH (DPHsocre, term, article);

		return allResults;
	}



}
