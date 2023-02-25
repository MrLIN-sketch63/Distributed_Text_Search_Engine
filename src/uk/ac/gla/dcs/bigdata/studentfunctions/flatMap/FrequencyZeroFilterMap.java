package uk.ac.gla.dcs.bigdata.studentfunctions.flatMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;



public class FrequencyZeroFilterMap implements FlatMapFunction<TermArticle,TermArticle>{

	
	private static final long serialVersionUID = -5421918183346003486L;
	
	
	//Global
	Broadcast<List<DocTermFrequency>> broadcastDocTermFrequencyDataset;
	
	//
	boolean frquencyZero;
	
	
	public FrequencyZeroFilterMap(Broadcast<List<DocTermFrequency>> broadcastDocTermFrequencyDataset) {
		this.broadcastDocTermFrequencyDataset = broadcastDocTermFrequencyDataset;
	}

	
	public Iterator<TermArticle> call(TermArticle value) throws Exception {
		
		List<DocTermFrequency> DocTermFrequencyList = broadcastDocTermFrequencyDataset.value();
//		TermArticle TermArticlefiltered = new TermArticle();
		NewsArticle article = value.getArticle();
		String term = value.getTerm();
		String newsID = article.getId();
		frquencyZero = true;
		
		for(DocTermFrequency docTermFrequency:DocTermFrequencyList) {
			short frequency = docTermFrequency.frequencySearch(newsID, term);
			
			if(frequency > 0) {
				this.frquencyZero = false;
			}
		}
		
		
		if (!this.frquencyZero) {
			List<TermArticle> termArticleList = new ArrayList<TermArticle>(1);
			termArticleList.add(value); 
			return termArticleList.iterator(); 
		} else {
			List<TermArticle> termArticleList = new ArrayList<TermArticle>(0);
			return termArticleList.iterator();
		}
	}

}
