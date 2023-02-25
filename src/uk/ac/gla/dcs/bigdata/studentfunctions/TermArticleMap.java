package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.Query;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;



public class TermArticleMap implements FlatMapFunction<String,TermArticle>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 100L;
	//Global data
//	Broadcast<Dataset<NewsArticlesCleaned>> broadcastNews;
	
	Dataset<String> termsList;
	Broadcast<Dataset<NewsArticlesCleaned>> broadcastCleanedNews ;
	
	public  TermArticleMap(Dataset<String> termsList,Broadcast<Dataset<NewsArticlesCleaned>> broadcastCleanedNews ) {
		this.broadcastCleanedNews = broadcastCleanedNews;
		this.termsList = termsList;
	}

	

	@Override
	public Iterator<TermArticle> call(String value) throws Exception {
		// TODO Auto-generated method stub
		List<TermArticle> termArticleList =  new ArrayList<TermArticle>();
		List<NewsArticlesCleaned> newsList = new ArrayList<NewsArticlesCleaned>();
		
		Dataset<NewsArticlesCleaned> cleanedNews = broadcastCleanedNews.value();
		newsList = cleanedNews.collectAsList();
		Iterator<NewsArticlesCleaned> newsIterator =  newsList.iterator();
		
		while(newsIterator.hasNext()){
			if(newsIterator.next()!=null)
				termArticleList.add(new TermArticle(value, newsIterator.next()));		
			}
		
		return termArticleList.iterator();
		
	
	}
	
}
