package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleanedList;

public class NewsArticleToListMap implements MapFunction<NewsArticlesCleaned, NewsArticlesCleanedList>{
	private static final long serialVersionUID = 1L;
	@Override
	public NewsArticlesCleanedList call(NewsArticlesCleaned article) throws Exception {
		List<NewsArticlesCleaned> asList = new ArrayList<NewsArticlesCleaned>(1);
		asList.add(article);
		return new NewsArticlesCleanedList(asList);
		
	}
	
	

}
