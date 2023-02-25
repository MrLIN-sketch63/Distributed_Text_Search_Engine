package uk.ac.gla.dcs.bigdata.studentfunctions.flatMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.Query;

import org.apache.commons.net.nntp.Article;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticle;



public class TermArticleMap implements FlatMapFunction<NewsArticle,TermArticle>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 100L;
	//Global data
//	Broadcast<Dataset<NewsArticlesCleaned>> broadcastNews;
	
	//Broadcast<List<String>> broadcastTermsList;
	Broadcast<List<String>> broadcastAlQueryTermsToList;
	
	public  TermArticleMap(Broadcast<List<String>> broadcastAlQueryTermsToList) {
		this.broadcastAlQueryTermsToList = broadcastAlQueryTermsToList;
	}

	@Override
	public Iterator<TermArticle> call(NewsArticle article) throws Exception {
		List<TermArticle> termArticle = new ArrayList<>();
		
		List<String> termsList = broadcastAlQueryTermsToList.value();
		
		for (String term : termsList) {
			termArticle.add(new TermArticle(term, article));
        }
		return termArticle.iterator();
	}
	
	



	
}
