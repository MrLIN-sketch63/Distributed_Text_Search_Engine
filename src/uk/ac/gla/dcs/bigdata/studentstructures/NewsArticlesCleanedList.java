package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;


public class NewsArticlesCleanedList implements Serializable{
	private static final long serialVersionUID = -5066661179554881889L;
	List<NewsArticlesCleaned> newsArticlesCleanedList;

	public NewsArticlesCleanedList() {}
	
	public NewsArticlesCleanedList(List<NewsArticlesCleaned> articleList) {
		super();
		this.newsArticlesCleanedList = articleList;
	}

	
	public List<NewsArticlesCleaned> getArticleList() {
		return newsArticlesCleanedList;
	}

	
	public void setArticleList(List<NewsArticlesCleaned> gameList) {
		this.newsArticlesCleanedList = gameList;
	}
	
	
	
}
