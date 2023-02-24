package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TermArticle implements Serializable{
	private static final long serialVersionUID = 7860296794078492249L;
	
	
	String term;
	NewsArticlesCleaned article;
	
	public TermArticle(String term, NewsArticlesCleaned article) {
		super();
		this.term = term;
		this.article = article;
	}
	

	public String getTerm() {
		return term;
	}
	public void setTerm(String term) {
		this.term = term;
	}
	public NewsArticlesCleaned getArticle() {
		return article;
	}
	public void setArticle(NewsArticlesCleaned article) {
		this.article = article;
	}
	
	
}

