package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TermArticleDPH implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7550296097101248763L;
	
	
	String term;
	NewsArticle article;
	double dphscore;
	
	
	public TermArticleDPH(String term, NewsArticle article, double dphscore) {
		this.term = term;
		this.dphscore = dphscore;
		this.article = article;
	}


	public String getTerm() {
		return term;
	}


	public void setTerm(String term) {
		this.term = term;
	}


	public NewsArticle getArticle() {
		return article;
	}


	public void setArticle(NewsArticle article) {
		this.article = article;
	}


	public double getDphscore() {
		return dphscore;
	}


	public void setDphscore(double dphscore) {
		this.dphscore = dphscore;
	}
	
	
	
	

}
