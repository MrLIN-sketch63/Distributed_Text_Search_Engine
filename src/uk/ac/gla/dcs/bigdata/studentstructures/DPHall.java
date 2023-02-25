package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class DPHall implements Serializable{
	
	private static final long serialVersionUID = 7309797024926062989L;
	
	double DPHscore;
	String terms;
	NewsArticle article;
	
	
	public DPHall() {}
	
	
	public DPHall(double dPHscore, String terms, NewsArticle  article) {
		super();
		this.DPHscore = dPHscore;
		this.terms = terms;
		this.article = article;
	}
	public double getDPHscore() {
		return DPHscore;
	}
	public void setDPHsocre(double dPHscore) {
		DPHscore = dPHscore;
	}
	public String getTerms() {
		return terms;
	}
	public void setTerms(String terms) {
		this.terms = terms;
	}
	public NewsArticle getArticle() {
		return article;
	}
	public void setArticle(NewsArticle  article) {
		this.article = article;
	}

	
}