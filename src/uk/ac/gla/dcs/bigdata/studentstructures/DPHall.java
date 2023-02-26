package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class DPHall implements Serializable{
	
	private static final long serialVersionUID = 7309797024926066989L;
	
	double DPHscore = 0;
	String terms;
	NewsArticle article;
	
	
	
	
	public DPHall(double DPHscore, String terms, NewsArticle article) {
		super();
		this.DPHscore = DPHscore;
		this.terms = terms;
		this.article = article;
	}
	
	public DPHall() {}

	
	@Override
	public String toString() {
		return "DPHall [DPHscore=" + DPHscore + ", terms=" + terms + ", article=" + article + "]";
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