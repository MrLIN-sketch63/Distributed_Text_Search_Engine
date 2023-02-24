package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class DPHall implements Serializable{
	
	private static final long serialVersionUID = 7309797024926062989L;
	
	double DPHsocre;
	String terms;
	NewsArticle article;
	
	public DPHall(double dPHsocre, String terms, NewsArticle  article) {
		super();
		DPHsocre = dPHsocre;
		this.terms = terms;
		this.article = article;
	}
	public double getDPHsocre() {
		return DPHsocre;
	}
	public void setDPHsocre(double dPHsocre) {
		DPHsocre = dPHsocre;
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