package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class DPHall<String ,newsArticles> implements Serializable{
	
	private static final long serialVersionUID = 7309797024926062989L;
	
	double DPHsocre;
	String terms;
	newsArticles article;
	
	public DPHall(double dPHsocre, String terms, newsArticles article) {
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
	public newsArticles getArticle() {
		return article;
	}
	public void setArticle(newsArticles article) {
		this.article = article;
	}

	
}