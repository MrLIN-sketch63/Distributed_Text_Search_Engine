package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TermArticle implements Serializable{
	private static final long serialVersionUID = 7860296794078492249L;
	
	
	String term;
	NewsArticle article;
	double DPHscore = 0;
	
	
	public double getDPHscore() {
		return DPHscore;
	}

	public void setDPHscore(double dPHscore) {
		DPHscore = dPHscore;
	}

	public TermArticle() {
	}
	
	public TermArticle(String term, NewsArticle article, double DPHscore) {
		super();
		this.term = term;
		this.article = article;
		this.DPHscore = DPHscore;
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
	
	
	
	
}

