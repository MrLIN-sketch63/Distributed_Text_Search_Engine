package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class QueryArticleDPH implements Serializable{
	private static final long serialVersionUID = 7865879794078492249L;

	
	Query query;
	NewsArticle article;
	double DPH = 0;
	
	public QueryArticleDPH() {
	}

	public QueryArticleDPH(Query query, NewsArticle article, double dPH) {
		super();
		this.query = query;
		this.article = article;
		DPH = dPH;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public double getDPH() {
		return DPH;
	}

	public void setDPH(double dPH) {
		DPH = dPH;
	}
	
}
