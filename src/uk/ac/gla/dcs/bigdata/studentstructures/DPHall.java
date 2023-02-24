package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import scala.Tuple2;

public class DPHall<newsArticles> implements Serializable{
	
	private static final long serialVersionUID = 7309797024926062989L;
	
	List<Tuple2<String, Double>> termdphpair;
	newsArticles article;
	
	public DPHall(List<Tuple2<String, Double>> termdphpair, newsArticles article) {
		super();
		
		this.termdphpair = termdphpair;
		this.article = article;
		
	}

	public List<Tuple2<String, Double>> getTermdphpair() {
		return termdphpair;
	}

	public void setTermdphpair(List<Tuple2<String, Double>> termdphpair) {
		this.termdphpair = termdphpair;
	}

	public newsArticles getArticle() {
		return article;
	}

	public void setArticle(newsArticles article) {
		this.article = article;
	}
	
	
	

	
}