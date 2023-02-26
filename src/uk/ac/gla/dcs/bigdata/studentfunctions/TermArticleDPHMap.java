package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.CollectionAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHall;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;

public class TermArticleDPHMap implements MapFunction<DPHall, TermArticleDPH> {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7373873928704569477L;

	@Override
	public TermArticleDPH call(DPHall value) throws Exception{
		
		String term = value.getTerms();
		NewsArticle news = value.getArticle();
		double dph = value.getDPHscore();
		TermArticleDPH res = new TermArticleDPH(term, news, dph);
		return res;
		
	}
}
