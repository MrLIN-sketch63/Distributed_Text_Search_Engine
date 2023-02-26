package uk.ac.gla.dcs.bigdata.studentfunctions.reducor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.nntp.Article;
import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleanedList;
import uk.ac.gla.dcs.bigdata.studentstructures.List.RankedResultList;



public class TitleReducer implements ReduceFunction<RankedResultList> {

	private static final long serialVersionUID = -2234797120289678538L;

	private transient TextDistanceCalculator textDistancecalculator;
	@Override
	public RankedResultList call(RankedResultList v1, RankedResultList v2) throws Exception {
		
		if (textDistancecalculator==null) textDistancecalculator = new TextDistanceCalculator();
		// Use a fast method to find String matches using hash map
		Map<String,RankedResult> titleMap = new HashMap<String,RankedResult>();
		List<RankedResult> result = new ArrayList<RankedResult>(10);
		
		for (RankedResult resultV1 : v1.getRankedResultLis()) {
			NewsArticle articleV1 =resultV1.getArticle();
			String titleV1 = articleV1.getTitle();
			
			if(titleV1 == null) {
				continue;
			}
			
			for (RankedResult resultV2 : v2.getRankedResultLis()) {
				NewsArticle articleV2 =resultV2.getArticle();
				String titleV2 = articleV2.getTitle();
			
				if(titleV2!=null) {
					double similarity = textDistancecalculator.similarity(titleV1, titleV2);
					if(similarity <0.5) {
						if(resultV1.compareTo(resultV2)>=0) {
							result.add(resultV1);
						}
						else {
							result.add(resultV2);
						}
					}
					else {
						result.add(resultV1);
						result.add(resultV2);
					}
				}
				
				else {
					break;}
			}
				
		}
	
		
		return new RankedResultList(result);
	}



}
