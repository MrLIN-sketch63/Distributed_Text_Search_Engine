package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;


public class FinalResultMap implements MapFunction<DocumentRanking , DocumentRanking >{

	/**
	 * 
	 */
	private static final long serialVersionUID = 100002131213L;

	@Override
	public DocumentRanking call(DocumentRanking value) throws Exception {
		// TODO Auto-generated method stub
		
		List<RankedResult> finalRankedResultList = new ArrayList<RankedResult>(10);				
		List<RankedResult> rankedResultList = value.getResults();
		Query query = value.getQuery();
		
		
		for(RankedResult rankedResult: rankedResultList) {
			if(finalRankedResultList.size()==0) {
				finalRankedResultList.add(rankedResult);
				continue;
			}
			
			NewsArticle article = rankedResult.getArticle();
			String title = article.getTitle();
			boolean flag = true;//True:keep this result, vice versa.
			
			if(!finalRankedResultList.contains(rankedResult)) {
				for(RankedResult finalRankedResult:finalRankedResultList) {
					NewsArticle finalArticle = finalRankedResult.getArticle();
					String finalTitle = finalArticle.getTitle();
					if(finalTitle!=null && title!=null) {
						double distance = TextDistanceCalculator.similarity(finalTitle, title);
						if(distance<0.5) {
							flag = false;
							break;}}
					else {flag = false;}
				}
				
			}
			
			if(flag) {
				if(finalRankedResultList.size()<10) {
					finalRankedResultList.add(rankedResult);
				}
			}		
			
		}
		
		DocumentRanking finalDocumentRanking =  new DocumentRanking(query,finalRankedResultList);
		return finalDocumentRanking;
	}

}
