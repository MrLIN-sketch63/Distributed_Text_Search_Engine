package uk.ac.gla.dcs.bigdata.studentfunctions;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;



/**Qixiang Mo**/
public class NewsProcessorMap implements MapFunction<NewsArticle, NewsArticlesCleaned> {
	private static final long serialVersionUID = -4631167868446468000L;

	private transient TextPreProcessor newsProcessor;


	public NewsProcessorMap() {
		
	}



	@Override
	public NewsArticlesCleaned call(NewsArticle value) throws Exception {
		// TODO Auto-generated method stub
	
		List<String> title = new ArrayList<String>();
		List<String> terms = new ArrayList<String>();
		Long doc_length = (long) 0;

		if (newsProcessor==null) newsProcessor = new TextPreProcessor();


		//first using this docid
		String newsID = value.getId();
		String newsTitle = value.getTitle();
		List<ContentItem> newsContentItems = value.getContents();
		String newsParagraph = "";


		int i = 0;
		
		for(ContentItem newsContentItem : newsContentItems) {
			if(newsContentItem!=null) { 
				String subType = newsContentItem.getSubtype();

			if( subType!= null) {
				if (subType.equals("paragraph")){
					if(!newsContentItem.getContent().equals(null) || !newsContentItem.getContent().equals("")){
						newsParagraph = newsParagraph + newsContentItem.getContent();
						i++;}//if the paragraph is null or blank then skip it to the next paragraph
				}

				if(i==5) {
					break;}
			}
			}
		}


		if(newsParagraph!=null) {
			terms.addAll(newsProcessor.process(newsParagraph));//terms are paragraphs that have undergone text preprocessing
			doc_length += terms.size();}

		if(newsTitle != null) {
			title.addAll(newsProcessor.process(newsTitle));//title is the title after text preprocessing
			doc_length += title.size();}
		
		NewsArticlesCleaned article =  new NewsArticlesCleaned(newsID, title, terms, doc_length, value);;
		
		

		return article;
	}

}

