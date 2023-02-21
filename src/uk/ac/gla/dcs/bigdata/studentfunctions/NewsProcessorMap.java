package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlesCleaned;


/**Qixiang Mo**/
public class NewsProcessorMap implements MapFunction<NewsArticle,NewsArticlesCleaned> {
	private static final long serialVersionUID = -4631167868446468000L;
	
	private transient TextPreProcessor newsProcessor;

	@Override
	public NewsArticlesCleaned call(NewsArticle value) throws Exception {
		// TODO Auto-generated method stub
		
		
		String newsID = value.getId();
		String newsTitle = value.getTitle();
		List<ContentItem> newsContentItems = value.getContents();
		String newsParagraph = null;
		
		
		int i = 0;
		for(ContentItem newsContentItem : newsContentItems) {
			String subType = newsContentItem.getSubtype();
			
			if(subType=="paragraph"){
				if(newsContentItem.getContent()!= null || newsContentItem.getContent() != ""){
					newsParagraph = newsParagraph + newsContentItem.getContent();
					i++;}//if the paragraph is null or blank then skip it to the next paragraph					
			}
			
			if(i==5) {
				break;
			}
		}
		
		List<String> terms = newsProcessor.process(newsParagraph);//terms是经过了文本预处理的paragraph
		List<String> title = newsProcessor.process(newsTitle);//title是经过了文本预处理的title
		
		
		NewsArticlesCleaned article =  new NewsArticlesCleaned(newsID, title, terms);
		
			
		return article;
	}




}
