package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Converts a Row containing a String Json news article into a NewsArticle object 
 * @author Richard
 *
 */


public class NewsFormaterMap implements MapFunction<Row,NewsArticle> {

	private static final long serialVersionUID = -4631167868446468097L;

	private transient ObjectMapper jsonMapper;// transient是Java语言的关键字,用来表示一个成员变量不是该对象序列化的一部分。当一个对象被序列化的时候,transient型变量的值不包括在序列化的结果中
	
//	private transient TextPreProcessor newsProcessor;
	
	
	@Override
	public NewsArticle call(Row value) throws Exception {
		
//		/**Qixiang Mo modify**/
//		String originalNews = value.mkString();
//		
//		List<String> newsTerms = newsProcessor.process(originalNews);//queryTerms是经过了文本预处理的文本
//		
//		String content = String.join(" ", newsTerms);	

		if (jsonMapper==null) jsonMapper = new ObjectMapper();		
		
		NewsArticle article = jsonMapper.readValue(value.mkString(), NewsArticle.class);

		
		
		return article;
	}
		
		
	
}
