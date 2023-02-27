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


//在此代码中，NewsFormaterMap 是一个 MapFunction，它将 Row 作为输入并返回 NewsArticle 对象。 call() 方法
//使用 mkString() 方法将输入行读取为 JSON 字符串，然后使用 Jackson ObjectMapper 将 JSON 字符串反序列化为 NewsArticle 对象。 
//transient 关键字用于指示 jsonMapper 不应与对象一起序列化，因为它可以在需要时重新创建
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
		//将json文本转化成java的NewsArticle这个class,
		//mkString(seq:String)方法是将原字符串使用特定的字符串seq分割,如果值为 [1, 2, 3]，结果将是字符串“123”。
		
		
		return article;
	}
		
		
	
}
