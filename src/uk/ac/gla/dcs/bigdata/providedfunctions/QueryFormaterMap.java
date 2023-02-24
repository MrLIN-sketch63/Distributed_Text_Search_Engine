package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;


import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;



//这个formetermap接受了一个spark的row 和我们已经生成的query数据结构
public class QueryFormaterMap implements MapFunction<Row,Query> {

	private static final long serialVersionUID = 6475166483071609772L;

	private transient TextPreProcessor processor;
	
	@Override
	public Query call(Row value) throws Exception {
	
		if (processor==null) processor = new TextPreProcessor();//文本预处理软件，处理成想要的样子
		
		String originalQuery = value.mkString();
		System.out.println(originalQuery);
		
		List<String> queryTerms = processor.process(originalQuery);//queryTerms是经过了文本预处理的文本
		System.out.println(queryTerms);
		
		String test = String.join(" ", queryTerms);
		System.out.println(test);
		
		short[] queryTermCounts = new short[queryTerms.size()];
		for (int i =0; i<queryTerms.size(); i++) queryTermCounts[i] = (short)1;//short)1 用于表示数组 queryTermCounts 中每个查询词的频率计数。
		
		Query query = new Query(originalQuery, queryTerms, queryTermCounts);
		
		return query;
	}
	
	
	  public static void main(String[] args) throws Exception {
	        
	        // create an instance of the QueryFormaterMap class
	        QueryFormaterMap queryFormater = new QueryFormaterMap();
	        
	        // define some sample query text to format
	        Row sampleQuery = RowFactory.create("The quick brown fox jumps over the lazy dog");
	        
	        // format the sample query using the QueryFormaterMap class
	        Query formattedQuery = queryFormater.call(sampleQuery);
	        
	        // print the formatted query and its properties
	        System.out.println("Formatted Query: " + formattedQuery.getOriginalQuery());//Formatted Query: The quick brown fox jumps over the lazy dog
	        System.out.println("Query Terms: " + formattedQuery.getQueryTerms());//Query Terms: [quick, brown, fox, jump, lazi, dog]
	        System.out.println("Query Term Counts: " + Arrays.toString(formattedQuery.getQueryTermCounts()));//Query Term Counts: [1, 1, 1, 1, 1, 1]
	        
	    }

}
