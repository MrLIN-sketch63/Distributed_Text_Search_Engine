package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHall;
import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;

public class DocRankMap implements MapFunction<Query, DocumentRanking>{


	/**
	 * 
	 */
	private static final long serialVersionUID = 3490083426550218984L;
	
	
	Broadcast<List<TermArticleDPH>> dphall;
	
	
	public DocRankMap(Broadcast<List<TermArticleDPH>> dphall) {
		this.dphall = dphall;
	}
	

	@Override
	public DocumentRanking call(Query value) throws Exception {
		
		
		
		//System.out.println("print the schema!!!!!!!!!!!!!");
		//System.out.println(dphall.getValue().size());
		
		HashMap<NewsArticle, Double> dphmap = new HashMap<>();
		List<RankedResult> docranks = new ArrayList<>();
		
				
		List<String> queryTerms = value.getQueryTerms();
		double total_score = 0;
		
		
		int term_num = queryTerms.size();

//		for(String qt: queryTerms) {
//			qt = "\"" + qt + "\"";
//			String part_condition = "terms = "+qt;
//			condition = condition + " OR " + part_condition;
//			condition = condition.substring(4);
//		}
//		
//		System.out.println("print the condition!!!!!!!!!!!!!");
//		System.out.println(condition);
		
		for(TermArticleDPH cur: dphall.getValue()) {
			String cur_term = cur.getTerm();
			NewsArticle cur_doc = cur.getArticle();
			if(queryTerms.contains(cur_term)) {
				if(dphmap.containsKey(cur_doc)) {
					dphmap.put(cur_doc, dphmap.get(cur_doc) + cur.getDphscore());
				}else {
					dphmap.put(cur_doc, cur.getDphscore());
				}
			}
		}
		
		List<NewsArticle> collect = dphmap.entrySet().stream().filter(x -> x.getValue() != null).sorted((o1, o2) -> {
            if (o1.getValue() < o2.getValue()) {
                return 1;
            }
            return -1;
        }).map(x -> x.getKey()).collect(Collectors.toList());
		System.out.println("sortsortsort!!!");

		for(NewsArticle doc: collect) {
			String docid = doc.getId();
			NewsArticle article = doc; 
			double score = dphmap.get(doc) / term_num;
			RankedResult rankres = new RankedResult(docid, article, score);
			System.out.println(rankres.getScore());
			docranks.add(rankres);
		}
		System.out.println(docranks);
	
		
		DocumentRanking res = new DocumentRanking(value, docranks);

		
		return res;
		
		
        
		
//		System.out.println(collect);
//
//		
//	
//		//System.out.println();
//		Dataset<TermArticleDPH> termofquerydph = dphall.getValue().filter(condition);
//		
//		
//		termofquerydph.printSchema();
//		System.out.println(termofquerydph.count());
//
//		
//		RelationalGroupedDataset groupdph = dphall.getValue().groupBy("article");
//		Dataset<Row> groupdphdata = groupdph.df();
//		groupdphdata.printSchema();
//		
//		Dataset<Row> docdph = groupdph.avg("DPHscore").orderBy(functions.desc("avg(DPHscore)"));
//		
//		docdph.printSchema();
//		
		
		
		
		
		//TODO
		
//		Encoder<List<Double>> listEncoder = (Encoder<List<Double>>) Encoders.javaSerialization(Encoders.DOUBLE().getClass());
//		
//		Dataset<Tuple2<NewsArticle, double[]>> rankreslist = querydocdph.map(row -> {
//			NewsArticle col1 = row.getAs("doc");
//			double[] col2 = row.getAs("dphscore");
//		    return new Tuple2<>(col1, col2);
//		}, Encoders.tuple(Encoders.bean(NewsArticle.class), listEncoder));
//		
		
		
		

		// Collect the results into a list
		
		//Dataset<RankedResult> rankedresult = querydocdph.map(func, evidence$6)
		
		

		
		
		
		
	}

}