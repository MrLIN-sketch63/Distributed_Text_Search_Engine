package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.List.RankedResultList;

public class RankedResultToListMap implements MapFunction<RankedResult, RankedResultList>{

	private static final long serialVersionUID = 1L;
	@Override
	public RankedResultList call(RankedResult rankedResult) throws Exception {
		List<RankedResult> asList = new ArrayList<RankedResult>(1);
		asList.add(rankedResult);
		return new RankedResultList(asList);
		
	}
	
}
