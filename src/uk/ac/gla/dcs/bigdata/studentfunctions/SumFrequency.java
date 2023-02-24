package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;

import java.util.Iterator;

public class SumFrequency implements MapGroupsFunction<String, DocTermFrequency, Tuple2<String, Long>>{
    private static final long serialVersionUID = -6820284532911119737L;
    @Override
    public Tuple2<String, Long> call(String term, Iterator<DocTermFrequency> values) throws Exception {

        long sumfrequency = (long)0;

        while (values.hasNext()) {
            DocTermFrequency d = values.next();
            sumfrequency += d.getFrequency();
        }

        return new Tuple2<String ,Long>(term,sumfrequency);
    }
}
