package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermFrequency;

public class DocToTerm implements MapFunction<DocTermFrequency,String> {
    private static final long serialVersionUID = -912102331193043376L;

    @Override
    public String call(DocTermFrequency value) throws Exception {
        return value.getTerm();
    }
}
