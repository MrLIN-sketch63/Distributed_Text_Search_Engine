package uk.ac.gla.dcs.bigdata.studentstructures.List;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.studentstructures.QueryArticleDPH;


public class QueryArticleDphList implements Serializable{
	private static final long serialVersionUID = -5066661179664991989L;
	List<QueryArticleDPH> QueryArticleDphList;
	public QueryArticleDphList(List<QueryArticleDPH> queryArticleDphList) {
		super();
		QueryArticleDphList = queryArticleDphList;
	}
	public List<QueryArticleDPH> getQueryArticleDphList() {
		return QueryArticleDphList;
	}
	public void setQueryArticleDphList(List<QueryArticleDPH> queryArticleDphList) {
		QueryArticleDphList = queryArticleDphList;
	}
	
	
}
