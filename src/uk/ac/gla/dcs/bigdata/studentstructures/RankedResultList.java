package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class RankedResultList  implements Serializable{
	private static final long serialVersionUID = -5066661179554991889L;
	List<RankedResult> RankedResultList;

	
	public RankedResultList () {}
	
	public RankedResultList(List<RankedResult> rankedresult) {
		super();
		this.RankedResultList  = rankedresult;
	}

	
	public List<RankedResult> getRankedResultLis() {
		return RankedResultList ;
	}

	
	public void setRankedResultLis(List<RankedResult> rankedResultList) {
		this.RankedResultList  = rankedResultList;
	}
	
}
