package uk.ac.gla.dcs.bigdata.studentstructures.List;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.studentstructures.TermArticleDPH;

public class TermArticleDPHList implements Serializable{
	private static final long serialVersionUID = -5066661179664991989L;
	List<TermArticleDPH> TermArticleDPHList;
	public List<TermArticleDPH> getTermArticleDPHList() {
		return TermArticleDPHList;
	}
	public void setTermArticleDPHList(List<TermArticleDPH> termArticleDPHList) {
		TermArticleDPHList = termArticleDPHList;
	}
	public TermArticleDPHList(List<TermArticleDPH> termArticleDPHList) {
		super();
		TermArticleDPHList = termArticleDPHList;
	}

}
