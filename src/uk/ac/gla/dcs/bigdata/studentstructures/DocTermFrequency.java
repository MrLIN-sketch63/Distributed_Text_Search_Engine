package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class DocTermFrequency implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2704857443974634078L;
	
	String id; 
	String term; 
	Long frequency;
	
	public DocTermFrequency(String id, String term, Long frequency) {
		super();
		this.id = id;
		this.term = term;
		this.frequency = frequency;
		
	}

	public DocTermFrequency(){
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public Long getFrequency() {
		return frequency;
	}

	public void setFrequency(Long frequency) {
		this.frequency = frequency;
	}
	
	
	

}
