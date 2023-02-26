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
	short frequency;
	Double socre;
	int doc_length;
	
	public DocTermFrequency(){
	}

	public DocTermFrequency(String id, String term, short frequency, int doc_length) {
		super();
		this.id = id;
		this.term = term;
		this.frequency = frequency;
		this.doc_length = doc_length;
		
	}


	public String getId() {
		return id;
	}

	public Double getSocre() {
		return socre;
	}

	public void setSocre(Double socre) {
		this.socre = socre;
	}

	public int getDoc_length() {
		return doc_length;
	}

	public void setDoc_length(int doc_length) {
		this.doc_length = doc_length;
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

	public short getFrequency() {
		return frequency;
	}

	public void setFrequency(short frequency) {
		this.frequency = frequency;
	}
		
	public short frequencySearch(String id, String term) {
		if(this.id.equals(id)&& this.term.equals(term)) {
			return this.frequency;
		}
		
		else {
			return 0;
		}
	}

}
