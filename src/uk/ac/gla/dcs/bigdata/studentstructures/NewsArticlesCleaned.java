package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


public class NewsArticlesCleaned implements Serializable {
	private static final long serialVersionUID = 7860293794078492243L;
	
	String id; // unique article identifier
	List<String> title; // article title
	List<String> paragraph; // the contents of the paragraph // the contents of the title and the paragraph
	Long doc_length;

	
	public NewsArticlesCleaned(String id, List<String> title, List<String> paragraph, Long doc_length) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.doc_length = doc_length;

	}


	public NewsArticlesCleaned() {
		// TODO Auto-generated constructor stub
	}


	public Long getDoc_length() {
		return doc_length;
	}

	public void setDoc_length(Long doc_length) {
		this.doc_length = doc_length;
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<String> getTitle() {
		return title;
	}
	public void setTitle(List<String> title) {
		this.title = title;
	}
	public List<String> getParagraph() {
		return paragraph;
	}

	public void setParagraph(List<String> paragraph) {
		this.paragraph = paragraph;
	}
	

}
