package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;


public class NewsArticlesCleaned implements Serializable {
	private static final long serialVersionUID = 7860293794078492243L;
	
	String id; // unique article identifier
	List<String> title; // article title
	List<String> paragraph; // the contents of the paragraph
	List<String> content;  // the contents of the title and the paragraph
	
	public NewsArticlesCleaned(String id, List<String> title, List<String> paragraph) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.content.addAll(this.title);
		this.content.addAll(this.paragraph);
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
