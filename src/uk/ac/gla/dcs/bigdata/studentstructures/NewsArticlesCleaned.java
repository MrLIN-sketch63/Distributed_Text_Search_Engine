package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class NewsArticlesCleaned implements Serializable {
	private static final long serialVersionUID = 7860293794078492243L;
	
	String id; // unique article identifier
	List<String> title; // article title
	List<String> paragraph; // the contents of the paragraph // the contents of the title and the paragraph
	String originalNews;
	List<String> content = new ArrayList<String>();
	
	public NewsArticlesCleaned(String id, List<String> title, List<String> paragraph, String originalNews) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.originalNews = originalNews;
		
		if(this.title != null) this.content.addAll(this.title);
		if(this.content != null) this.content.addAll(this.paragraph);
	}

	public String getOriginalNews() {
		return originalNews;
	}
	public List<String> getContent() {
		return content;
	}

	public void setContent(List<String> content) {
		this.content = content;
	}

	public void setOriginalNews(String originalNews) {
		this.originalNews = originalNews;
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
