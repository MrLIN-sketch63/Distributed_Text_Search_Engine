package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class NewsArticlesCleaned implements Serializable {
	private static final long serialVersionUID = 7860293794078492243L;
	
	String id; // unique article identifier
	List<String> title; // article title
	List<String> paragraph; // the contents of the paragraph // the contents of the title and the paragraph
	String originalNews;
	List<String> content = new ArrayList<String>();
	Long doc_length;
	HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
	
	public NewsArticlesCleaned(String id, List<String> title, List<String> paragraph, String originalNews, Long doc_length) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.originalNews = originalNews;
		this.doc_length = doc_length;
		
		
		
//		  for (String word : wordMap.keySet()) {
//	            System.out.println(word + ": " + wordMap.get(word));
//	        }
		if(this.title != null) this.content.addAll(this.title);
		if(this.content != null) this.content.addAll(this.paragraph);
		WordFrequency();
		System.out.println(wordMap);
	}

	public Long getDoc_length() {
		return doc_length;
	}

	public void setDoc_length(Long doc_length) {
		this.doc_length = doc_length;
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
	

	public void WordFrequency() {
		
		for (String word : this.content) {
            if (this.wordMap.containsKey(word)) {
                wordMap.put(word, wordMap.get(word) + 1);
            } else {
                wordMap.put(word, 1);
            }
        }
		}
}
