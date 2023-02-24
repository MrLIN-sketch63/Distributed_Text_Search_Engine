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
	NewsArticle originalNews;
	List<String> content = new ArrayList<String>();
	Long doc_length;
	List<String> titleAndParagraphTerms = new ArrayList<String>();
	
	HashMap<String, Long> wordMap = new HashMap<String, Long>();
	
	public NewsArticlesCleaned(String id, List<String> title, List<String> paragraph, NewsArticle originalNews, Long doc_length) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.originalNews = originalNews;
		this.doc_length = doc_length;

		if(this.title != null) this.content.addAll(this.title);
		if(this.content != null) this.content.addAll(this.paragraph);
		
		WordFrequency();

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

	public NewsArticle getOriginalNews() {
		return originalNews;
	}
	public List<String> getContent() {
		return content;
	}

	public void setContent(List<String> content) {
		this.content = content;
	}

	public void setOriginalNews(NewsArticle originalNews) {
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
	public HashMap<String, Long> getWordMap() {
		return wordMap;
	}


	public void setParagraph(List<String> paragraph) {
		this.paragraph = paragraph;
	}
	
	public void WordFrequency() {
		
		for (String word : this.content) {
            if (this.wordMap.containsKey(word)) {
                wordMap.put(word, wordMap.get(word) + (long)1);
            } else {
                wordMap.put(word, (long) 1);
            }
        }
		}


}
