package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;


public class DPHQueryTerm implements Serializable{
	private static final long serialVersionUID = 7309797024926062989L;
	
	short termFrequencyInCurrentDocument; //一个term在当前文档出现的数量
	int totalTermFrequencyInCorpus;//一个term在所有文件出现的次数
	int currentDocumentLength;//当前文件的长度
	double averageDocumentLengthInCorpus;//所有文档的平均长度
	long totalDocsInCorpus;//一共有多少个文件在数据集中
	double DPHsocre;
	
	public DPHQueryTerm(short termFrequencyInCurrentDocument, int totalTermFrequencyInCorpus, int currentDocumentLength,
			double averageDocumentLengthInCorpus, long totalDocsInCorpus) {
		super();
		this.termFrequencyInCurrentDocument = termFrequencyInCurrentDocument;
		this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
		this.currentDocumentLength = currentDocumentLength;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
	}
	
	public double getDPHsocre() {
		return DPHsocre;
	}
	public void setDPHsocre(double dPHsocre) {
		DPHsocre = dPHsocre;
	}
	public short getTermFrequencyInCurrentDocument() {
		return termFrequencyInCurrentDocument;
	}
	public int getTotalTermFrequencyInCorpus() {
		return totalTermFrequencyInCorpus;
	}
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}
	public double getAverageDocumentLengthInCorpus() {
		return averageDocumentLengthInCorpus;
	}
	public long getTotalDocsInCorpus() {
		return totalDocsInCorpus;
	}
	
	
}
