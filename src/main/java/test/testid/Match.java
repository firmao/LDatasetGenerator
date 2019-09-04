package test.testid;

import java.util.Map;

public class Match {
	private Map<String, String> source;
	private Map<String, String> target;
	private double score;
	
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	public Map<String, String> getSource() {
		return source;
	}
	public void setSource(Map<String, String> source) {
		this.source = source;
	}
	public Map<String, String> getTarget() {
		return target;
	}
	public void setTarget(Map<String, String> target) {
		this.target = target;
	}
}
