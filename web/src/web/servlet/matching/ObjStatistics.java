package web.servlet.matching;

import java.util.Set;

public class ObjStatistics {
	Set<String> properties;
	Set<String> propertiesMatched;
	int totalNumberDatasets;
	Set<String> top10DatasetsMoreSimilar;
	
	public Set<String> getProperties() {
		return properties;
	}
	public void setProperties(Set<String> properties) {
		this.properties = properties;
	}
	public Set<String> getPropertiesMatched() {
		return propertiesMatched;
	}
	public void setPropertiesMatched(Set<String> propertiesMatched) {
		this.propertiesMatched = propertiesMatched;
	}
	public int getTotalNumberDatasets() {
		return totalNumberDatasets;
	}
	public void setTotalNumberDatasets(int totalNumberDatasets) {
		this.totalNumberDatasets = totalNumberDatasets;
	}
	public Set<String> getTop10DatasetsMoreSimilar() {
		return top10DatasetsMoreSimilar;
	}
	public void setTop10DatasetsMoreSimilar(Set<String> top10DatasetsMoreSimilar) {
		this.top10DatasetsMoreSimilar = top10DatasetsMoreSimilar;
	}
}
