package test.testid;

import java.util.Map;

public class LODDataset {
	/*
	 * -List of properties -Number of duplicated instances, triples with other
	 * datasets(sameAs) -Percentage of literals, blank nodes, uri in subjects or
	 * object or predicate -Number of loops -Avg in and out degrees of nodes
	 * -Maximum in and out degree of a node used in the dataset -Similarity score
	 * with other datasets that is greater than 0(based on the properties and
	 * concepts that they share) -Number of links with other datasets(SameAs) -Total
	 * subjects -Total objects -Total predicates -Total triples -Total classes
	 * (Concepts)
	 */
	String datasetName;
	int numDuplicatedInstances, numSameAs, numLoops, avgInOutDegree, maxInOutDegree, numSubjects, numPredicates,
			numObjects, numTriples, numClasses, numProperties;

	public int getNumObjects() {
		return numObjects;
	}

	public void setNumObjects(int numObjects) {
		this.numObjects = numObjects;
	}

	Map<String, Integer> datasetsSimilar;

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public int getNumDuplicatedInstances() {
		return numDuplicatedInstances;
	}

	public void setNumDuplicatedInstances(int numDuplicatedInstances) {
		this.numDuplicatedInstances = numDuplicatedInstances;
	}

	public int getNumSameAs() {
		return numSameAs;
	}

	public void setNumSameAs(int numSameAs) {
		this.numSameAs = numSameAs;
	}

	public int getNumLoops() {
		return numLoops;
	}

	public void setNumLoops(int numLoops) {
		this.numLoops = numLoops;
	}

	public int getAvgInOutDegree() {
		return avgInOutDegree;
	}

	public void setAvgInOutDegree(int avgInOutDegree) {
		this.avgInOutDegree = avgInOutDegree;
	}

	public int getMaxInOutDegree() {
		return maxInOutDegree;
	}

	public void setMaxInOutDegree(int maxInOutDegree) {
		this.maxInOutDegree = maxInOutDegree;
	}

	public int getNumSubjects() {
		return numSubjects;
	}

	public void setNumSubjects(int numSubjects) {
		this.numSubjects = numSubjects;
	}

	public int getNumPredicates() {
		return numPredicates;
	}

	public void setNumPredicates(int numPredicates) {
		this.numPredicates = numPredicates;
	}

	public int getNumTriples() {
		return numTriples;
	}

	public void setNumTriples(int numTriples) {
		this.numTriples = numTriples;
	}

	public int getNumClasses() {
		return numClasses;
	}

	public void setNumClasses(int numClasses) {
		this.numClasses = numClasses;
	}

	public int getNumProperties() {
		return numProperties;
	}

	public void setNumProperties(int numProperties) {
		this.numProperties = numProperties;
	}

	public Map<String, Integer> getDatasetsSimilar() {
		return datasetsSimilar;
	}

	public void setDatasetsSimilar(Map<String, Integer> datasetsSimilar) {
		this.datasetsSimilar = datasetsSimilar;
	}

}
