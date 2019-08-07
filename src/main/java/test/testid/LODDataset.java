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
	long numDuplicatedInstances, numSameAs, numLoops, avgInOutDegree, maxInOutDegree, numSubjects, numPredicates,
			numObjects, numTriples, numClasses, numProperties;

	Map<String, Integer> datasetsSimilar;

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public long getNumDuplicatedInstances() {
		return numDuplicatedInstances;
	}

	public void setNumDuplicatedInstances(long numDuplicatedInstances) {
		this.numDuplicatedInstances = numDuplicatedInstances;
	}

	public long getNumSameAs() {
		return numSameAs;
	}

	public void setNumSameAs(long numSameAs) {
		this.numSameAs = numSameAs;
	}

	public long getNumLoops() {
		return numLoops;
	}

	public void setNumLoops(long numLoops) {
		this.numLoops = numLoops;
	}

	public long getAvgInOutDegree() {
		return avgInOutDegree;
	}

	public void setAvgInOutDegree(long avgInOutDegree) {
		this.avgInOutDegree = avgInOutDegree;
	}

	public long getMaxInOutDegree() {
		return maxInOutDegree;
	}

	public void setMaxInOutDegree(long maxInOutDegree) {
		this.maxInOutDegree = maxInOutDegree;
	}

	public long getNumSubjects() {
		return numSubjects;
	}

	public void setNumSubjects(long numSubjects) {
		this.numSubjects = numSubjects;
	}

	public long getNumPredicates() {
		return numPredicates;
	}

	public void setNumPredicates(long numPredicates) {
		this.numPredicates = numPredicates;
	}

	public long getNumObjects() {
		return numObjects;
	}

	public void setNumObjects(long numObjects) {
		this.numObjects = numObjects;
	}

	public long getNumTriples() {
		return numTriples;
	}

	public void setNumTriples(long numTriples) {
		this.numTriples = numTriples;
	}

	public long getNumClasses() {
		return numClasses;
	}

	public void setNumClasses(long numClasses) {
		this.numClasses = numClasses;
	}

	public long getNumProperties() {
		return numProperties;
	}

	public void setNumProperties(long numProperties) {
		this.numProperties = numProperties;
	}

	public Map<String, Integer> getDatasetsSimilar() {
		return datasetsSimilar;
	}

	public void setDatasetsSimilar(Map<String, Integer> datasetsSimilar) {
		this.datasetsSimilar = datasetsSimilar;
	}

}
