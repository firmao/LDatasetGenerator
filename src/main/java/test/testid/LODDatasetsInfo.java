package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashSet;
import java.util.Set;

public class LODDatasetsInfo {

	public static void main(String[] args) throws Exception {
		ExperimentNN exp = new ExperimentNN();
		Set<String> datasets = new LinkedHashSet<String>();
//		datasets.addAll(exp.getDatasets(new File("/media/andre/Seagate/personalDatasets/"), 1000000));
//		datasets.addAll(exp.getDatasets(new File("dirHDT"), 100000));
//		datasets.addAll(exp.getDatasets(new File("dirHDTFamous"), 1000000));
		datasets.addAll(exp.getDatasets(new File("dirHDTtests"), 9999));
//		datasets.addAll(getEndpoints(new File("endpoints.txt")));
		Set<LODDataset> lodDatasets = new LinkedHashSet<LODDataset>();
		for (String ds : datasets) {
			LODDataset lodDs = getLODDataset(ds);
			lodDatasets.add(lodDs);
		}
		printInfo(lodDatasets, "LODDsStatitiscs.tsv");
	}

	private static void printInfo(Set<LODDataset> lodDatasets, String fileName) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("Dataset\tNumSubjects\tNumPredicates\tNumObjects\tNumTriples"
				+ "\tNumSameAs\tNumClasses\tNumProperties"
				+ "\tNumDuplicatedInstances\tNumLoops\tNumDatasetsSimilar\tAvgInOutDegree\tMaxInOutDegree");
		for (LODDataset lodDs : lodDatasets) {
			String line = lodDs.getDatasetName() + "\t" + lodDs.getNumSubjects() + "\t" + lodDs.getNumPredicates() 
			+ "\t" + lodDs.getNumObjects() + "\t" + lodDs.getNumTriples() + "\t" + lodDs.getNumSameAs() 
			+ "\t" + lodDs.getNumClasses() + "\t" + lodDs.getNumProperties() + "\t" + lodDs.getNumDuplicatedInstances() 
			+ "\t" + lodDs.getNumLoops() + "\t" + lodDs.getDatasetsSimilar().size() + "\t" + lodDs.getAvgInOutDegree() 
			+ "\t" + lodDs.getMaxInOutDegree();
			writer.println(line);
		}
		writer.close();
	}

	private static LODDataset getLODDataset(String ds) {
		LODDataset lodDs = new LODDataset();
		lodDs.setDatasetName(ds);
		
		lodDs.setNumSubjects(LODUtil.getNumSubjects(ds));
		lodDs.setNumPredicates(LODUtil.getNumPredicates(ds));
		lodDs.setNumObjects(LODUtil.getNumObjects(ds));
		lodDs.setNumTriples(LODUtil.getNumTriples(ds));
		
		lodDs.setNumSameAs(LODUtil.getNumSameAs(ds));
		lodDs.setNumClasses(LODUtil.getNumClasses(ds));
		lodDs.setNumProperties(LODUtil.getNumProperties(ds));
		
		lodDs.setNumDuplicatedInstances(LODUtil.getNumDuplicateInstances(ds));
		lodDs.setNumLoops(LODUtil.getNumLoops(ds));
		lodDs.setDatasetsSimilar(LODUtil.getDatasetsSimilar(ds));
		lodDs.setAvgInOutDegree(LODUtil.getAvgInOutDegree(ds));
		lodDs.setMaxInOutDegree(LODUtil.getMaxInOutDegree(ds));
		
		return lodDs;
	}

}
