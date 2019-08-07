package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

public class LODDatasetsInfo {

	public static void main(String[] args) throws Exception {
		ExperimentNN exp = new ExperimentNN();
		Set<String> datasets = new LinkedHashSet<String>();
//		datasets.addAll(exp.getDatasets(new File("/media/andre/Seagate/personalDatasets/"), 1000000));
//		datasets.addAll(exp.getDatasets(new File("dirHDT"), 100000));
		datasets.addAll(exp.getDatasets(new File("dirHDTFamous"), 99));
//		datasets.addAll(exp.getDatasets(new File("dirHDTtests"), 9999));
//		datasets.addAll(getEndpoints(new File("endpoints.txt")));
		Set<LODDataset> lodDatasets = new LinkedHashSet<LODDataset>();
		for (String ds : datasets) {
			LODDataset lodDs = getLODDataset(ds);
			lodDatasets.add(lodDs);
		}
		printInfo(lodDatasets, "LODDsStatitiscs.tsv");
	}

	private static void printInfo(Set<LODDataset> lodDatasets, String fileName)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println(
				"Dataset\tNumSubjects\tNumPredicates\tNumObjects\tNumTriples" + "\tNumSameAs\tNumClasses\tNumProperties"
						+ "\tNumDuplicatedInstances\tNumLoops\tNumDatasetsSimilar\tAvgInOutDegree\tMaxInOutDegree");
		for (LODDataset lodDs : lodDatasets) {
			if (lodDs.getNumTriples() > 0) {
				String line = lodDs.getDatasetName() + "\t" + lodDs.getNumSubjects() + "\t" + lodDs.getNumPredicates()
						+ "\t" + lodDs.getNumObjects() + "\t" + lodDs.getNumTriples() + "\t" + lodDs.getNumSameAs()
						+ "\t" + lodDs.getNumClasses() + "\t" + lodDs.getNumProperties() + "\t"
						+ lodDs.getNumDuplicatedInstances() + "\t" + lodDs.getNumLoops() + "\t"
						+ lodDs.getDatasetsSimilar().size() + "\t" + lodDs.getAvgInOutDegree() + "\t"
						+ lodDs.getMaxInOutDegree();
				writer.println(line);
			}
		}
		writer.close();
		System.out.println("File Output: " + (new File(fileName)).getAbsolutePath());
	}

	private static LODDataset getLODDataset(String ds) throws IOException, NotFoundException {
		LODDataset lodDs = new LODDataset();
		lodDs.setDatasetName(ds);
		HDT hdt = null;
		if (ds.endsWith("hdt")) {
			try {
				hdt = HDTManager.mapHDT(ds, null);
				Header header = hdt.getHeader();
				lodDs.setNumSubjects(LODUtil.getHeaderInfo(header, "subject"));
				lodDs.setNumPredicates(LODUtil.getHeaderInfo(header, "predicates"));
				lodDs.setNumObjects(LODUtil.getHeaderInfo(header, "objects"));
				lodDs.setNumTriples(LODUtil.getHeaderInfo(header, "triples"));

				lodDs.setNumSameAs(LODUtil.getNumSameAs(ds, hdt));
				lodDs.setNumClasses(LODUtil.getNumClasses(ds, hdt));
				lodDs.setNumProperties(LODUtil.getNumProperties(ds, hdt));

				lodDs.setNumDuplicatedInstances(LODUtil.getNumDuplicateInstances(ds, hdt));
				lodDs.setNumLoops(LODUtil.getNumLoops(ds, hdt));
				lodDs.setDatasetsSimilar(LODUtil.getDatasetsSimilar(ds, hdt));
				lodDs.setAvgInOutDegree(LODUtil.getAvgInOutDegree(ds, hdt));
				lodDs.setMaxInOutDegree(LODUtil.getMaxInOutDegree(ds, hdt));
			} catch (Exception e) {
				System.out.println("FAIL: " + ds + " Error: " + e.getMessage());
			} finally {
				if (hdt != null) {
					hdt.close();
				}
			}
		} else {
			lodDs.setNumSubjects(LODUtil.getNumSubjects(ds, hdt));
			lodDs.setNumPredicates(LODUtil.getNumPredicates(ds, hdt));
			lodDs.setNumObjects(LODUtil.getNumObjects(ds, hdt));
			lodDs.setNumTriples(LODUtil.getNumTriples(ds, hdt));

			lodDs.setNumSameAs(LODUtil.getNumSameAs(ds, hdt));
			lodDs.setNumClasses(LODUtil.getNumClasses(ds, hdt));
			lodDs.setNumProperties(LODUtil.getNumProperties(ds, hdt));

			lodDs.setNumDuplicatedInstances(LODUtil.getNumDuplicateInstances(ds, hdt));
			lodDs.setNumLoops(LODUtil.getNumLoops(ds, hdt));
			lodDs.setDatasetsSimilar(LODUtil.getDatasetsSimilar(ds, hdt));
			lodDs.setAvgInOutDegree(LODUtil.getAvgInOutDegree(ds, hdt));
			lodDs.setMaxInOutDegree(LODUtil.getMaxInOutDegree(ds, hdt));
		}
		return lodDs;
	}

}
