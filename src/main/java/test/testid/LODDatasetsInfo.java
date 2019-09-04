package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.time.StopWatch;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.header.Header;

public class LODDatasetsInfo {

	public static final Map<String, StringBuffer> datasetsErr = new LinkedHashMap<String, StringBuffer>();
	public static void main(String[] args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		ExperimentNN exp = new ExperimentNN();
		Set<String> datasets = new LinkedHashSet<String>();
//		datasets.addAll(exp.getDatasets(new File("/media/andre/Seagate/personalDatasets/"), 1000000));
		//datasets.addAll(exp.getDatasets(new File("dirHDT"), 10));
		//datasets.addAll(exp.getDatasets(new File("dirHDTFamous"), 20));
		datasets.addAll(exp.getDatasets(new File("dirHDTLaundromat"), -1));
//		datasets.addAll(exp.getDatasets(new File("dirHDTtests"), 9999));
//		datasets.addAll(getEndpoints(new File("endpoints.txt")));
//		datasets.add("http://dbpedia.org/sparql");
//		datasets.add("http://semanticweb.cs.vu.nl/dss/sparql/");
		
		System.out.println("#Datasets: " + datasets.size());
		Set<LODDataset> lodDatasets = new LinkedHashSet<LODDataset>();
		for (String ds : datasets) {
		//datasets.parallelStream().forEach(ds -> {
			LODDataset lodDs = null;
			try {
				lodDs = getLODDataset(ds);
			} catch (Exception e) {
				includeError(ds, e.getMessage());
				e.printStackTrace();
			}
			lodDatasets.add(lodDs);
		}
		//});
		printInfo(lodDatasets, "LODDsStatitiscs.tsv");
		System.out.println("#Datasets With Error: " + datasetsErr.size());
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}

	private static void includeError(String ds, String message) {
		if(datasetsErr.get(ds) != null) {
  		    datasetsErr.get(ds).append("---" + message);
		} else {
			StringBuffer sb = new StringBuffer();
			sb.append(message);
			datasetsErr.put(ds, sb);
		}	
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
						+ lodDs.getNumDatasetSimilar() + "\t" + lodDs.getAvgInOutDegree() + "\t"
						+ lodDs.getMaxInOutDegree();
				writer.println(line);
			} else {
				includeError(lodDs.getDatasetName(), "#Triples < 1");
			}
		}
		writer.close();
		System.out.println("File Output: " + (new File(fileName)).getAbsolutePath());
		PrintWriter writerError = new PrintWriter("Errors.txt", "UTF-8");
		writerError.println("File\tError");
		for (Entry<String, StringBuffer> entry : datasetsErr.entrySet()) {
			writerError.println(entry.getKey() + "\t" + entry.getValue().toString());
		}
		writerError.close();
	}

	private static LODDataset getLODDataset(String ds) throws IOException, NotFoundException {
		LODDataset lodDs = new LODDataset();
		lodDs.setDatasetName(ds);
		HDT hdt = null;
		if (ds.endsWith("hdt")) {
			try {
				hdt = HDTManager.mapHDT(ds, null);
				Header header = hdt.getHeader();
				Dictionary dic = hdt.getDictionary();
				lodDs.setNumSubjects(dic.getNsubjects());
				lodDs.setNumPredicates(dic.getNpredicates());
				lodDs.setNumObjects(dic.getNobjects());
				lodDs.setNumTriples(LODUtil.getHeaderInfo(header, "triples"));

				lodDs.setNumSameAs(LODUtil.getNumSameAs(ds, hdt));
				lodDs.setNumClasses(LODUtil.getNumClasses(ds, hdt));
				//lodDs.setNumProperties(LODUtil.getNumProperties(ds, hdt));
				lodDs.setNumProperties(dic.getNpredicates());

				lodDs.setNumDuplicatedInstances(LODUtil.getNumDuplicateInstances(ds, hdt));
				lodDs.setNumLoops(LODUtil.getNumLoops(ds, hdt));
				lodDs.setNumDatasetSimilar(LODUtil.getNumSimilarDatasets(ds, hdt));
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
			lodDs.setNumDatasetSimilar(LODUtil.getNumSimilarDatasets(ds, hdt));
			lodDs.setAvgInOutDegree(LODUtil.getAvgInOutDegree(ds, hdt));
			lodDs.setMaxInOutDegree(LODUtil.getMaxInOutDegree(ds, hdt));
		}
		return lodDs;
	}

}