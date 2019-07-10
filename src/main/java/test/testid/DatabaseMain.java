package test.testid;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabaseMain {

	public static void main(String[] args) throws IOException {
		String dataset = "6e9e97fa4f1c9ba28ae0dd3786c2de41.hdt";
		Set<String> ret = searchDB(dataset);
		System.out.println("Dataset: " + dataset);
		ret.forEach(System.out::println);
	}

	public static void add(Map<String, Set<String>> mapExactMatch, Map<String, Map<String, String>> mapSim) {
		for (Entry<String, Set<String>> entry : mapExactMatch.entrySet()) {
			String fNameExact = entry.getKey();
			Set<String> props = entry.getValue();
			String fNameSim = fNameExact.replaceAll("_Exact.txt", "_Sim.tsv");
			String[] str = fNameExact.replaceAll("_Exact.txt", "").split("---");
			String source = str[0].replaceAll(DsRelationStatistics.OUTPUT_DIR + "/", "");
			String target = str[1].replaceAll("_Exact.txt", "");
			if (!addDB(source, target, props.size(), mapSim.get(fNameSim).size())) {
				System.err.println("Problem source: " + source + " target: " + target);
			}

//			if((mapSim.size() > 0) && (mapSim.get(fNameSim) != null)) {
//				writer.println(fNameExact + "\t" + props.size() + "\t" + mapSim.get(fNameSim).size()+ "\t" + mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
//			} else {
//				writer.println(fNameExact + "\t" + props.size() + "\t" + 0 + "\t" + mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
//			}
		}
	}

	public static boolean addDB(String source, String target, int sizeExact, int sizeSim) {
		boolean ret = false;

		return ret;
	}

	public static Set<String> getResults(String dataset) throws IOException {
		Set<String> results = searchDB(dataset);
		if (results == null) {
			results = new LinkedHashSet<String>();
			results.add("Dataset not found in our index, please add to the index queue");
		}
		return results;
	}

	private static Set<String> searchDB(String dataset) throws IOException {
		Set<String> ret1 = new LinkedHashSet<String>();
		try (Stream<String> lines = Files.lines(Paths.get(DsRelationStatistics.OUTPUT_DIR + "/tableMatches.tsv"))) {
			ret1 = lines.filter(line -> line.contains(dataset)).map(String::toUpperCase).collect(Collectors.toSet());
		}
		Set<String> ret = new LinkedHashSet<String>();
		ret.add("Dataset\t#ExactMatch\t#SimMatch");
		for (String line : ret1) {
			String s1 = line.replaceAll("OUT_TESTS/", "");
			s1 = s1.replaceAll("ANDRE_", "");
			s1 = s1.replaceAll("_EXACT.TXT", "");
			String split1[] = s1.split("\t");
			String split2[] = split1[0].split("---");
			for (String st : split2) {
				if (!st.contains(dataset.toUpperCase())) {
					int numExact = Integer.parseInt(split1[1].trim());
					int numSim = Integer.parseInt(split1[2].trim());
					if ((numExact > 0) || (numSim > 0)) {
						String sRet = st + "\t" + split1[1] + "\t" + split1[2];
						ret.add(sRet);
					}
				}
			}
		}

		return ret;
	}

	public static Set<String> getResults(Set<String> properties) {
		Set<String> results = searchDB(properties);
		if (results == null) {
			results = new LinkedHashSet<String>();
			results.add(
					"Properties not found in our index, please add a dataset with thosse properties to the index queue");
		}
		return results;
	}

	private static Set<String> searchDB(Set<String> properties) {
		// TODO Auto-generated method stub
		return null;
	}

}
