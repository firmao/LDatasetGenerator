package test.testid;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LODStatistics {

	public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {
		System.out.println("testing...");
		List<String> datasets = new ArrayList<String>();
		datasets.add("/media/andre/DATA/Dropbox/ws1/LDatasetGenerator/dirHDT/40d2cf880244d2f3ec7bda1098b65961.hdt");
		datasets.add("http://clinicaltrials.bio2rdf.org/sparql");
		datasets.add("http://dbpedia.org/sparql");
		Util.generateStatistics(datasets, "test1.tsv");
	}
	
	private static void filterProperties(Map<String, Set<String>> map, Set<String> setPropFilter) {
		Map<String, Set<String>> newMapPropValue = new HashMap<String, Set<String>>();
		for (String prop : setPropFilter) {
			newMapPropValue.put(prop, map.get(prop));
		}
		map.clear();
		map.putAll(newMapPropValue);
	}

	public static void generateStatistics(List<String> datasets, String predicate) throws FileNotFoundException, UnsupportedEncodingException {
		System.out.println("Sources/Datasets: " + datasets.size());
		String cSparql = "SELECT ?p (COUNT(?p) as ?pCount) WHERE\n" + 
				"{\n" + 
				"  ?s ?p ?o .\n" + 
				"  filter(?p=<"+predicate+">)\n" + 
				"}\n" + 
				"GROUP BY ?p";
		Map<String, Integer> mBestDs = new HashMap<String, Integer>();
		Set<String> ret = new HashSet<String>();
		PrintWriter writer = new PrintWriter("statisticsProp.tsv", "UTF-8");
		
		for (String source : datasets) {
		//lstSources.parallelStream().forEach( source -> {
			try {
				TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
				Runnable block = new Runnable() {
					public void run() {
						
						if (Util.isEndPoint(source)) {
							//ret.addAll(execQueryEndPoint(cSparql, source));
							ret.addAll(Util.execQueryEndPoint(cSparql, source, true, -1));
						} else {
							ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
						}
						for (String num : ret) {
							if(num.contains("^^http")) {
								String theNum = num.substring(num.indexOf(" ") +1, num.indexOf("^^http"));
								int n = Integer.valueOf(theNum);
								mBestDs.put(source, n);
								writer.println(source + "\t" + n);
							}
						}
						ret.clear();
					}
				};
				timeoutBlock.addBlock(block);// execute the runnable block
			} catch (Throwable e) {
				System.out.println("TIME-OUT-ERROR - dataset/source: " + source);
			}
		}
		writer.close();
		try {
			String bestDs = Util.getMax(mBestDs);
			int number = mBestDs.get(bestDs);
			System.out.println("Dataset with more " + predicate + " is " +  bestDs + " : " + number);
		} catch(Exception e) {
			System.out.println(e.getMessage() + "\nError obtaining dataset with more " + predicate);
		}
	}

}
