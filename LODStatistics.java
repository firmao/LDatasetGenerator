package test.testid;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LODStatistics {

	public static void main(String args[]) {
		Set<String> setPropFilter = new HashSet<String>();
		Set<String> setProp1 = new HashSet<String>();
		Set<String> setProp2 = new HashSet<String>();
		Set<String> setProp3 = new HashSet<String>();
		Set<String> setProp4 = new HashSet<String>();
		Map<String, Set<String>> mapPropValue = new HashMap<String, Set<String>>();
		
		setProp1.add("one");
		setProp1.add("one1.5");
		setProp2.add("two");
		setProp3.add("three");
		setProp3.add("three1.4");
		setProp4.add("four");
		setPropFilter.add("prop2");
		setPropFilter.add("prop3");
		mapPropValue.put("prop1", setProp1);
		mapPropValue.put("prop2", setProp2);
		mapPropValue.put("prop3", setProp3);
		mapPropValue.put("prop4", setProp4);
		
		System.out.println("Before: " + mapPropValue);
		filterProperties(mapPropValue, setPropFilter);
		System.out.println("after: " + mapPropValue);
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
							ret.addAll(Util.execQueryEndPoint(cSparql, source, true));
						} else {
							ret.addAll(Util.execQueryRDFRes(cSparql, source));
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
		String bestDs = Util.getMax(mBestDs);
		int number = mBestDs.get(bestDs);
		System.out.println("Dataset with more " + predicate + " is " +  bestDs + " : " + number);
	}

}
