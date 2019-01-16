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
