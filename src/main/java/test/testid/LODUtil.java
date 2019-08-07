package test.testid;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class LODUtil {

	public static int getNumClasses(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getAvgInOutDegree(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumPredicates(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumProperties(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumSameAs(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumSubjects(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumTriples(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumDuplicateInstances(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumLoops(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static Map<String, Integer> getDatasetsSimilar(String ds) {
		// TODO Auto-generated method stub
		return null;
	}

	public static int getMaxInOutDegree(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}

	public static int getNumObjects(String ds) {
		String cSparql = "";
		Set<String> ret = execSparql(cSparql,ds);
		return ret.size();
	}
	
	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();

		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(900000); // 15 minutes
			Runnable block = new Runnable() {
				public void run() {
			//		ret.addAll(Util.execQueryEndPoint(cSparql, source));
					if (Util.isEndPoint(source)) {
						ret.addAll(Util.execQueryEndPoint(cSparql, source));
					} else {
						ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
					}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.err.println("TIME-OUT-ERROR - dataset/source: " + source);
		}
		
		return ret;
	}

}
