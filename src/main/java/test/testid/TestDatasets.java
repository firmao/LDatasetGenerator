package test.testid;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class TestDatasets {

	public static void main(String[] args) throws Exception {
		System.out.println("java -jar TestDatasets.jar <datasets:Dir or file> <SPARQL query>");
		//String datasetsPlace = "endpoints.txt";
		String datasetsPlace = "dirHDT";
		String cSparql = "SELECT ?s WHERE { ?s a <http://dbpedia.org/ontology/City> } limit 10";

		if (args.length > 0) {
			if (args[0] != null) {
				datasetsPlace = args[0];
			}
			if (args[1] != null) {
				cSparql = args[1];
			}
		}
		File f = new File(datasetsPlace);
		Set<String> setDatasets = new LinkedHashSet<String>();
		if (!f.exists()) {
			System.out.println("Dataset Location does not exist: " + f.getAbsolutePath());
		}
		if (f.isDirectory()) {
			setDatasets.addAll(loadDsFromDir(f));
		} else {
			setDatasets.addAll(loadDsFromFile(f));
		}
		PrintWriter writer = new PrintWriter("GoodDatasets.txt", "UTF-8");
		for (String ds : setDatasets) {
			Set<String> goodDs = execSparql(cSparql, ds);
			if (goodDs.size() > 1) {
				writer.println(ds);
			}
		}
		writer.close();
	}

	private static Collection<String> loadDsFromFile(File f) throws IOException {
		Set<String> setLines = new LinkedHashSet<String>(FileUtils.readLines(f, "UTF-8"));
		return setLines;
	}

	private static Collection<String> loadDsFromDir(File f) throws Exception {
		ExperimentNN exp = new ExperimentNN();
		return exp.getDatasets(f, 99999999);
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();

		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(180000); // 3 minutes
			Runnable block = new Runnable() {
				public void run() {
					// ret.addAll(Util.execQueryEndPoint(cSparql, source));
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
		System.out.println("#Size: " + ret.size());
		return ret;
	}
}
