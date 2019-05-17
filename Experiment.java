package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.ObjectInputStream.GetField;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.jena.lang.csv.CSV2RDF;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

public class Experiment {
	private String ds, dt, goldStandard;
	private Set<String> manyDt;
	
	public Set<String> getManyDt() {
		return manyDt;
	}

	public void setManyDt(Set<String> manyDt) {
		this.manyDt = manyDt;
	}

	public String getDs() {
		return ds;
	}

	public void setDs(String pDs) {
		if (!pDs.endsWith(".hdt")) {
			try {
				this.ds = convertHDT(pDs);
			}catch(Exception e) {
				System.err.println("Problem converting file to HDT: " + pDs);
			}
		}
		this.ds = pDs;
	}

	public String getDt() {
		return dt;
	}

	public void setDt(String pDt) {
		if (!pDt.endsWith(".hdt")) {
			try {
				this.dt = convertHDT(pDt);
			}catch(Exception e) {
				System.err.println("Problem converting file to HDT: " + pDt);
			}
		}
		this.dt = pDt;
	}

	public String getGoldStandard() {
		return goldStandard;
	}

	public void setGoldStandard(String goldStandard) {
		this.goldStandard = goldStandard;
	}

	private String convertHDT(String pDs) throws IOException, ParserException {
		// Configuration variables
		String baseURI = "http://example.com/mydataset";
		String hdtOutput = pDs.replace(pDs.substring(pDs.indexOf(".")), ".hdt");

		if(pDs.endsWith(".csv")) {
			pDs = convertCSV2NT(pDs);
		}
		
		// Create HDT from RDF file
		HDT hdt = HDTManager.generateHDT(pDs, // Input RDF File
				baseURI, // Base URI
				RDFNotation.guess(pDs), // Input Type
				new HDTSpecification(), // HDT Options
				null // Progress Listener
		);

		// OPTIONAL: Add additional domain-specific properties to the header:
		// Header header = hdt.getHeader();
		// header.insert("myResource1", "property" , "value");

		// Save generated HDT to a file
		hdt.saveToHDT(hdtOutput, null);
		return hdtOutput;
	}

	private String convertCSV2NT(String pDs) throws IOException {
		CSV2RDF.init();
	    //load through manager:
	    Model m = RDFDataMgr.loadModel(pDs) ;
	    //classic way to load:
//	    Model m = ModelFactory.createDefaultModel();
//	    try (InputStream in = JenaCSVTest.class.getResourceAsStream("/test.csv")) {
//	        m.read(in, "http://example.com", "csv");
//	    }
	    m.setNsPrefix("test", "http://example.com#");
	    String fileName = pDs.replaceAll(".csv", ".nt");
	    FileWriter out = new FileWriter( fileName );
	    m.write(out, "nt");
		return fileName;
	}

	public void execute() throws IOException {
		final Map<String, String> mapMatches = new LinkedHashMap<String, String>();
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();
		
		if(getManyDt() != null) {
			Set<String> dts = getManyDt();
			for (String dt : dts) {
				mapPropsDs.put(dt, PropertyMatching.schemaMatching(getDs(), dt));
			}
			writeDsPropsMatched(mapPropsDs);
		}else {
			mapMatches.putAll(PropertyMatching.schemaMatching(getDs(), getDt()));
			String fileName = "MatchingMap.tsv";
			writeFile(mapMatches, fileName, getDs(), getDs());

			evaluate(mapMatches);
		}
	}

	private void writeDsPropsMatched(Map<String, Map<String, String>> mapPropsDs) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("RelationDsPropsMatched2.tsv", "UTF-8");
		writer.println("DatasetSource\tDatasetTarget\t#Matches\tPairs");
		for (Entry<String, Map<String, String>> dsProp : mapPropsDs.entrySet()) {
			String dt = dsProp.getKey();
			Map<String, String> matches = dsProp.getValue();
			int excludeCases = 0;
			for (Entry<String, String> match : matches.entrySet()) {
				if(match.getValue() == null) {
					excludeCases++;
				}
			}
			writer.println(getDs() + "\t" + dt + "\t" + (matches.size()-excludeCases) + "\t" + matches);
		}
		writer.close();
	}
	
	private void writeFile(Map<String, String> mapMatches, String fileName, String dsS, String dsT)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("#Dataset Source:" + dsS + "\t" + "#Dataset Target:" + dsT);
		for (Entry<String, String> entry : mapMatches.entrySet()) {
			writer.println(entry.getKey() + "\t" + entry.getValue());
		}
		writer.close();
	}

	private void evaluate(Map<String, String> mapMatches) throws IOException {
		final Map<String, String> mapGoldStandard = new LinkedHashMap<String, String>();
		mapGoldStandard.putAll(convFileToMap(getGoldStandard()));
		Evaluation ev = compare(mapMatches, mapGoldStandard);
		PrintWriter writer = new PrintWriter(getGoldStandard().replaceAll(".tsv", "_eval.txt"), "UTF-8");
		writer.println(mapMatches);
		writer.println("Precision: " + ev.getPrecision());
		writer.println("Recall: " + ev.getRecall());
		writer.println("F-Score: " + ev.getFscore());
		writer.close();
	}

	private static Map<String, String> convFileToMap(String fMap) throws IOException {
		final Map<String, String> mapGoldStandard = new LinkedHashMap<String, String>();
		List<String> lstLines = FileUtils.readLines(new File(fMap), "UTF-8");
		for (String line : lstLines) {
			String s[] = line.split("\t");
			if (s.length < 2)
				continue;

			mapGoldStandard.put(s[0], s[1]);
		}

		return mapGoldStandard;
	}

	private Evaluation compare(Map<String, String> mapMatches, Map<String, String> mapGoldStandard) {

//		if(mapMatches.size() != mapGoldStandard.size()) {
//			System.err.println("No possible to evaluate, because the maps should have the same size");
//		}

		Evaluation ev = new Evaluation();
		int tp = 0;
		int fn = 0;
		int fp = 0;
		int p = 0;
		double fScore = 0.0;
		double recall = 0.0;
		double precision = 0.0;
		for (Entry<String, String> entry : mapGoldStandard.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			if (mapMatches.containsKey(key)) {
				p++;
				if (mapMatches.get(key).equalsIgnoreCase(value)) {
					System.out.println("equal: " + key + ", " + value);
					tp++;
				} else {
					fp++;
				}
			} else {
				fn++;
			}
		}

//		if(tp == mapGoldStandard.size()) {
//			recall = 1.0;
//			precision = 1.0;
//			fScore = 1.0;
//			ev.setFscore(fScore);
//			ev.setPrecision(precision);
//			ev.setRecall(recall);
//			
//			return ev;
//		}

		precision = Double.valueOf(tp) / (Double.valueOf(tp) + Double.valueOf(fp));
		recall = Double.valueOf(tp) / (Double.valueOf(tp) + Double.valueOf(fn));
//		precision = Double.valueOf(tp) / Double.valueOf(p);
//		recall = Double.valueOf(tp) / Double.valueOf(mapGoldStandard.size());

		if ((precision > 0.0) && (recall > 0.0)) {
			fScore = 2.0 * ((precision * recall) / (precision + recall));
		}

		ev.setFscore(fScore);
		ev.setPrecision(precision);
		ev.setRecall(recall);

		return ev;
	}

	public Set<String> getDatasets(File file, int limit) {
		Set<String> ret = new LinkedHashSet<String>();
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			int count = 0;
			for (File source : files) {
				if (source.isFile()) {
					if (count >= limit)
						break;
					if(source.getName().endsWith(".hdt")) {
						ret.add(source.getAbsolutePath());
						count++;
					}
				}
			}
		} else {
			System.err.println(file.getName() + " is not a directory !");
		}

		return ret;
	}

}
