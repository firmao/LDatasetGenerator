package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.jena.lang.csv.CSV2RDF;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.TableSchema;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;

public class Experiment {
	private String ds, dt, goldStandard;
	private Set<String> manyDt;

	public Set<String> getManyDt() {
		return manyDt;
	}

	public void setManyDt(Set<String> pManyDt) {
		Set<String> nManyDt = new LinkedHashSet<String>();
		for (String d : pManyDt) {
			if (!d.endsWith(".hdt")) {
				try {
					nManyDt.add(convertHDT(d));
				} catch (Exception e) {
					System.err.println("Problem converting file to HDT: " + d);
				}
			}else {
				nManyDt.add(d);
			}
		}
		
		this.manyDt = nManyDt;
	}

	public String getDs() {
		return ds;
	}

	public void setDs(String pDs) {
		if (!pDs.endsWith(".hdt")) {
			try {
				this.ds = convertHDT(pDs);
				return;
			} catch (Exception e) {
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
				return;
			} catch (Exception e) {
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

	public String convertHDT(String pD) throws Exception {
		// Configuration variables
		String baseURI = "http://example.com/mydataset";
		String hdtOutput = pD.replaceAll(pD.substring(pD.lastIndexOf(".")), ".hdt");

		String sFileNT = null;
		String sFileCSV = null;
		File fHdt = new File(hdtOutput);
		if (fHdt.exists()) {
			return hdtOutput;
		}

		if (pD.endsWith(".csv")) {
			sFileNT = convertCSV2NT(pD);
		}
		
		if(pD.endsWith(".json")) {
			sFileCSV = convertJson2CSV(pD);
			sFileNT = convertCSV2NT(sFileCSV);
		}

		// Create HDT from RDF file
		HDT hdt = HDTManager.generateHDT(sFileNT, // Input RDF File
				baseURI, // Base URI
				RDFNotation.guess(sFileNT), // Input Type
				new HDTSpecification(), // HDT Options
				null // Progress Listener
		);

		// OPTIONAL: Add additional domain-specific properties to the header:
		// Header header = hdt.getHeader();
		// header.insert("myResource1", "property" , "value");

		// Save generated HDT to a file
		hdt.saveToHDT(hdtOutput, null);
		
//		File fCSV = new File(sFileCSV);
//		fCSV.delete();
//		File fNT = new File(sFileNT);
//		fNT.delete();
		
		return hdtOutput;
	}

	/*mainly for web tables
	 * 
	 */
	private String convertJson2CSV(String file) throws FileNotFoundException, UnsupportedEncodingException {
		String nameCsv = file.replaceAll(".json", ".csv");
		PrintWriter writer = new PrintWriter(nameCsv, "UTF-8");
		JsonTableParser jsonParser = new JsonTableParser();
		Table t = jsonParser.parseTable(new File(file));
		TableSchema schema = t.getSchema();

		StringBuffer lines = new StringBuffer();
		lines.append(schema.get(0).getHeader());
		for (int i = 1; i < schema.getSize(); i++) {
			lines.append("," + schema.get(i).getHeader());
		}

		lines.append("\n");
		// print all rows including their row number
		// (with a maximum column width of 20 characters):
		for (TableRow r : t.getRows()) {
			lines.append(r.getValueArray()[0]);
			for (int i=1; i < r.getValueArray().length; i++) {
				lines.append("," + r.getValueArray()[i]);
			}
			lines.append("\n");
		}
		writer.println(lines.toString());
		writer.close();
		return nameCsv;
	}

	private String convertCSV2NT(String pDs) throws IOException {
		String fileNameNT = pDs.replaceAll(".csv", ".nt");
		File fHdt = new File(fileNameNT);
		if (fHdt.exists()) {
			return fileNameNT;
		}

		CSV2RDF.init();
		// load through manager:
		Model m = RDFDataMgr.loadModel(pDs);
		// classic way to load:
//	    Model m = ModelFactory.createDefaultModel();
//	    try (InputStream in = JenaCSVTest.class.getResourceAsStream("/test.csv")) {
//	        m.read(in, "http://example.com", "csv");
//	    }
		m.setNsPrefix("test", "http://example.com#");
		FileWriter out = new FileWriter(fileNameNT);
		m.write(out, "nt");
		return fileNameNT;
	}

	public void execute() throws IOException {
		final Map<String, String> mapMatches = new LinkedHashMap<String, String>();
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();

		if (getManyDt() != null) {
			Set<String> dts = getManyDt();
			for (String dt : dts) {
			//dts.parallelStream().forEach(dt -> {
				try {
					mapPropsDs.put(dt, PropertyMatching.schemaMatching(getDs(), dt));
				} catch (IOException e) {
					e.printStackTrace();
				}
			//});
			}
			writeDsPropsMatched(mapPropsDs);
			evaluateGoldDir(mapPropsDs);
		} else {
			mapMatches.putAll(PropertyMatching.schemaMatching(getDs(), getDt()));
			String fileName = "MatchingMap.tsv";
			writeFile(mapMatches, fileName, getDs(), getDt());

			evaluate(mapMatches);
		}
	}

	public void writeDsPropsMatched(Map<String, Map<String, String>> mapPropsDs)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("RelationDsPropsMatched2.tsv", "UTF-8");
		writer.println("DatasetSource\tDatasetTarget\t#Matches\tPairs");
		for (Entry<String, Map<String, String>> dsProp : mapPropsDs.entrySet()) {
			String dt = dsProp.getKey();
			Map<String, String> matches = dsProp.getValue();
			int excludeCases = 0;
			for (Entry<String, String> match : matches.entrySet()) {
				if (match.getValue() == null) {
					excludeCases++;
				}
			}
			writer.println(getDs() + "\t" + dt + "\t" + (matches.size() - excludeCases) + "\t" + matches);
		}
		writer.close();
	}

	public void writeFile(Map<String, String> mapMatches, String fileName, String dsS, String dsT)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("#Dataset Source:" + dsS + "\t" + "#Dataset Target:" + dsT);
		for (Entry<String, String> entry : mapMatches.entrySet()) {
			writer.println(entry.getKey() + "\t" + entry.getValue());
		}
		writer.close();
	}

	public void evaluate(Map<String, String> mapMatches) throws IOException {
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
	
	public void evaluateGoldDir(Map<String, Map<String, String>> mapMatches) throws IOException {
		final Map<String, String> mapGoldStandard = new LinkedHashMap<String, String>();
		mapGoldStandard.putAll(convFileToMap(getGoldStandard()));
		Evaluation ev = compare1ToMany(mapMatches, mapGoldStandard);
		PrintWriter writer = new PrintWriter(getGoldStandard() + "_eval.txt", "UTF-8");
		writer.println(mapMatches);
		writer.println("Precision: " + ev.getPrecision());
		writer.println("Recall: " + ev.getRecall());
		writer.println("F-Score: " + ev.getFscore());
		writer.close();
	}

	private Evaluation compare1ToMany(Map<String, Map<String, String>> pMapMatches, Map<String, String> mapGoldStandard){
		
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
		
		//TODO: Try to relate each webTable result with a web table 
		//goldStandard, because they have the same structure
		for (String dt : pMapMatches.keySet()) {
			Map<String, String> mapMatches = pMapMatches.get(dt); 
			for (Entry<String, String> entry : mapGoldStandard.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				//if (mapMatches.containsKey(key)) {
				if (mapMatches.containsKey(getMostSimilar(mapMatches, key, value))) {
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

	private String getMostSimilar(Map<String, String> mapMatches, String pKey, String pValue) {
		for (Entry<String, String> entry : mapMatches.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			
			if(key == null || value == null) return pKey;
			
			if(key.contains(pKey)) {
				return key;
			}
			if(value.contains(pKey)) {
				return value;
			}
			
			if(key.contains(pValue)) {
				return key;
			}
			if(value.contains(pValue)) {
				return value;
			}
		}
		return pKey;
	}

	private static Map<String, String> convFileToMap(String fMap) throws IOException {
		final Map<String, String> mapGoldStandard = new LinkedHashMap<String, String>();
		File fileMap = new File(fMap);
		
		if(fileMap.isDirectory()) {
			mapGoldStandard.putAll(obtainGoldWebTablesCSV(fileMap));
		} else {
			List<String> lstLines = FileUtils.readLines(fileMap, "UTF-8");
			for (String line : lstLines) {
				String s[] = line.split("\t");
				if (s.length < 2)
					continue;
	
				mapGoldStandard.put(s[0], s[1]);
			}
		}
		return mapGoldStandard;
	}

	private static Map<String, String> obtainGoldWebTablesCSV(File pFileMap) throws IOException {
		final Map<String, String> mapGoldStandard = new LinkedHashMap<String, String>();
		String sExt[] = {"csv"};
		Collection<File> files = FileUtils.listFiles(pFileMap, sExt, true);
		for (File file : files) {
			List<String> lstLines = FileUtils.readLines(file, "UTF-8");
			for (String line : lstLines) {
				String s[] = line.split(",");
				if (s.length < 2)
					continue;
	
				mapGoldStandard.put(s[0].trim(), s[1].trim());
			}
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

	public Set<String> getDatasets(File file, int limit) throws Exception {
		Set<String> ret = new LinkedHashSet<String>();
		if (file.isDirectory()) {
			String sExt[] = {"json","csv","tar","tar.gz","hdt"};
			Collection<File> files = FileUtils.listFiles(file, sExt, true);
			//File[] files = file.listFiles();
			int count = 0;
			for (File source : files) {
				if (source.isFile()) {
					if (count >= limit)
						break;
					if (source.getName().endsWith(".hdt")) {
						ret.add(source.getAbsolutePath());
						count++;
					}
					if(source.getName().endsWith(".tar.gz")) {
						ret.addAll(convertZip2HDT(source));
						count++;
					}
					if(source.getName().endsWith(".tar")) {
						ret.addAll(convertZip2HDT(source));
						count++;
					}
					if(source.getName().endsWith(".json")) {
						ret.add(convertHDT(source.getAbsolutePath()));
						count++;
					}
					if(source.getName().endsWith(".csv")) {
						ret.add(convertHDT(source.getAbsolutePath()));
						count++;
					}
				}
			}
		} else {
			System.err.println(file.getName() + " is not a directory !");
		}
		return ret;
	}

	private Set<String> convertZip2HDT(File source) throws Exception {
		Set<String> ret = new LinkedHashSet<String>();
		File fUncompressed = Util.unconpress(source);
		if (fUncompressed.isDirectory()) {
			ret.addAll(getDatasets(fUncompressed, 9999999));	
		}else {
			if(fUncompressed.getName().endsWith(".hdt")) {
				ret.add(fUncompressed.getAbsolutePath());
			}else {
				if(fUncompressed.getName().endsWith(".tar")) {
					ret.addAll(convertZip2HDT(fUncompressed));
				}else {
					ret.add(convertHDT(fUncompressed.getAbsolutePath()));
				}
			}
		}
		return ret;
	}
}
