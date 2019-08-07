package test.testid;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVReader;

public class CSV2JSONEntities {

	static int countSkiped = 0;
	public static void main(String[] args) throws IOException {
		final String DIR_CSV = "cities_csv_en";
		File files [] = (new File(DIR_CSV)).listFiles();
		for (File file : files) {
			File fDir = new File("outJsonFAMER7/" + file.getName().replaceAll(".csv", "") + "/");
			fDir.mkdirs();
			
			//Map<String, Map<String, String>> mEntities = parseCSV(file);
			Map<String, Map<String, String>> mEntities = parseCSVOpen(file);
			
			printFiles(mEntities, fDir);
			System.out.println("file: " + file.getAbsolutePath());
			System.out.println("length: " + file.length());
			System.out.println("SkipedLines: " + countSkiped);
		}
		
	}

	private static void printFiles(Map<String, Map<String, String>> mEntities, File fDir) throws IOException {
		for (Entry<String, Map<String, String>> entry : mEntities.entrySet()) {
			String name = null;
			if(entry.getKey().contains("http")){
				name = Util.getURLName(entry.getKey());
			}else {
				name = entry.getKey();
			}
			String fName = fDir +"/" + name + ".json";
			File f = new File(fName);
			if(!f.exists()) {
				f.createNewFile();
			}
			PrintWriter writer = new PrintWriter(fName, "UTF-8");
			writer.println("{");
			StringBuffer sbLines = new StringBuffer();
			for (Entry<String, String> atts : entry.getValue().entrySet()) {
				String key = atts.getKey();
				String value = atts.getValue();
				sbLines.append("\""+key + "\": " + "\"" + value + "\",\n");					
			}
			String content = sbLines.toString().substring(0, sbLines.length()-2);
			writer.println(content);
			writer.println("}");
			writer.close();
		}
		
	}

	private static Map<String, Map<String, String>> parseCSVOpen(File file) {
		Map<String, Map<String, String>> mEntities = new LinkedHashMap<String, Map<String, String>>();
		CSVReader reader = null;
        try
        {
            //Get the CSVReader instance with specifying the delimiter to be used
//            if(file.getName().contains("geo")) {
//            	reader = new CSVReader(new FileReader(file.getAbsolutePath()),';');
//            } else {	
            	reader = new CSVReader(new FileReader(file.getAbsolutePath()),',');
//            }
            String [] sValues;
            String [] sNames = null;
    		int count = 0;
            while ((sValues = reader.readNext()) != null)
            {
            	if(count == 0) {
    				sNames = sValues;
    				count++;
    				continue;
    			}
    
    			if(sNames.length != sValues.length) {
    				countSkiped++;
    				continue;
    			}
    			String name = sValues[0];
    			for(int i=1; i<sValues.length;i++) {
    				String attName = sNames[i];
    				String attValue = sValues[i];
    				if(mEntities.get(name) != null) {
    					mEntities.get(name).put(attName, attValue);
    				} else {
    					Map<String, String> mapAttributes = new LinkedHashMap<String, String>();
    					mapAttributes.put(attName,attValue);
    					mEntities.put(name, mapAttributes);
    				}
    			}
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
		
		return mEntities;
	}
	
	private static Map<String, Map<String, String>> parseCSV(File file) throws IOException {
		List<String> fLines = Files.readAllLines(Paths.get(file.getAbsolutePath()));
		Map<String, Map<String, String>> mEntities = new LinkedHashMap<String, Map<String, String>>();
		String [] sNames = null;
		int count = 0;
		for (String sLine : fLines) {
			String line = sLine.replaceAll("\",\"", "\t");
			line = line.replaceAll("\"", "");
			line = line.replaceAll(", ", ". ");
			line = line.replaceAll(",", "\t");
			
			String [] sValues = line.split("\t");
			//String [] sValues = line.split("\t");
			if(count == 0) {
				sNames = sValues;
				count++;
				continue;
			}
			String name = sValues[0];
			//String attributes = line.replaceAll(name + ",", "");
			if(sNames.length != sValues.length) {
				countSkiped++;
				continue;
			}
			for(int i=1; i<sValues.length;i++) {
				String attName = sNames[i];
				String attValue = sValues[i];
				if(mEntities.get(name) != null) {
					mEntities.get(name).put(attName, attValue);
				} else {
					Map<String, String> mapAttributes = new LinkedHashMap<String, String>();
					mapAttributes.put(attName,attValue);
					mEntities.put(name, mapAttributes);
				}
			}
		}
		return mEntities;
	}

}
