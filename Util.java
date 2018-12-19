package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Util {

	/*
	 * Save the resources in a file
	 */
	public static void writeFile(Set<String> resources, File fResources) throws IOException {
		if(!fResources.exists()) fResources.createNewFile();
		PrintWriter writer = new PrintWriter(fResources.getName(), "UTF-8");
		for (String resource : resources) {
			writer.println(resource.replaceAll("\n", ""));
		}
		writer.close();
	}

	public static void writeFile(String resource, Map<String, String> mapPropValue) throws FileNotFoundException, UnsupportedEncodingException {
		String s[] = resource.split("/");
		String fileName = "out/" + s[2] + "_" + s[s.length - 1] + ".tsv";
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("#---Properties and values from: " + resource);
		writer.println("Property\tValue");
		for (Entry<String, String> entry : mapPropValue.entrySet()) {
			String prop = entry.getKey();
			String value = entry.getValue();
			writer.println(prop + "\t" + value);
		}
		writer.close();
	}
	


}
