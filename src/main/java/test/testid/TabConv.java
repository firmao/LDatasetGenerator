package TabConverter.TabConverterTex;

import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.toMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class TabConv {

	public static void main(String args[]) throws IOException {
		int i, j;
		int row = 20;
		int column = 20;
		String array[][] = new String[row][column];
		//String file = "tabEdgardJaccard.tsv";
		String file = "tabEdgardContained.tsv";
		String [] datasets = getDatasets(file);
		
		for (int k = 0; k < datasets.length; k++) {
			array[k][0] = datasets[k];
			for (int l = 1; l < datasets.length; l++) {
				array[k][l] = score(datasets[k], datasets[l-1], file);
			}
		}
		
//		array[0][0] = "\t";
//		array[0][1] = "D1"; // Linha 1
//		array[0][2] = "D2";
//		// until array[0][19] = "D20"
//
//		array[1][0] = "D1";
//		array[1][1] = "score(D1,D1)"; // Linha 2
//		array[1][2] = "score(D1,D2)";
//		// ... until array[1][19] = "score(D1,D20)"
//
//		array[2][0] = "D2";
//		array[2][1] = "score(D2,D1)"; // Linha 3
//		array[2][2] = "score(D2,D2)";
//		// ... until array[2][19] = "score(D2,D20)"
//		// ...
//		// until array[19][19] = "score(D20,D20)"
		File f = new File("New_" + file);
		PrintWriter p = new PrintWriter(f);
		for (i = 0; i < column; i++) {
			if(i > 0) {
				p.print(array[i-1][0] + "\t");
			}
			for (j = 0; j < row; j++) {
				if((i == 0) && (j == 0)) {
					p.print("\t" + array[j][i] + "\t");
				} else {
					p.print(array[j][i] + "\t");
				}
			}
			p.println(" ");
		}
		p.println("yago\t0,02\t0,06\t0,09\t0,13\t0,16\t0,20\t0,01\t0,02\t0,02\t0,03\t0,03\t0,02\t0,03\t0,04\t0,04\t0,05\t0,06\t0,07\t0,08\t1,00");
		p.close();
		System.out.println("File: " + f.getAbsolutePath());
	}

	private static String[] getDatasets(String file) throws IOException {
		Set<String> datasets = new TreeSet<String>();
		
		BufferedReader TSVFile = new BufferedReader(new FileReader(file));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			if (dataArray[0].equalsIgnoreCase("dataset_source") || dataArray[1].equalsIgnoreCase("dataset_target")) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}	
			datasets.add(shortName(dataArray[0].toLowerCase()));
			datasets.add(shortName(dataArray[1].toLowerCase()));
			
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		String[] a = new String[datasets.size()];
		return datasets.toArray(a);
	}

	private static String shortName(String name) throws IOException {
		BufferedReader TSVFile = new BufferedReader(new FileReader("names.tsv"));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			if (dataArray[1].equalsIgnoreCase(name)) {
				TSVFile.close();
				return dataArray[0];
			}	
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		return null;
	}

	private static String score(String d1, String d2, String file) throws IOException {
		if(d1.equalsIgnoreCase(d2)) return "1,0";
		BufferedReader TSVFile = new BufferedReader(new FileReader(file));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			String ds = shortName(dataArray[0]);
			String dt = shortName(dataArray[1]);
			String score = dataArray[2];
			if((d1.equalsIgnoreCase(ds) && d2.equalsIgnoreCase(dt)) || (d1.equalsIgnoreCase(dt) && d2.equalsIgnoreCase(ds))) {
				TSVFile.close();
				return score;
			}
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		return null;
	}

	public static void main1(String[] args) throws IOException {
		String array[][] = new String[20][20];
		String[][] s = new String[200][3];

		BufferedReader TSVFile = new BufferedReader(new FileReader("tabEdgardJaccard.tsv"));

		String dataRow = TSVFile.readLine(); // Read first line.

		int line = 0;
		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			s[line][0] = dataArray[0];
			s[line][1] = dataArray[1];
			s[line][2] = dataArray[2];
			line++;
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		// System.out.println("size: " + datasets.size() + " Elements: " +
		// datasets.toString());

		for (int i = 1; i < s.length; i++) {
			for (int j = 0; j < s[i].length; j++) {
				System.out.println(s[i][j] + " ");
			}
		}

		for (int i = 1; i < s.length; i++) {
			if (i == 1) {
				for (int k = 1; k < array.length; k++) {
					array[0][0] = "\t";
					// array[0][k] = datasets[k];
				}
			}
			for (int j = 0; j < s[i].length; j++) {
				System.out.println(s[i][j] + " ");
			}
		}

		System.out.println("Printing resulting array...");
		for (int i = 0; i < 20; i++) {
			for (int j = 0; j < 20; j++) {
				System.out.print(array[j][i] + "\t");
			}
			System.out.println(" ");
		}
	}

	public static void firstTry(String[] args) throws IOException {
		Map<Map<String, String>, String> mCoef = new HashMap<Map<String, String>, String>();

		BufferedReader TSVFile = new BufferedReader(new FileReader("tabEdgardJaccard.tsv"));

		String dataRow = TSVFile.readLine(); // Read first line.

		Map<String, String> mDs = new HashMap<String, String>();
		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			mDs.put(dataArray[0], dataArray[1]);
			String score = dataArray[2];
			mCoef.put(mDs, score);

			for (String item : dataArray) {
				System.out.print(item + "  ");
			}
			System.out.println(); // Print the data line.
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();

		// End the printout with a blank line.
		System.out.println();

		System.out.println("*** My time now ****");
		for (Entry<Map<String, String>, String> entry : mCoef.entrySet()) {
			Map<String, String> mDsDt = entry.getKey();
			String score = entry.getValue();
			System.out.println(mDsDt.toString() + " " + score);
		}
	}

	public static void main3(String[] args) throws IOException {
		Map<String, LinkedHashMap<String, String>> mScore = new LinkedHashMap<String, LinkedHashMap<String, String>>();
		Set<String> datasets = new TreeSet<String>();
		
		BufferedReader TSVFile = new BufferedReader(new FileReader("tabEdgardJaccard.tsv"));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			LinkedHashMap<String, String> mPair = new LinkedHashMap<String, String>();
			if (dataArray[0].equalsIgnoreCase("dataset_source") || dataArray[1].equalsIgnoreCase("dataset_target")) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}	
			datasets.add(dataArray[0].toLowerCase());
			datasets.add(dataArray[1].toLowerCase());
			mPair.put(dataArray[1].toLowerCase(), dataArray[2]);
			mScore.put(dataArray[0].toLowerCase(), mPair);
			
			mScore.get(dataArray[0].toLowerCase()).putAll(mPair);
			
			
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();

		Map<String, LinkedHashMap<String, String>> mSorted = new LinkedHashMap<String, LinkedHashMap<String, String>>();
		mSorted.putAll(sortMapByKey(mScore));

		StringBuffer line = new StringBuffer();
		for (String dataset : datasets) {
			line.append("\t" + dataset);
		}
		line.append("\n");
		for (Entry<String, LinkedHashMap<String, String>> eScore : mSorted.entrySet()) {
			if (!eScore.getKey().equals("Dataset_Source")) {
				String dataset = eScore.getKey();
				line.append(dataset + getAllScoreSorted(mSorted, dataset) + "\n");
			}
		}
		System.out.println(line.toString());
	}

	private static Map<String, LinkedHashMap<String, String>> sortMapByKey(
			Map<String, LinkedHashMap<String, String>> mScore) {
		Map<String, LinkedHashMap<String, String>> sorted = mScore.entrySet().stream().sorted(comparingByKey())
				.collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

		return sorted;
	}

	private static String getAllScoreSorted(Map<String, LinkedHashMap<String, String>> mSorted, String dataset) {
		StringBuffer line = new StringBuffer();
		for (Entry<String, LinkedHashMap<String, String>> eScore : mSorted.entrySet()) {
			if (eScore.getKey().equals(dataset)) {
				for (String score : eScore.getValue().values()) {
					line.append("\t" + score);
				}
			}
		}

		return line.toString();
	}
}
