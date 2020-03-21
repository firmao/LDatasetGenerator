package TabConverter.TabConverterTex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

public class TabConv {

	public static void main1(String args[]) throws IOException {
		int i, j;
		int row = 20;
		int column = 20;
		String array[][] = new String[row][column];
		// String file = "tabEdgardJaccard.tsv";
		String file = "tabEdgardContained.tsv";
		String[] datasets = getDatasets(file);

		for (int k = 0; k < datasets.length; k++) {
			array[k][0] = datasets[k];
			for (int l = 1; l < datasets.length; l++) {
				array[k][l] = score(datasets[k], datasets[l - 1], file);
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
			if (i > 0) {
				p.print(array[i - 1][0] + "\t");
			}
			for (j = 0; j < row; j++) {
				if ((i == 0) && (j == 0)) {
					p.print("\t" + array[j][i] + "\t");
				} else {
					p.print(array[j][i] + "\t");
				}
			}
			p.println(" ");
		}
		p.println(
				"yago\t0,02\t0,06\t0,09\t0,13\t0,16\t0,20\t0,01\t0,02\t0,02\t0,03\t0,03\t0,02\t0,03\t0,04\t0,04\t0,05\t0,06\t0,07\t0,08\t1,00");
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
		if (d1.equalsIgnoreCase(d2))
			return "1,0";
		BufferedReader TSVFile = new BufferedReader(new FileReader(file));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			String ds = shortName(dataArray[0]);
			String dt = shortName(dataArray[1]);
			String score = dataArray[2];
			if ((d1.equalsIgnoreCase(ds) && d2.equalsIgnoreCase(dt))
					|| (d1.equalsIgnoreCase(dt) && d2.equalsIgnoreCase(ds))) {
				TSVFile.close();
				return score;
			}
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		return null;
	}

	public static void main(String args[]) throws IOException {
		String fileName = "tabEdgardJaccard.tsv";
		StringBuffer sb = new StringBuffer();
		PrintWriter p = new PrintWriter("cyto.json");
		sb.append("{\n\"nodes\": [\n");
		
		String[] datasets = getDatasets(fileName);
		
		for (int i = 0; i < datasets.length; i++) {
			sb.append("{\n" + "\"data\": {\"id\": \"" + datasets[i] + "\", \"label\": \"" + datasets[i] + "\"}\n" + "},");
		}
		String s = sb.substring(0, sb.length() - 1);
		sb = null; sb = new StringBuffer();
		sb.append(s);
		
		sb.append("],\n" + "            \"edges\": [");
		
		BufferedReader TSVFile = new BufferedReader(new FileReader(fileName));
		String dataRow = TSVFile.readLine(); // Read first line.
		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			String ds = shortName(dataArray[0]);
			if(ds == null) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}
			String dt = shortName(dataArray[1]);
			String score = dataArray[2];
			sb.append("{\n\"data\": {\n" + 
					"            \"id\": \""+(ds + dt)+"\",\n" + 
					"                   \"source\": \"" + ds + "\",\n" + 
					"                   \"target\": \""+dt+"\"\n" + 
					"            }\n" + 
					"            },");
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		TSVFile.close();
		s = sb.substring(0, sb.length() - 1);
		sb = null; sb = new StringBuffer();
		sb.append(s);
		
		sb.append("]    \n" + 
				"    }");
		p.println(sb.toString());
		p.close();
		System.out.println("DONE");
	}
}
