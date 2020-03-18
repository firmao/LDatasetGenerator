package TabConverter.TabConverterTex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TabConv {

	public static void main2(String args[]) {
		int i, j;
		int row = 3;
		int column = 3;
		String array[][] = new String[row][column];

		array[0][0] = "\t";
		array[0][1] = "D1";		// Linha 1
		array[0][2] = "D2";

		array[1][0] = "D1";		
		array[1][1] = "score(D1,D1)";   	// Linha 2
		array[1][2] = "score(D1,D2)";
		
		array[2][0] = "D2";
		array[2][1] = "score(D2,D1)";		// Linha 3
		array[2][2] = "score(D2,D2)";

		for (i = 0; i < column; i++) {
			for (j = 0; j < row; j++) {
				System.out.print(array[j][i] + "\t");
			}
			System.out.println(" ");
		}
	}

	public static void main(String[] args) throws IOException {
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
		//System.out.println("size: " + datasets.size() + " Elements: " + datasets.toString());

		for (int i = 1; i < s.length; i++) {
			for (int j = 0; j < s[i].length; j++) {
				System.out.println(s[i][j] + " ");
			}
		}
		
		for (int i = 1; i < s.length; i++) {
			if(i==1) {
				for (int k = 1; k < array.length; k++) {
					array[0][0] = "\t";
					array[0][k] = datasets[k];
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

}
