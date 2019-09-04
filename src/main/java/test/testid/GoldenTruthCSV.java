package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.text.similarity.JaroWinklerDistance;

import au.com.bytecode.opencsv.CSVReader;

import java.util.Set;

public class GoldenTruthCSV {
	public static final Map<String, String> mAlreadyCompared = new LinkedHashMap<String, String>();

	public static void main(String[] args) throws Exception {
		System.out.println("SERIAL !!!");
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		Set<String> ds = new LinkedHashSet<String>();
		ds.add("citiesTest/d1.csv");
		ds.add("citiesTest/d2.csv");
		ds.add("citiesTest/d3.csv");
		
//		ds.add("cities_csv_en_norm1/dbpedia.csv");
//		ds.add("cities_csv_en_norm1/eea.europa.eu_most-populated-cities.csv");
//		ds.add("cities_csv_en_norm1/geonames-all-cities-with-a-population-1000.csv");
//		ds.add("cities_csv_en_norm1/lod.openlinksw.com_25sources.csv");
//		ds.add("cities_csv_en_norm1/mahnhein_searchJoins.webdatacommons.org.csv");
//		ds.add("cities_csv_en_norm1/wikidata.csv");
//		ds.add("cities_csv_en_norm1/yago.csv");
		Set<String> dt = new LinkedHashSet<String>();
		dt.addAll(ds);
		long totalComparisons = ds.size() * dt.size();
		System.out.println("Total datasets: " + ds.size());
		int count = 0;
		Set<Match> setMatches = new LinkedHashSet<Match>();
		PrintWriter writer = new PrintWriter("matchScores_s.tsv", "UTF-8");
		writer.println("DatasetSource\tDatasetTarget\tScore");
		for (String source : ds) {
			for (String target : dt) {
				if (source.equals(target))
					continue;
				if (alreadyCompared(source, target))
					continue;

				System.out.println("Starting dataset comparison: " + count + " from " + totalComparisons);
				System.out.println(source + "---" + target);
				count++;
				double dScore = getScore(source, target);
				writer.println(source + "\t" + target + "\t" + dScore);
				mAlreadyCompared.put(source, target);
				mAlreadyCompared.put(target, source);
			}
		}
		writer.close();
		// printMatches(setMatches, 0.8);
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}

	private static void printMatches(Set<Match> setMatches, double threshold)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("matches.tsv", "UTF-8");
		writer.println("DatasetSource\tSource\tDatasetTarget\tTarget\tScore");
		for (Match match : setMatches) {
			String dSource = null;
			String dTarget = null;
			String source = null;
			String target = null;
			for (Entry<String, String> entry : match.getSource().entrySet()) {
				dSource = entry.getKey();
				source = entry.getValue();
			}
			for (Entry<String, String> entry : match.getTarget().entrySet()) {
				dTarget = entry.getKey();
				target = entry.getValue();
			}
			writer.println(dSource + "\t" + source + "\t" + dTarget + "\t" + target + "\t" + match.getScore());
		}
		writer.close();
	}

	private static double getScore(String source, String target)
			throws FileNotFoundException, UnsupportedEncodingException {
		double scoreTotal = 0.0;
		Set<Double> scores = new LinkedHashSet<Double>();

		JaroWinklerDistance jaro = new JaroWinklerDistance();
		Set<String> linesSource = getLines(source);
		Set<String> linesTarget = getLines(target);
		File fSource = new File(source);
		File fTarget = new File(target);
		String fileName = fSource.getName().replaceAll(".csv", "") + "---" + fTarget.getName().replaceAll(".csv", "");
		PrintWriter writer = new PrintWriter("Entity_MatchScore_" + fileName + ".tsv", "UTF-8");
		writer.println("EntitySource\tEntityTarget\tEntityScore");
		int countEntityErrors = 0;
		for (String lSource : linesSource) {
			String[] sSource = lSource.split(",");
			for (String lTarget : linesTarget) {
				String[] sTarget = lTarget.split(",");
				Set<Double> scoresEntity = new LinkedHashSet<Double>();
				double scoreEntity = 0.0;
				for (int i = 0; i < sSource.length; i++) {
					double dSim = 0.0;
					try {
						String s = sSource[i].trim();
						String t = sTarget[i].trim();
						try {
							double ds = Double.parseDouble(s);
							double dt = Double.parseDouble(t);
							if(ds == dt) dSim = 1.0;
						} catch(Exception ex) {
							dSim = jaro.apply(s, t);
						}
//						if(Util.isNumeric(sSource[i]) && isNumber(sTarget[i])) {
//							dSim = simNumber(sSource[i], sTarget[i]);
//						} else {
//							dSim = jaro.apply(sSource[i], sTarget[i]);
//						}
						scoresEntity.add(dSim);
					} catch (Exception e) {
						countEntityErrors++;
						continue;
					}
				}
				for (Double sc : scoresEntity) {
					scoreEntity += sc;
				}
				scoreEntity = scoreEntity / scoresEntity.size();
				if(scoreEntity > 0.8) {
					writer.println(sSource[0] + "\t" + sTarget[0] + "\t" + scoreEntity);
				}
				scores.add(scoreEntity);
			}
		}
		writer.close();
		for (Double sc : scores) {
			scoreTotal += sc;
		}
		scoreTotal = scoreTotal / scores.size();
		System.out.println("#Entity Errors: " + countEntityErrors);
		return scoreTotal;
	}

	private static Set<String> getLines(String file) {
		Set<String> ret = new LinkedHashSet<String>();
		try {
			List<String> lstLines = FileUtils.readLines(new File(file), "UTF-8");
			int count = 0;
			for (String line : lstLines) {
				if(count > 0) {
					ret.add(line);
				}
				count++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

	private static boolean alreadyCompared(String source, String target) {
		if (mAlreadyCompared.get(source) != null) {
			if (mAlreadyCompared.get(source).equals(target)) {
				return true;
			}
		}

		return false;
	}

}
