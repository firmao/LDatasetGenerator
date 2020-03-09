package main.java.test.testid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ResultsFromLDFragments {

	public static void main(final String[] args) throws IOException, InterruptedException {
		String fragmentsHost = "https://api.krr.triply.cc/datasets/krr/lod-a-lot/fragments";
        String sparql = "Select * where {?s ?p ?o} limit 10";
		String result = getFromLDFragments(fragmentsHost, sparql);
		System.out.println(result);
    }
	
	public static String getFromLDFragments(String fragmentsHost, String sparql) throws IOException, InterruptedException {
		List<String> commands = new ArrayList<String>();
        commands.add("/usr/local/bin/node");
        //Add arguments
        commands.add("Client.js/bin/ldf-client");
        commands.add(fragmentsHost);
        //commands.add("Client.js/queries/artists-york.sparql");
        commands.add(sparql);
        //System.out.println(commands);

        //Run macro on target
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        //Read output
        StringBuilder out = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null, previous = null;
        while ((line = br.readLine()) != null)
            if (!line.equals(previous)) {
                previous = line;
                out.append(line).append('\n');
                //System.out.println(line);
            }

        //Check result
        if (process.waitFor() != 0) {
            System.err.println("ERROR AT ResultsFromLDFragments.getFromLDFragments()");
        }
		
		return out.toString();
	}
}
