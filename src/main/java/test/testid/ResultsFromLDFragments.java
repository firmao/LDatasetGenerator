package main.java.test.testid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ResultsFromLDFragments {

	public static void main(final String[] args) throws IOException, InterruptedException {
		String nodeDir = "/usr/local/bin/node";
		String ldfClient = "Client.js/bin/ldf-client";
		String fragmentsHost = "https://api.krr.triply.cc/datasets/krr/lod-a-lot/fragments";
        String sparql = "Select * where {?s ?p ?o} limit 10";
		String result = getFromLDFragments(fragmentsHost, sparql, nodeDir, ldfClient);
		System.out.println(result);
    }
	
	
	/**
	 * @param fragmentsHost The host containing the LDFragments, i.g., https://api.krr.triply.cc/datasets/krr/lod-a-lot/fragments
	 * @param sparql Can be a text containing the sparql query or a path to a file.sparql containing the query
	 * @param nodeDir The path where node.js are installed, i.g. /usr/local/bin/node
	 * @param ldfClient The path to the LDFragments client, i.g. Client.js/bin/ldf-client
	 * @return The results from the sparql query in JSON format
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static String getFromLDFragments(String fragmentsHost, String sparql, String nodeDir, String ldfClient) throws IOException, InterruptedException {
		List<String> commands = new ArrayList<String>();
        commands.add(nodeDir);
        //Add arguments
        commands.add(ldfClient);
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
