package test.testid;

import org.apache.commons.text.similarity.HammingDistance;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.LevenshteinDetailedDistance;
import org.apache.commons.text.similarity.LevenshteinResults;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase2;

public class JaccardFilter extends FunctionBase2 
{  
     public NodeValue exec(NodeValue value1, NodeValue value2){
         //int i = StringUtils.getLevenshteinDistance(value1.asString(), value2.asString()); 
    	 //return NodeValue.makeInteger(i); 
    	 JaccardSimilarity jacSim = new JaccardSimilarity();
    	 double dSim = jacSim.apply(value1.asString(), value2.asString());
    	 return NodeValue.makeDouble(dSim);
     }
     
     public static void main(String args[]) {
    	 JaccardSimilarity jacSim = new JaccardSimilarity();
    	 JaroWinklerDistance jaro = new JaroWinklerDistance();
    	 LevenshteinDetailedDistance leve = new LevenshteinDetailedDistance();
    	 HammingDistance ham = new HammingDistance();
    	 
    	 String s1 = "<http://dbpedia.org/ontology/death>";
    	 String s2 = "<http://dbpedia.org/ontology/birth>";
    	 
    	 Long start = System.currentTimeMillis();
    	 double dSim = jacSim.apply(s1, s2);
    	 Long total = System.currentTimeMillis() - start;
 		 System.out.println("Jaccard: " + dSim + "\nTime: " + total + " ms.");
    	 
 		 start = System.currentTimeMillis();
	   	 dSim = jaro.apply(s1, s2);
	   	 total = System.currentTimeMillis() - start;
		 System.out.println("Jaro: " + dSim + "\nTime: " + total + " ms.");
		 
		 start = System.currentTimeMillis();
	   	 dSim = AndreMFKC_Parallel.sim(s1, s2, 100);
	   	 total = System.currentTimeMillis() - start;
		 System.out.println("Andre MFKC: " + dSim + "\nTime: " + total + " ms.");
		 
		 start = System.currentTimeMillis();
	   	 dSim = ham.apply(s1, s2);
	   	 total = System.currentTimeMillis() - start;
		 System.out.println("Hammilton: " + dSim + "\nTime: " + total + " ms.");
		 
		 start = System.currentTimeMillis();
		 LevenshteinResults dSimLeve = leve.apply(s1, s2);
	   	 total = System.currentTimeMillis() - start;
		 System.out.println("Levenstein: " + dSimLeve.toString() + "\nTime: " + total + " ms.");
     }
}
