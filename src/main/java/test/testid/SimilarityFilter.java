package test.testid;

import org.apache.commons.text.similarity.HammingDistance;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.LevenshteinDetailedDistance;
import org.apache.commons.text.similarity.LevenshteinResults;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase2;

public class SimilarityFilter extends FunctionBase2 
{  
     public NodeValue exec(NodeValue value1, NodeValue value2){
    	 JaroWinklerDistance sim = new JaroWinklerDistance();
    	 double dSim = sim.apply(value1.asString(), value2.asString());
    	 return NodeValue.makeDouble(dSim);
     }
     
     public static void main(String args[]) {
    	 JaccardSimilarity jacSim = new JaccardSimilarity();
    	 JaroWinklerDistance jaro = new JaroWinklerDistance();
    	 LevenshteinDetailedDistance leve = new LevenshteinDetailedDistance();
    	 HammingDistance ham = new HammingDistance();
    	 
    	 String s1 = "Kartoffelsalat";
    	 String s2 = "Runkelrüben";
    	 System.out.println("s1: " + s1);
    	 System.out.println("s2: " + s2);
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
		 
	   	 try{
	   		 start = System.currentTimeMillis();
	   		 dSim = ham.apply(s1, s2);
	   		 total = System.currentTimeMillis() - start;
			 System.out.println("Hammilton: " + dSim + "\nTime: " + total + " ms.");
	   	 }catch(Exception e) {
	   		 System.err.println("Fail with Hammilton: " + e.getMessage());
	   	 }
	   	 
		 start = System.currentTimeMillis();
		 LevenshteinResults dSimLeve = leve.apply(s1, s2);
	   	 total = System.currentTimeMillis() - start;
		 System.out.println("Levenstein: " + dSimLeve.toString() + "\nTime: " + total + " ms.");
     }
}
