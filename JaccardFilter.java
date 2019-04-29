package test.testid;

import org.apache.commons.text.similarity.JaccardSimilarity;
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
}
