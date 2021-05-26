<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
<h1>Generic Sparql Query on HDT files or another RDF format</h1>
<form action="sparqlservlet">
  <label for="fname">Dataset URL:</label>
  <input type="text" id="dataset" name="dataset" value="http://141.57.11.86:8082/dirHDTLaundromat/decompressed/76/7682278f9dd608f09c4c073a915e58a1/7682278f9dd608f09c4c073a915e58a1.hdt"><br><br>
  <label for="lname">SPARQL query:</label>
  <textarea id="query" name="query" rows="4" cols="50">select distinct ?Concept where {[] a ?Concept} LIMIT 100</textarea><br><br>
  <input type="submit" value="Submit">
  <br/><a href="http://141.57.11.86:8083/dSimilarity/genericEndPointQuery.jsp">Back to Generic Query Endpoint.</a>
  <br/><a href="http://w3id.org/relod">Go to ReLOD, the Dataset
				Similarity index</a>
</form>
</body>
</html>