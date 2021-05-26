<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
	<h1>Which properties and classes the HDT/RDF files have in common?</h1>
	<form action="sparqlservlet">
		<label for="fname">Datasets</label>
		<textarea id="datasets" name="datasets" rows="4" cols="80">http://141.57.11.86:8082/dirHDTLaundromat/decompressed/76/7682278f9dd608f09c4c073a915e58a1/7682278f9dd608f09c4c073a915e58a1.hdt, http://141.57.11.86:8082/dirHDTLaundromat/decompressed/d8/d8f64e50a076962375bb2e4f12a70f03/d8f64e50a076962375bb2e4f12a70f03.hdt</textarea>
		<br> <br> <input type="submit" value="Submit">
	</form>
	<br>
	<h3>Or</h3>
	<ol style="margin-top: 0px">
	<li><a href="http://141.57.11.86:8083/dSimilarity/index.jsp">Compare SPARQL endpoints</a></li>
		<li><a href="genericEndPointQuery.jsp">Query your prefered
				dataset</a></li>
		<li><a href="http://w3id.org/relod">Go to ReLOD, the Dataset
				Similarity index</a></li>
	</ol>
</body>
</html>