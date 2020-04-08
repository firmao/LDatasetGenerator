<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>LOD Dataset Relation</title>
<script type="text/javascript">
	//<![CDATA[

	function selectQuery ( idx ) {
		var q = null;
		if ( idx == 0 ) {
			q = "http://query.wikidata.org/sparql";
		}
		else if ( idx == 1 ) {
			q = "http://purl.org/dc/terms/date,http://crime.rkbexplorer.com/id/location,http://purl.org/dc/terms/subject";
		}
		else if ( idx == 2 ) {
			q = "Select ?s ?p ?o where {?s <http://purl.org/dc/terms/date> ?o}";
		}
		else if ( idx == 3 ) {
			q = "SELECT DISTINCT ?s ?p	WHERE { <http://mpii.de/yago/resource/The_Last_Ninja> ?s ?p }";
		}
		else if ( idx == 4 ) {
			q = "SELECT DISTINCT ?s ?p	WHERE { <http://wordnet.rkbexplorer.com/id/wordsense-mess_around-verb-1> ?s ?p }";
		}
		else if ( idx == 5 ) {
			q = "SELECT DISTINCT ?s ?p	WHERE { <http://dbpedia.org/resource/Leipzig> ?s ?p }";
		}
		else if ( idx == 6 ) {
			q = "SELECT DISTINCT ?s ?p	WHERE { <http://dbpedia.org/resource/Berlin> ?s ?p }";
		}
		else if ( idx == 7 ) {
			q = "SELECT DISTINCT ?s ?p	WHERE { <http://citeseer.rkbexplorer.com/id/resource-CS116606> ?s ?p }";
		}
		if ( q != null ) {
			document.getElementById("queryForm").query.value = q;
		}
	}


	//]]>
	</script>
</head>
<body>
	<h1><strong>The incremental LOD Dataset relation index</strong></h1>
	<p>
		<strong>About</strong>:<br />LOD Dataset relation index was developed
		with the intention to provide an index of the relations among the LOD
		Datasets.<br />We execute this task to verify which properties the
		datasets are sharing in order to see how similar those RDF datasets
		are. We compare by exact match, by the similarity of the names and by
		instance similarity.
	</p>
	<p>Just to make it clear, here we have two aims (1) Discover similar datasets according to his properties and (2) discover which datasets contain a certain set of properties.</p>
	<p>A bit more of updated and formal information about can be found here: <a href="https://www.overleaf.com/read/qjyhwtqvrwwt">ShareLatex project</a><p/>
	<p>
		<strong>Input types</strong>: <span
			style="text-decoration: underline;"><em>Dataset</em></span> or <span
			style="text-decoration: underline;"><em>Set of properties
				(URIs)</em></span> or <span style="text-decoration: underline;"><em>SPARQL
				query</em></span>.
	</p>
	<p>
		<p>The <strong>output</strong> is divided into <strong>2 types</strong>: (<strong>1</strong>) <strong>Exact matches</strong>, where is represented JSON format, containing the property and the datasets where the property was found (<strong>2</strong>) A table containing the <strong>similar matches</strong>, with properties and dataset indicating the <strong>Source and Target</strong>.&nbsp;</p>
	</p>
	<div style="margin-left:5%">
			Examples:
			<ol style="margin-top:0px">
				<li><a href="javascript:selectQuery(0);">Dataset</a></li>
				<li><a href="javascript:selectQuery(1);">Set of properties or classes
				(URIs) separated by comma ","</a></li>
				<li><a href="javascript:selectQuery(2);">SPARQL query</a></li>
			</ol>
			<p>For more examples <a href="examples.html">click here</a></p>
		</div>
	<form id="queryForm" action="matchDataset" method="get">
		<fieldset style="border: none">
			<textarea name="query" cols="100" rows="15"></textarea>
			<br/>
			Preferred Result Format:
			<input type="radio" name="output" value="json" checked="checked"/> JSON
			<br/>
			<input type="radio" name="opt" value="dataset"/> Search by the dataset name
			<br/>
			<input type="radio" name="opt" value="properties"/> Search by property/class
			<br/>
			<input type="radio" name="opt" value="sparql"/> Search by SPARQL query
			<br/>
	 		<input type="submit" value="Execute" />
		</fieldset>
	</form>
	<p>Datasets already indexed:
	<ul>
		<li>All 539 from LOD cloud:&nbsp;<a
			href="https://github.com/firmao/wimuT/blob/master/Endpoints_numtriples_lodcloud.csv">https://github.com/firmao/wimuT/blob/master/Endpoints_numtriples_lodcloud.csv</a></li>
		<li>915 datasets (HDT files) randomly selected from&nbsp;<a
			href="http://lodlaundromat.org/" rel="nofollow">LOD Laundromat</a>.
		</li>
		<p>Example of some&nbsp;<a title="Famous datasets" href="https://github.com/firmao/LODDatasetRelationsWeb/blob/master/famousDataset.csv" target="_blank" rel="noopener">Famous datasets are already indexed (click here).</a></p>
		<li>Takes 88 hours to generate the LOD dataset relation index.</li>
	</ul>
	<p>
		Click the <a title="Add Dataset" href="uploadHDT.jsp"> <span
			style="background-color: #2b2301; color: #fff; display: inline-block; padding: 3px 10px; font-weight: bold; border-radius: 5px;">Add
				Dataset</span>
		</a> button to add your own dataset to our index.
	</p>
	<p>
		Usability questionnaire:&nbsp;<a
			href="https://forms.gle/stjHrakr7RBSCUH19" target="_blank"
			rel="noopener">https://forms.gle/stjHrakr7RBSCUH19</a>
	</p>
	<p>
		GitHub repository:&nbsp;<a
			href="https://github.com/firmao/LDatasetGenerator">https://github.com/firmao/LDatasetGenerator</a>
	</p>
	<p>Some <a href="statistics.html" target="_blank" rel="noopener">statistics(click)</a></p>
	<br>
	<p>WE WOULD LIKE TO REMIND THAT THIS IS ONLY A PROOF OF CONCEPT,
		NOT THE FINAL VERSION. THE AIM IS TO IMPROVE THIS WORK according to
		your feedback.</p>
	<p>
		Contact:<br />Andr&eacute; Valdestilhas<br />email: <a
			href="mailto:fimao@gmail.com">fimao@gmail.com</a>
	</p>
</body>
</html>