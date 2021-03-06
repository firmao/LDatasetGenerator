<%@page import="web.servlet.matching.DatabaseMain"%>
<%@page import="java.util.Map.Entry"%>
<%@page import="java.util.LinkedHashMap"%>
<%@page import="java.util.Map"%>
<%@page import="java.util.LinkedHashSet"%>
<%@page import="java.util.Set"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>

<head>
	<title>ReLOD demo dataset visual similarity relations</title>

	<meta name="viewport" content="width=device-width, user-scalable=no, initial-scale=1, maximum-scale=1">

	<script src="https://cdnjs.cloudflare.com/ajax/libs/bluebird/3.5.2/bluebird.min.js"></script>
	<script src="https://cdnjs.cloudflare.com/ajax/libs/fetch/2.0.3/fetch.min.js"></script>
	<script src="https://unpkg.com/cytoscape/dist/cytoscape.min.js"></script>
	<script src="https://unpkg.com/weaverjs@1.2.0/dist/weaver.min.js"></script>
	<script src="https://unpkg.com/webcola/WebCola/cola.min.js"></script>
	<script src="cytoscape-cola.js"></script>

	<style>
		body {
			font-family: helvetica;
			font-size: 14px;
		}

		#cy {
			width: 100%;
			height: 100%;
			position: absolute;
			left: 0;
			top: 0;
			z-index: 999;
		}

		h1 {
			opacity: 0.5;
			font-size: 1em;
		}
	</style>

	<script>
		window.addEventListener('DOMContentLoaded', function(){

			var cy = window.cy = cytoscape({
				container: document.getElementById('cy'),

				layout: {
					/*name: 'spread',*/
					name: 'cola'
				},

				style: [
					{
						selector: 'node',
						style: {
							'label' : 'data(label)',
							'background-color': '#ea8a31'
						}
					}, {
						selector: 'edge',
						style: {
							'label' : 'data(label)',
							'curve-style': 'haystack',
							'haystack-radius': 0,
							'width': 3,
							'opacity': 0.666,
							'line-color': '#fcc694'
						}
					}, {
						selector: 'node[type="query"]',
						style: {
							'shape': 'triangle',
							'background-color': 'red',
							'label' : 'data(label)',
							'width' : '120px',
							'height' : '120px',
							'color' : 'blue',
							'background-fit' : 'contain',
							'background-clip' : 'none'
						}
					}
				],
				elements: {
					"nodes": [
						<%
						String query = (String)session.getAttribute("origQuery");
						Set<String> setNodes = new LinkedHashSet<String>();
						setNodes.addAll((Set)session.getAttribute("datasets"));
						StringBuffer sbNodes = new StringBuffer();
						for (String ds : setNodes) {
							if(ds == null) continue;
							String s[] = ds.split(";");
							if(s == null) continue;
							String nodeName = null;
							if(s.length > 0){
								nodeName = s[0];
							} else{
								nodeName = ds;
							}
							
							if(query.toLowerCase().contains("select")){
								Set<String> setQuery = DatabaseMain.extractProperties(query);
								for (String sDs : setQuery) {
									if(nodeName.toLowerCase().contains(sDs.toLowerCase())){
										sbNodes.append("{ \"data\": {\"id\": \""+nodeName+"\", \"label\": \""+nodeName+"\", \"type\": \"query\"}},\n");									
									} else {
										sbNodes.append("{ \"data\": {\"id\": \""+nodeName+"\", \"label\": \""+nodeName+"\"}},\n");									
									}
								}
							} else if(query.indexOf(",") > 0){
								String sDs[] = query.split(",");
								for(int i=0; i<sDs.length; i++){
									if(nodeName.toLowerCase().contains(sDs[i].toLowerCase())){
										sbNodes.append("{ \"data\": {\"id\": \""+nodeName+"\", \"label\": \""+nodeName+"\", \"type\": \"query\"}},\n");									
									} else {
										sbNodes.append("{ \"data\": {\"id\": \""+nodeName+"\", \"label\": \""+nodeName+"\"}},\n");									
									}
								}
							} else {
								query = query.toLowerCase().replaceAll("http://", "");
								query = query.toLowerCase().replaceAll("https://", "");
								if(nodeName.toLowerCase().contains(query)){
									sbNodes.append("{ \"data\": {\"id\": \""+nodeName+"\", \"label\": \""+nodeName+"\", \"type\": \"query\"}},\n");									
								} else {
									sbNodes.append("{ \"data\": {\"id\": \""+nodeName+"\", \"label\": \""+nodeName+"\"}},\n");									
								}
							}
						}
						String nodes = sbNodes.toString();
						if(nodes.length() > 2){
							nodes = nodes.substring(0, nodes.length() - 2);
						}
						out.println(nodes);
						%>
						],
					"edges": [
						<%
						Map<String, Set<String>> mapEdges = new LinkedHashMap<String, Set<String>>();
						mapEdges.putAll((Map)session.getAttribute("edges"));
						StringBuffer sbEdges = new StringBuffer();
						for (Entry entry : mapEdges.entrySet()) {
							Set<String> setRel = (Set)entry.getValue();						
							for (String ds : setRel) {
								String s[] = ds.split(";");
								if(s.length > 1){
									sbEdges.append("{ \"data\": { "
										+ "\"id\": \""+entry.getKey()+"-"+s[0]+"\", "
										+ "\"label\": \""+s[1]+"\", "
										+ "\"source\": \""+entry.getKey()+"\", "
										+ "\"target\": \""+s[0]+"\"}"
										+ "},\n");
								} else {
									sbEdges.append("{ \"data\": { "
											+ "\"id\": \""+entry.getKey()+"-"+s[0]+"\", "
											+ "\"label\": \"contained\", "
											+ "\"source\": \""+entry.getKey()+"\", "
											+ "\"target\": \""+s[0]+"\"}"
											+ "},\n");
								}
							}
						}
						String edges = sbEdges.toString();
						if(edges.length() > 2){
							edges = edges.substring(0, edges.length() - 2);
						}	
						out.println(edges);
						%>
						]
				}

			});
		});
	</script>
</head>

<body>
<h1>ReLOD demo dataset visual similarity relations</h1>

<div id="cy"></div>

</body>

</html>