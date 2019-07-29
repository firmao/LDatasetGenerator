# LDatasetGenerator
Extract all properties and values from entities from the whole LOD cloud.

DOCUMENTATION AND THE CODE ARE NOT COMPLETE YET, STILL REFACTORING AND FINISHING EXPERIMENTS. NOT GARANTEE THAT WILL WORK, IF YOU NEED A STABLE VERSION, PLEASE ASK Andre Valdestilhas (firmao@gmail.com).

## Web Interface to query the index (Proof of concept)
https://github.com/firmao/LODDatasetRelationsWeb

<img src="http://139.18.13.76:8082/queryIndex.png">

## Including: 
An approach for automatic dataset matching.

Automatic identification of datasets.

## Setup:
Clone the repository and don't forget to put on the classpath all jar files from lib directory.

## Datasets already included on the index:
- All 539 from LOD cloud: https://github.com/firmao/wimuT/blob/master/Endpoints_numtriples_lodcloud.csv
- 915 datasets (HDT files) randomly selected from [LOD Laundromat](http://lodlaundromat.org/).
- Takes 88 hours to generate the LOD dataset relation index.

## Classes
- Property matching using gold Standard from webTables(http://www.webdatacommons.org/webtables/)
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/PropertyMatchingNN.java

- Simple property matching with small gold standard and datasets:
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/PropertyMatching.java

- Statistics about datasets relations from the LOD cloud:
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/DsRelationStatistics.java

- Simple Dataset info, where use 2 parameters, 'java -jar TestDatasets.jar <datasets:Dir or file> <SPARQL query>'
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/TestDatasets.java
  
- Extract property and value from a given collection of datasets.
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/Main.java
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/DsGenerator.java

- Class reponsible for creating the LOD datasets relation index:
- https://github.com/firmao/LDatasetGenerator/blob/master/src/main/java/test/testid/IndexCreator.java

<img src="http://139.18.13.76:8082/createIndex.png">
