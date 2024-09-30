<meta name="robots" content="noindex">

# GETL: An Extract-Transform-Load Framework Across Different Graph Data Models

## Dependencies

The GETL is a Java Maven project and requires JDK 11 and Maven 3.6.1 to build from source.
- Java (>=11)
- Maven (>=3.6.1)

Extra dependencies are required by examples:
- MySQL (>=5.7)

## Configurations

Before building the project, please configure the path of the data file and MySQL in the yaml file.

In the `src/main/resources/config.yaml` file, we can configure MySQL configuration and PG\RDF data file paths for the project.

## Building GETL and examples

Next, change into the root directory of GETL, and use the Maven command to build the project.
```
mvn package 
```
Now, we have generated the `GETL-1.00.jar` file in the `\target` directory. We can run the project with command:
```
java -jar GETL-1.00.jar -c q1
```
`-c` command line argument control which example class will be run.

A part of runnable example classes are shown below:

| Abbreviation | Full class name                      |
| :------------: | :------------------------------------: |
| q1           | com.getl.example.query.Q1            |
| q2           | com.getl.example.query.Q2            |
| ...          | ...                                  |
| q7           | com.getl.example.query.Q7            |
| pg2ug        | com.getl.example.converter.PG2UGTest |
| rm2ug        | com.getl.example.converter.RM2UGTest |
| ...          | ...                                  |

If we allocate too little memory, there may be problems. We can use the following command to ensure that the JVM has sufficient heap and stack space.
```
java -jar -Xms40960m -Xmx92160m -Xss64m GETL-1.00.jar -c q1
```
## Implement an runnable class

We can also customize a class to implement the ETL operations we want to accomplish. This operation is very simple. We just need to let our class extend the `com.getl.example.Runnable` abstract class and implement the `accept` method. Finally, register this runnable class in the `com.getl.example.GetlExampleMain` class, and we can execute it like the class in the example

## Dataset

Due to the large size of the dataset files, we didn't upload them to this page. Below are the datasets used and their download paths.

| Graph                                | Graph Model      | Data Size                               | Download URL                                                 |
| ------------------------------------ | ---------------- | --------------------------------------- | ------------------------------------------------------------ |
| MovieLens                            | relational model | 41,741,454 rows                         | https://grouplens.org/datasets/movielens/25m                 |
| LDBC Social Network Benchmark graphs | property graph   | 3,966,203 vertices and 23,031,794 edges | https://repository.surfsara.nl/datasets/cwi/snb              |
| Wikipedia                            | RDF              | 22,791,171 triples                      | https://databus.dbpedia.org/dbpedia/mappings/mappingbased-objects/2022.12.01/mappingbased-objects_lang=en.ttl.bz2. |

