<meta name="robots" content="noindex">

# GETL: An Extract-Transform-Load Framework Across Different Graph Data Models

## Dependencies

The GETL is a Java Maven project and requires JDK 11 and Maven 3.6.1 to build from source.
- Java (>=11)
- Maven (>=3.6.1)

Extra dependencies are required by examples:
- MySQL (>=5.7)

## Building GETL and examples

Before building the project, please configure the path of the test file and MYSQL in the code.

In the `src/main/Java/com/getl/example` directory, there are all the experiments. Please select the experiment you want to run and copy its full class name (e.g. `com.getl.example.query.Q1`) to the `<main-class>` tag in the `pom.xml` file

Then, move to the `src/main/Java/com/getl/constant/CommonConstant.Java` file and configure MySQL configuration and PG\RDF data file paths for the project.

Next, change into the root directory of GETL, and use the Maven command to build the project.
```
mvn package 
```
Now, we have generated the `GETL-1.00.jar` file in the `\target` directory. We can run the project with command:
```
java -jar GETL-1.00.jar
```
If we allocate too little memory, there may be problems. We can use the following command to ensure that the JVM has sufficient heap and stack space.
```
java -jar -Xms40960m -Xmx92160m -Xss64m GETL-1.00.jar
```

## Dataset

Due to the large size of the dataset files, we didn't upload them to this page. Below are the datasets used and their download paths.

| Graph                                | Graph Model      | Data Size                               | Download URL                                                 |
| ------------------------------------ | ---------------- | --------------------------------------- | ------------------------------------------------------------ |
| MovieLens                            | relational model | 41,741,454 rows                         | https://grouplens.org/datasets/movielens/25m                 |
| LDBC Social Network Benchmark graphs | property graph   | 3,966,203 vertices and 23,031,794 edges | https://repository.surfsara.nl/datasets/cwi/snb              |
| Wikipedia                            | RDF              | 22,791,171 triples                      | https://databus.dbpedia.org/dbpedia/mappings/mappingbased-objects/2022.12.01/mappingbased-objects_lang=en.ttl.bz2. |

