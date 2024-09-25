# Please add your team members' names here. 

## Team members' names 

1. Student Name: Karnika Choudhury

   Student UT EID: kc45756

2. Student Name: Samrut Gadde

   Student UT EID: sg53989

3. Student Name: Kshitij Kapoor

   Student UT EID: kk36592

4. Student Name: Navya Agrawal

   Student UT EID: na27378

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 

Screenshots
![Dataproc](images/dataproc_sc.png)


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-small.csv  output final ``` 

Or has hadoop application

```hadoop jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar  taxi-data-sorted-small.csv output final```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
