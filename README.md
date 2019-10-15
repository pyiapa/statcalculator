# statcalculator #
- - - -

## Author ##

Name:  Paris Yiapanis (pyiapa)  
Email: pyiapa@gmail.com


## Project Description ##

This project computes various summary statistics based on an expenses input CSV file.
Each row of the input file indicates a given expense. The row provides information on the date, 
type of expense, amount, whether it was reimbursed or not, and location.


## Design Decisions ##

* At the moment, the code computes 4 different summary statistics for the expenses input provided:
  - Most visited country
  - Most visited country in 2018
  - Costliest month
  - Daily spending for the last 60 days


* Each statistic is implemented as a separate class (single responsibility).

* To make the application cleaner and open for extension I have created a 
manager class (StatManager) to manage the calculation of each statistic.

* Each statistic is represented as a general Statistic abstract class (trait in Scala). 
Thus the StatManager manages several Statistic objects without the need to know what each statistic does.

* Using this approach, the developer can easily extend the application by creating a new statistic 
and simply registering it with the StatManager (the StatManager handles all functionality and does not need to be modified).
  
* The result of each statistic is held in a Result object

* To ensure correct file format, statistics are further subdivided into other traits depending on the area they serve. For example,
statistics that related to expenses, follow a specific format. Thus, there will be an ExpensesStatistic (which in turn extends Statistic) trait
where they can extend from.  Statistics that use different input file formats can use a different trait (to be implemented accordingly) in
a similar fashion. The motivation behind this is to allow shared format checking and avoid implementing the same functionality if the statistics
share he same input format. 

* Ultimately, we can create new statistics without changing the main logic. The StatManager also does
not know (and does not care) about the format of the input. This is handle by the format checkers.

* I have implemented 26 tests across 9 test suites. The tests are not meant to be comprehensive 
but only to serve as an example.

## Assumptions ##
 
 * I have assumed that the data can grow to giga/tera/peta/ bytes and thus decided to build a Spark application
 that can run on a cluster environment. 
 
 * I have assumed that if an expense is reimbursed then the amount (cost) for that item will be zero.
 Thus when calculating costs, the reimbursed items have been excluded.
 
 * I have assumed that most "visited country in the world" is the most visited country across all years
 
 * I have assumed that costliest month is the month with higher costs across all years.
 
 * Regarding calculating the daily spending for the last 60 days: if there was more than one expense in the same
 day then same-day expenses have been added together. Thus in the result there will be no duplicate dates.
 
## Output ##
 
 * When the application is built, a jar will be generated in the '/target' directory. Executing the jar will produce a folder '/output'
 in the current running directory. Inside the output folder there will be a separate folder for each 
 statistic. The folders indicate statistic output and are named after the statistics. Each of these folders will have 
 a CSV file containing the output for a given statistic. This file can be used for visualization (see 'Visualization' section)
 
## Visualization ##
 
 * As mentioned earlier, after executing the application, an '/output' folder will be produced in the
 current running directory including subdirectories for each statistic. Each subdirectory will also 
 include a Python script to be used for visualization. 
 
 * IMPORTANT: Each script must run inside the same directory
 as the corresponding statistic.
 
 * If for some reason the scripts have not been generated inside each statistic's output directory, they
 can also be found in '/target/visualization-scripts' after the project is built. In this case, each
 script must be moved inside the output folder of its corresponding statistic.
 
 * Please see the 'Building and Running' section on details on how to install the Python requirements and 
 run the scripts
 
 * As most statistics in this example produce single results, perhaps the statistic worth visualizing in
 this case would be the daily cost for the last 60 days
 
## Building and Running ##
 
* The project was built using maven 3.3.9, Scala 2.11.
For the visualization scripts, Python 3.5.1 was used. Java Development Kit will be 
required to be install to run maven (JDK 1.8 or above in this case).

* Other dependencies: Java 1.8, Spark 2.3.1, Scalatest 3.0.4, Scalactic 3.0.0

* To build, unzip the folder statcalculator and build with maven. For example, typing

  mvn clean install

  will compile the sources, execute the tests and package the compiled files in a JAR file. 
  It will also install the resulting artifact into the local repository, so it can be used 
  as dependencies by other Maven builds.

* The above build will produce a jar file with all the dependencies included inside a '/target' directory

* To run the application in a local machine, cd inside the directory of the built jar file and type the
following command in the terminal:

```scala
  java -jar statcalculator-0.0.1-SNAPSHOT-jar-with-dependencies
```

* To run in a cluster, please change the parameter in SparkSession before building,
to the desired number of cores wishing the application to utilize. Also, you will need to
change the input and output paths. Further info on launching an application on the cluster can be found here:

  https://spark.apache.org/docs/latest/submitting-applications.html

* The application will produce an '/output' folder in the current running directory. Inside, there will
be a different directory for each statistic that was calculated. These directories include the output 
in CSV format as well as python scripts to visualize the results.

* Before visualizing, ensure all Python requirements are satisfied (and an appropriate Python version
is installed). Each statistic output directory will contain a 'requirements.txt' file.
To install the Python requirements, type:

```python
  pip install -r requirements.txt
  ```

* To visualize a statistic, cd to the desired statistic directory (e.g. costliest_month) and type:

```python
  Python costliest_month.py
  ```

  This will produce a plot of the result for the costliest month

* Note: if you use ScalaIDE please make sure to change Scala compiler setting to 2.11

## Testing ##
 
* A total of 26 tests across 9 test suites are included. The test cases check for both valid and invalid input
  as well as correct functionality.
  
* The tests are not meant to be comprehensive but only to serve as an example. 

## Further work ##

* If running on a cluster, Input could be loaded directly from a storage like Amazon S3.

* Output could also be moved to S3 for persistent storage

* In a cluster, results could be visualized using Apache Zeppelin. Zeppelin integrates well with Spark
and for example can be used to execute SQL queries directly against SparkSQL. Query results can be visualized
using charts and graphs

* Results could be also exported in hive tables and visualized via external software such as Tableau