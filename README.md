Closest Airport Locator
=================
See our [web site](https://kafka.apache.org) for details on the project.

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

Java 8 should be used for building in order to support both Java 8 and Java 11 at runtime.

Scala 2.12 is used by default,

Spark 2.4.4

You need to have Kafka 2.11.x ro above cluster

### Build a jar and run it ###
    mvn clean install

### Run unit/integration tests ###
Update the input for test at `src/main/resources/user/` or `src/main/resources/airport/` 
    mvn test

### Assumptions ###
Distance calculate between the two coordinates is directly proportional to the distance by road to the destination.

### Limitation ###
Application will need restart while airport data changes.

### Application configurations ###
Sample file `app.properties`

### Application event feed and output info ###
Application takes input as follows,
1. `topic.name.user` input topic name for user data
2. `file.path.airport` csv file path for Airport data.  
3. `topic.name.result` output kafka topic i.e. `<userId,airportId>` events

### Start application parameters ###
###### VM arguments as
1. -Dspark.master=local[1]
2. -Dspark.name=ClosestAirportLocator  

###### Main Class
1. com.ds.training.CoordinateLocator

###### Program arguments
1. src/main/resources/app.properties****  



