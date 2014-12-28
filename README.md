# StormWordCount
----------

###<u>Note</u>: Updated to latest available `Storm` version i.e. v0.9.3 on 28th December, 2014. Storm package structure is a bit different now as it is a [TLP in Apache](https://blogs.apache.org/foundation/entry/the_apache_software_foundation_announces64) eff. 29th Sep, 2014.

## Introduction
Skeleton Storm project for [The Fifth Elephant, 2013](https://fifthelephant.in/2013) workshop on [Big Data, Real-time Processing and Storm](https://fifthelephant.in/2013/workshops) on 11th July, 2013 at NIMHANS Convention Centre, Bangalore.<br>
This will be a live-coding session where you will learn how to use Storm. This workshop is for software developers with some background in Java programming who are interested in distributed data processing.

This repository contains an application for demonstrating Storm distributed framework by counting the words present in random sentences fed by the code [in the Spout] in real-time.<br>This project does not need internet access while executing the topology i.e. once configured and Maven downloads all the required dependencies. Please check my other repo, [StormTweetsWordCount](https://github.com/P7h/StormTweetsWordCount) for counting words in tweets which *needs* internet access for getting data from Twitter.

[Apache Storm](http://storm.apache.org) is a free and open source distributed real-time computation system, developed at BackType by Nathan Marz and team. It has been open sourced by Twitter [post BackType acquisition] in August, 2011.<br>
This application has been developed and tested initially with Storm v0.8.2 on Windows 7 in local mode; and was eventually updated and tested with Storm v0.9.3 on 28th December, 2014. Application may or may not work with earlier or later versions than Storm v0.9.3.<br>

This application has been tested in:<br>

+ Local mode on a CentOS machine and even on Microsoft Windows 7 machine.
+ Cluster mode on a private cluster and also on Amazon EC2 environment of 4 machines and 5 machines respectively; with all the machines in private cluster running Ubuntu while EC2 environment machines were powered by CentOS.

This application uses and complements [Storm Starter](https://github.com/apache/storm/tree/master/examples/storm-starter) Project.

## Features
* Application receives random sentences from Spout.<br>
* It splits each sentence with space as the delimiter and counts frequency of the words present in sentences.<br>
* Every 5 seconds, during processing, the application logs the word and its count to the console and also to a log file. <br>
* In local mode, topology runs for 2 minutes and then shuts down. Topology time duration can be updated by modifying [this](src/main/java/org/p7h/storm/offline/wordcount/topology/WordCountTopology.java#L48) value.<br>
* Also this project has been made compatible with both Eclipse IDE and IntelliJ IDEA. Import the project in your favorite IDE [which has Maven plugin installed] and you can quickly follow the code.
* As of today, this codebase has almost no or very less comments.

## Dependencies
* Storm v0.9.3
* Google Guava v18.0
* Logback v1.1.2

Also, please check [`pom.xml`](pom.xml) for more information on the various dependencies of the project.<br>

## Requirements
This project uses Maven to build and run the topology.<br>
You need the following on your machine:

* Oracle JDK >= 1.7.x
* Apache Maven >= 3.2.3
* Clone this repo and import as an existing Maven project to either Eclipse IDE or IntelliJ IDEA.
* This application uses [Google Guava](https://code.google.com/p/guava-libraries) for making life simple while using Collections.
* Requires ZooKeeper, etc installed and configured in case of executing this project in distributed mode i.e. Storm Cluster.<br>
	- Follow the steps mentioned on [Storm Wiki](http://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) for more details on setting up a Storm Cluster.<br>

Rest of the required frameworks and libraries are downloaded by Maven as required in the build process, the first time the Maven build is invoked.

## Usage
To build and run this topology, you must use Java 1.7 or above.

### Local Mode:
Local mode can also be run on Windows environment without installing any specific software or framework as such. *Note*: Please be sure to clean your temp folder as it adds lot of temporary files in every run.<br>
In local mode, this application can be run from command line by invoking:<br>

    mvn clean compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=org.p7h.storm.offline.wordcount.topology.WordCountTopology

or

    mvn clean compile package && java -jar target/storm-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar
	
### Distributed [or Cluster / Production] Mode:
Distributed mode requires a complete and proper Storm Cluster setup. Please refer this [wiki page on Apache Storm website](http://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) for setting up a Storm Cluster.<br>
In distributed mode, after starting Nimbus and Supervisors on individual machines, this application can be executed on the master [or Nimbus] machine by invoking the following on the command line:

    storm jar target/storm-wordcount-1.0-SNAPSHOT.jar org.p7h.storm.offline.wordcount.topology.WordCountTopology WordCount

> Note: Repo's recent updation to Storm v0.9.3 was not tested in Cluster mode. But it should work as before [i.e. earlier versions of Storm], if the cluster setup is all fine.

## Problems
If you find any issues, please report them either raising an [issue](https://github.com/P7h/StormWordCount/issues) here on Github or alert me on my Twitter handle [@P7h](http://twitter.com/P7h). Or even better, please send a [pull request](https://github.com/P7h/StormWordCount/pulls).
Appreciate your help. Thanks!

## License
Copyright &copy; 2013-15 Prashanth Babu.<br>
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).