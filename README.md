# Distributed-Key-Value-Store-with-Data-Partitioning
Course project on Distributed Key-Value Store with Data Partitioning
The project contains 1 client and 6 Nodes
The client can write,read and push the node from the ring.
Download the files and compile the source file. 
To compile us    javac -cp $AKKA CLASSPATH NodeApp.java ClientApp.java
To Run change the directory to one of the node and us java -cp $AKKA CLASSPATH:.:.. NodeApp
To Run Client and to do one of  the 3 tasks use 
 java -cp $AKKA CLASSPATH:.:.. ClientApp Remote ip Remote port write Key Value
 java -cp $AKKA CLASSPATH:.:.. ClientApp Remote ip Remote port read Key
 java -cp $AKKA CLASSPATH:.:.. ClientApp Remote ip Remote port leave
