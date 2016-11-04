package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
 
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the name and age fields, and convert to integers
      val name = fields(1).toString
      val age = fields(2).toInt
      // Create a tuple that is our result.
      (name, age)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByName")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("C:/Users/rodrigo.rondena/Desktop/SparkScala/SparkScalaCourse/fakefriends.csv")
    
    // Use our parseLines function to convert to (name, age) tuples
    val rdd = lines.map(parseLine)
    
    // Lots going on here...
    // We are starting with an RDD of form (name, age) where age is the KEY and name is the VALUE
    // We use mapValues to convert each name value to a tuple of (name, 1)
    // Then we use reduceByKey to sum up the total age and total instances for each name, by
    // adding together all the age values and 1's respectively.
    val totalsByName = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    // So now we have tuples of (age, (totalAge, totalInstances))
    // To compute the average we divide totalAge / totalInstances for each age.
    val averagesByName = totalsByName.mapValues(x => x._1 / x._2)
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByName.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }
    

}