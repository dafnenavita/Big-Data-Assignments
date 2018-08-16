// Databricks notebook source
def parseData(line: String): (String,List[String])={

    val data=line.split("\\t")

    (data(0),(data(1).split(",").toList))
  }

  def findCommon(arr1:List[String],arr2:List[String]) ={
    arr1.intersect(arr2)
  }



    val lines = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
    val split = lines.filter(line=> line.split("\\t").size == 2).map(parseData)
    val result = split.flatMap(entry=>
      entry._2.map(e=>{
        if(entry._1<e)
        ((entry._1,e),entry._2)

        else
          ((e,entry._1),entry._2)
      }))



val result2 = result.sortByKey().reduceByKey(findCommon)
val finalres = result2.map(li => li._1._1 + "," + li._1._2 + "\t" + li._2.mkString(","))
finalres.saveAsTextFile("/FileStore/output/mutualop1")
  

  


// COMMAND ----------


