// Databricks notebook source
//Split userlist and friend list
def userSplit(line: String): (String,Array[String])={

    val data=line.split("\\t")

    (data(0),data(1).split(","))
  }

// find mutual friends
  def mutual(arr1:Array[String],arr2:Array[String]) ={
    arr1.intersect(arr2)
  }

    //file paths 
    val inputfile = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
    val userData= sc.textFile("/FileStore/tables/userdata.txt")

    // mutual friend list
    val mutualFriends=inputfile.filter(line=> line.split("\\t").size == 2).map(userSplit).flatMap(entry=>
      entry._2.map(e=>{
        if(entry._1<e)
          ((entry._1,e),entry._2)

        else
          ((e,entry._1),entry._2)
      })).sortByKey().reduceByKey(mutual).map(friend=>(friend._1,friend._2.length)).filter(out=>out._2.toInt!=0)
	  
    //sorted friends based on top-10
	  val sortedFriends = mutualFriends.sortBy(_._2,false).take(10)

    //get user details 
    val mutualrdd=sc.parallelize(sortedFriends)
    val userDetails = userData.map(line => line.split(",")).map(line => (line(0), line.slice(1,8).mkString(",")))
    val userrdd = sc.broadcast(userDetails.collectAsMap())
    val userinfo = userrdd.value
    val output = mutualrdd.mapPartitions({ iter =>
      for {
        ((user1, user2), values) <- iter
        if (userinfo.contains(user1) && userinfo.contains(user2))
      } yield (values+"\t"+userinfo.get(user1).get.replace(',','\t'), userinfo.get(user2).get.replace(',','\t'))
    }, preservesPartitioning = true)

  // get output file 
  output.coalesce(1).saveAsTextFile("/FileStore/output/ques2_op")

 

// COMMAND ----------


