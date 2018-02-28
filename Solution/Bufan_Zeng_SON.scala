import java.io._

import util.control.Breaks._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.spark.rdd.RDD


object Bufan_Zeng_SON {
    // sort function for output
    def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted
    // apriori function
    def apriori(basket: Iterator[Set[String]], threshold:Int): Iterator[Set[String]] = {
        var chunklist = basket.toList

        // frequent single items
        val single = chunklist.flatten.groupBy(identity).mapValues(_.size).filter(x => x._2 >= threshold).map(x => x._1).toSet

        var size = 2
        // freq to store the candidate single items for next round
        var freq = single
        // hmap to store all the valid candidate set
        var hmap = Set.empty[Set[String]]
        // store all the frequent single items to hmap
        for (i <- single){
            hmap = hmap + Set(i)
        }
        // res to store the candidate single items after each round
        var res = Set.empty[String]
        // size of hmap
        var hmapsize = hmap.size

        while (freq.size >= size) {
            var candidate = freq.subsets(size)
            for (tmp <- candidate) {
                breakable{
                    var ct = 0
                    for (i <-chunklist){
                        if (tmp.subsetOf(i))
                        {
                            ct = ct + 1
                            if (ct >= threshold){
                                res = res ++ tmp
                                hmap = hmap + tmp
                                break
                            }
                        }
                    }
                }
            }
//            var candidate = freq.subsets(size).toSet
//            var amap = mutable.HashMap.empty[Set[String],Int]
//            for (i <- chunklist) {
//                for (j <- candidate) {
//                    breakable {
//                        if (j.subsetOf(i)) {
//                            if (amap.contains(j)) {
//                                amap(j) += 1
//                                if (amap(j) >= threshold) {
//                                    hmap = hmap + j
////                                    for (k <- j) {
////                                        res = res + k
////                                    }
//                                        res = res ++ j
//                                    break
//                                }
//                            } else {
//                                amap += (j -> 1)
//                            }
//                        }
//                    }
//                }
//            }

            if (hmap.size >hmapsize){
                freq = res
                res = Set.empty[String]
                size += 1
                hmapsize = hmap.size
            } else {
                size = 1 + freq.size
            }

        }
        hmap.iterator
    }

    def main(args: Array[String]) : Unit = {
        val start_time = System.currentTimeMillis()
        val conf = new SparkConf().setAppName("hw2_p1").setMaster("local[2]")
        var sc = new SparkContext(conf)
        var raw = sc.textFile(args(1))
        var header = raw.first()
        raw = raw.filter(row => row != header)
        // split by ,
        var data = raw.map(row => row.split(","))
        // get case number
        val casenum = args(0).toInt
        // get partition number and anjust sup
        var partitionnum = raw.getNumPartitions
        val sup = args(2).toInt
        // generate baskets

        var baskets: RDD[Set[String]] = null
        if (casenum == 1){
            baskets = data.map(x => (x(0), x(1))).groupByKey().map(_._2.toSet)
        } else {
            baskets = data.map(x => (x(1), x(0))).groupByKey().map(_._2.toSet)
        }


        var threshhold = 1
        if (sup/partitionnum >threshhold){
            threshhold = sup/partitionnum
        }
        var first = baskets.mapPartitions(chunk => {
            apriori(chunk, threshhold)
        }).map(x=>(x,1)).reduceByKey((v1,v2)=>1).map(_._1).collect()
        //
        val broadcasted = sc.broadcast(first)

        var secondmap = baskets.mapPartitions(chunk => {
            var chunklist = chunk.toList
            var out = List[(Set[String],Int)]()
            for (i<- chunklist){
                for (j<- broadcasted.value){
                    if (j.forall(i.contains)){
                        out = Tuple2(j,1) :: out
                    }
                }
            }
            out.iterator
        })
        var last = secondmap.reduceByKey(_+_).filter(_._2 >= sup).map(_._1).map(x => (x.size,x)).collect()
        val max = last.maxBy(_._1)._1
        val writer = new PrintWriter(new File("result.txt"))
        var tosort = sort(last.filter(_._1 == 1).map(_._2))
        for (i<- tosort)
        {
            if (i==tosort.last){
                writer.write(i.mkString("('","', '","')\n\n"))
            }
            else {
                writer.write(i.mkString("('","', '","'), "))
            }
        }
        var pr = ""
        for (i <- 2.to(max)){
            var x = sort(last.filter(_._1 == i).map(_._2))

            for (k<- x)
            {   pr = pr + "("
                var s = k.toList.sorted
                for (j<- s){
                    if (j==s.last){
                        pr = pr + "'" + j +"'), "
                    }
                    else {
                        pr = pr + "'" + j +"', "
                    }
                }
            }
            pr = pr.dropRight(2) + "\n\n"
        }
        writer.write(pr)
        writer.close()
        val end_time = System.currentTimeMillis()
        println("Time: " + (end_time - start_time)/1000 + " secs")
    }
}