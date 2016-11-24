import org.apache.spark.rdd.RDD

/**
  * Created by jonathan on 23/11/16.
  */
object Hashing {
  val BUCKETSIZE = 1000

  def initBuckets(size: Int): Array[List[String]] = {
    var arr = new Array[List[String]](size)
    arr.map(_ => List[String]())
  }

  def stupidHash(word: String, seed: Int = 0): Int = {
    word.getBytes.foldLeft(0)(_+_)
  }

  def javaHash(word: String, seed: Int = 0): Int = {
    var hash = 0

    for (ch <- word.toCharArray)
      hash = 31 * hash + ch.toInt

    hash = hash ^ (hash >> 20) ^ (hash >> 12)
    hash ^ (hash >> 7) ^ (hash >> 4)
  }

  def murmurHash(word: String, seed: Int): Int = {
    val c1 = 0xcc9e2d51
    val c2 = 0x1b873593
    val r1 = 15
    val r2 = 13
    val m = 5
    val n = 0xe6546b64

    var hash = seed

    for (ch <- word.toCharArray) {
      var k = ch.toInt
      k = k * c1
      k = (k << r1) | (hash >> (32 - r1))
      k = k * c2

      hash = hash ^ k
      hash = (hash << r2) | (hash >> (32 - r2))
      hash = hash * m + n
    }

    hash = hash ^ word.length
    hash = hash ^ (hash >> 16)
    hash = hash * 0x85ebca6b
    hash = hash ^ (hash >> 13)
    hash = hash * 0xc2b2ae35
    hash = hash ^ (hash >> 16)

    hash
  }

  def knuthHash(word: String, constant: Int): Int = {
    var hash = 0
    for (ch <- word.toCharArray)
      hash = ((hash << 5) ^ (hash >> 27)) ^ ch.toInt
    hash % constant
  }

  /**
    * encuentra el númuero primo mayor que n más cercano
    *
    */
  def getPrimeValue(base: Long): Long = {

    var primoMayor = base
    var flag = true

    while(flag){
      var divisor=1;
      var j=0;

      while(divisor<=primoMayor){
        if(primoMayor%divisor==0){
          j = j + 1;
        }
        divisor = divisor + 1;
      }
      if(j<=2){

        flag = false
      }else {primoMayor = primoMayor + 1}
    }
    primoMayor
  }

  def classHashFunction (bandAndDocument: RDD[Tuple2[String, String]], isDebug: Boolean, nband: Int) : RDD[Tuple2[String,String]] = {
    //Concatena todas las firmas que pertenezcan a una misma banda y documento
//    val longSignatures = bandAndDocument.map(line => (line._1, line._2.toLong))
    val sumSignatures = bandAndDocument.reduceByKey((a,b) => a+b)
    if (isDebug) sumSignatures.foreach(line => println("concatenate"+line))

    //Se hace el mapeo a ((banda, bucket), documento)
    val mapBucket = sumSignatures.map(x => (x._1.split(",")(0) + "," + ((BigInt(x._2) + x._1.split(",")(0).toLong) % getPrimeValue(nband)), x._1.split(",")(1)))
    if (isDebug) mapBucket.foreach(line => println("mapBucket: " + line))
    mapBucket
  }

  def main(words: List[String]): Unit = {
    val buckets = initBuckets(BUCKETSIZE)
//    val diff = words.length/BUCKETSIZE

    import scala.util.Random
    val seed = Random.nextInt
    for (word <- words) {
      // val bucket = stupidHash(word, seed) % BUCKETSIZE
      // val bucket = javaHash(word, seed) & (BUCKETSIZE-1)
      // val bucket = murmurHash(word, seed) % BUCKETSIZE
      // val bucket = knuthHash(word, 1009) & (BUCKETSIZE-1)
      val bucket = knuthHash(word, 31) & (BUCKETSIZE-1)

      buckets(bucket) ::= word
    }

    println(buckets.map(x => (x.length :: x.take(3)).mkString("\t")).mkString("\n"))
  }
}
