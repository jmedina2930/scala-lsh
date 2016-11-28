import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.util.Calendar
import java.io.File
import java.util.function.ToIntFunction

object LshMain {

  /**
    * Retorna si se va a correr el programa en modo depuracion.
    * En este modo se deja codigo adicional que puede afectar el rendimiento. Ejemplo: guardar archivos
    */
  def isDebug(): Boolean = {
    true
  }

  /**
    * Imprime texto con la hora actual
    */
  def printlnWithTime(message: String): Unit = {
    val now = Calendar.getInstance()
    val time = now.get(Calendar.HOUR_OF_DAY).toString() + ":" + now.get(Calendar.MINUTE).toString() + ":" + now.get(Calendar.SECOND).toString()
    println("[" + time + "]:" + message)
  }


  /**
    * Mapea "id-shingle,id-doc,signature" to "band, id-doc", "id-shingle,id-doc,signature" Entera de Numero de linea/(Lineas por Banda)
    */
  def getBandAndDocument(line: String, linesPerBand: Long): (String, String) = {
    val rawLine = line.replace("((", "").replace(")", "").replace(" ", "")
    val fields = rawLine.split(",")
    val nline = fields(0).toLong
    ((((nline-1) / linesPerBand) + 1).toString() + "," + fields(1), fields(2))
  }

  /**
    * Obtiene el bucket y con eso mapea a banda,bucket
    */
  def hashFunction(a: String, b: String): String = {
    println("jaja" + a)
    val line1 = a.split(",")
    val signature1 = line1(1)
    val line2 = b.split(",")
    val signature2 = line2(1)
    signature1 + signature2
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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala-LSH").setMaster("local")
    val sc = new SparkContext(conf)

    /* *********************************************************************************************/
    //CONFIGIRACION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    val signaturesFilePath : String = args(0)
    val directoryOutput : String = args(1)
    val nband : Int = args(2).toInt
    val rowsPerBand : Int = args(3).toInt

//    val signaturesFilePath = "data/test1/step2"
//    val directoryOutput = "data/out"
//    val nband = 20
//    val rowsPerBand = 5
    
    println("signaturesFilePath: "+signaturesFilePath)
    println("directoryOutput   : "+directoryOutput)
    println("nband             : "+nband)
    println("rowsPerBand       : "+rowsPerBand)

    /* *********************************************************************************************/

    printlnWithTime("Inicia")

    //lee archivo con tres valores: Id shingle(Long), Id documento (Long), Signature
    val file = sc.textFile(signaturesFilePath)
    if (isDebug) file.collect().foreach(line => println("linea: " + line))

    //Primera fase Map-Reduce----------------------------------------------------


    // Mapea banda Y documento "(1,1,0)", "(1,2,0)" map to: ("1,1", "0"),("1,2", "0")
    val bandAndDocument = file.map(line => getBandAndDocument(line, rowsPerBand))
    if (isDebug) bandAndDocument.foreach(line => println("bandAndDocument"+line))

    //se aplica una funcion hash
    val mapBucket = Hashing.randomHashFunction(bandAndDocument, isDebug(), nband, rowsPerBand)
    if (isDebug) mapBucket.foreach(line => println("mapBucket: " + line))

    //Se concatenan los documentos que pertenezcan a la misma banda y misma cubeta
    val reduceBucket = mapBucket.reduceByKey((a,b) => a + ";" +b)
    if (isDebug) reduceBucket.foreach(line => println("reduceBucket: " + line))

    //Se dejan solo los resultados que contengan por lo menos un par de documentos
    val reduceFilter = reduceBucket.filter(line => line._2.split(";").size > 1)
    if (isDebug) reduceFilter.foreach(line => println("reduceFilter: " + line))

    //Segunda fase Map-Reduce------------------------------------------------

    //Mapeo a: ((doc1, doc2, ...), 1)
    val map2 = reduceFilter.map(x => (x._2,1))
    if (isDebug()) map2.foreach(line => println("Map2: " + line))

    //Reduce para sumar los pares candidatos repetidos
    val reduce2 = map2.reduceByKey((a,b) => a+b)
    if (isDebug()) reduce2.foreach(line => println("Reduce2: " + line))
    
    reduce2.repartition(1).saveAsTextFile(directoryOutput)

    printlnWithTime("Fin")

    sc.stop

  }

}