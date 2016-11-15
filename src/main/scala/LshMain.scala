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
    * Crea archivo con datos de prueba
    * (h0,D0,0), (h0,D1,0), ...
    */
  def createDataFile(sc: SparkContext, directoryOutput: String): Unit = {
    val array = new Array[(String, String)](200)
    //key = hash,group,signature, h0, D1, 2
    for (i <- 0 until 20) {
      for (j <- 0 until 10) {
        array(i * 10 + j) = ("(" + (i + 1).toString() + "," + (j + 1).toString() + ")" -> (i % 5).toString())
      }
    }

    val arrayRdd = sc.parallelize(array)
    arrayRdd.saveAsTextFile(directoryOutput)
  }

  /**
    * Borra directorio de salida
    * hdfs = true => borra el directorio en el sistema distribuido, false en el sistema local
    */
  def deleteDirectoryOutput(directoryOutput: String, hdfs: Boolean): Unit = {

    if (!hdfs) {
      val file = new File(directoryOutput)
      if (file.listFiles() != null) file.listFiles().foreach { x => x.delete() }
      file.delete()
    } else {
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)
      hdfs.delete(new org.apache.hadoop.fs.Path(directoryOutput), true)
    }
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
  def getBandAndBucket(a: String, b: String): String = {
    println("jaja" + a)
    val line1 = a.split(",")
    val signature1 = line1(1)
    val line2 = b.split(",")
    val signature2 = line2(1)
    signature1 + signature2
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala-LSH").setMaster("local")
    val sc = new SparkContext(conf)

    /* *********************************************************************************************/
    //CONFIGIRACION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    //    Parametros de entrada (produccion cluster)
    //      val signaturesFilePath : String = args(0)
    //      val directoryOutput : String = args(1)
    //      val rowsPerBand : Int = args(2).toInt

    //      Parametros quemados (Pruebas locales)
    val hdfsFiles = false
    val signaturesFilePath = "data/signaturesMat"
    val directoryOutput = "data/out"
    val rowsPerBand = 5

    /* *********************************************************************************************/

    deleteDirectoryOutput(directoryOutput, hdfsFiles)
    //     createDataFile(sc, directoryOutput)


    printlnWithTime("Inicia")

    //lee archivo con tres valores: Id shingle(Long), Id documento (Long), Signature
    val file = sc.textFile(signaturesFilePath)
    if (isDebug) file.collect().foreach(line => println("linea: " + line))

    //Primera fase Map-Reduce----------------------------------------------------

    // Mapea banda Y documento "(1,1,0)", "(1,2,0)" map to: ("1,1", "0"),("1,2", "0")
    val bandAndDocument = file.map(line => getBandAndDocument(line, rowsPerBand))
    if (isDebug) bandAndDocument.foreach(line => println(line))

    //Concatena todas las firmas que pertenezcan a una misma banda y documento
    val concatenate = bandAndDocument.reduceByKey((a,b) => a+b)

    //Se hace el mapeo a ((banda, bucket), documento)
    val map1 = concatenate.map(x => ((x._1.split(",")(0) + "," + (x._2.toLong + x._1.split(",")(0).toLong) % 23), x._1.split(",")(1)))
    if (isDebug) map1.foreach(line => println("map1: " + line))

    //Se concatenan los documentos que pertenezcan a la misma banda y misma cubeta
    val reduce1 = map1.reduceByKey((a,b) => a + "," +b)

    //Se dejan solo los resultados que contengan por lo menos un par de documentos
    val reduceFilter = reduce1.filter(line => line._2.split(",").size > 1)
    if (isDebug) reduceFilter.foreach(line => println("Reduce1: " + line))


    //Segunda fase Map-Reduce------------------------------------------------

    //Mapeo a: ((doc1, doc2, ...), 1)
    val map2 = reduceFilter.map(x => (x._2,1))
    if (isDebug()) map2.foreach(line => println("Map2: " + line))

    //Reduce para sumar los pares candidatos repetidos
    val reduce2 = map2.reduceByKey((a,b) => a+b)
    if (isDebug()) reduce2.foreach(line => println("Reduce2: " + line))




    printlnWithTime("Fin")

    sc.stop

  }

}