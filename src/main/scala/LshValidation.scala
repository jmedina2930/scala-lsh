import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

//import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.ml.linalg.SparseVector
//import org.apache.spark.ml.linalg.DenseMatrix

//import org.apache.spark.ml.linalg.{Matrix, Matrices, DenseMatrix}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseMatrix}




import java.util.Calendar
import scala.collection.Seq

object LshValidation {
  
    /**
    * Retorna si se va a correr el programa en modo depuracion.
    * En este modo se deja codigo adicional que puede afectar el rendimiento. Ejemplo: guardar archivos
    */
  def isDebug(): Boolean = {
    false
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
     * lee un archivo correspondiente a una matriz.
     * Convierte las columnas en filas
     */
    def columnsToRows(file: RDD[String]): RDD[String] = {
      
      if(isDebug) file.collect().foreach(line => println("linea: "+line))
        
      val mapColums = file.zipWithIndex.flatMap{ 
        case (row, rowIndex) => row.replace("(","").replace(")","").replace(" ","").split(",").zipWithIndex.map { 
          case (value, columnIndex) => columnIndex -> value
        } 
      }
  
      if(isDebug) mapColums.foreach(println)
      
      val columns = mapColums.groupByKey().sortByKey().values
      val columnsFormatted = columns.map( a => a.mkString(";"))
      
      columnsFormatted      
    }  
  
  def jaccard(sv1: Vector, sv2: Vector): Double = {
    var acc_intersection = 0;
    
    println("v1="+sv1.size + ", "+sv1.numActives)
    println("v1=2="+sv2.size + ", "+sv2.numActives)
    
    if(sv1.numActives < sv2.numActives){
      sv1.foreachActive( (index, value) => {if(sv2(index) != 0.0) acc_intersection = acc_intersection+1})
    }else{
      sv2.foreachActive( (index, value) => {if(sv1(index) != 0.0) acc_intersection = acc_intersection+1})
    }
    println("inter="+acc_intersection)
    println("union="+(sv1.numActives + sv2.numActives - acc_intersection))
    acc_intersection.toDouble / (sv1.numActives + sv2.numActives - acc_intersection).toDouble    
  }
  
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala-LSH").setMaster("local")
    val sc = new SparkContext(conf)

    /* *********************************************************************************************/
    
    val filesDirectoryPath = "/home/hadoop/files/results/test5000/matrix-disperse"
    val outDirectoryPath = "/home/hadoop/files/LSH/validation/"
    val numDocs = 229
    val numShigles = 9000000       
    
    /* *********************************************************************************************/
    printlnWithTime("Inicia")
      
    val file = sc.textFile(filesDirectoryPath)
    
    /*
     * Mapea archivo de entrada a (docId: Int, shingleId: Int).
     * Lo deja persistente ya que se va a consultar en el ciclo posterior 
     */
    val docIdShingleId =file.map{ x => 
      val array = x.replace("(","").replace(")","").replace(" ","").split(",") 
      (array(1).toInt, array(0).toInt)
    }.persist()
    
    // Matrix similar a una matriz de correlacion
    val m = DenseMatrix.zeros[Double](numDocs, numDocs)
    val listarray = new ArrayBuffer[Tuple3[Int, Int, Double]]()    
    
    /*
     * Por cada par de documentos calcula la distancia de jaccar y la guarda en la matriz de correlacion
     */
    for(idDocBase <- 0 until numDocs){
      
      //filtra las tuplas del documento, los guarda en un array, los ordena y crea el vector disperso
      val docA = docIdShingleId.filter(x => x._1 == idDocBase).map( x => x._2).persist()
      val arrayA = docA.collect().sortWith(_<_) 
      val vectorA: Vector = Vectors.sparse(numShigles, arrayA, arrayA.map { x => 1.0 }) 
      
      for(j <- idDocBase until numDocs){
        println("index="+idDocBase+","+j)        
        if(idDocBase == j) m(idDocBase, j) = 1.0
        else{
              
            val docB = docIdShingleId.filter(x => x._1 == j).map( x => x._2)
            val arrayB = docB.collect().sortWith(_<_)
            val vectorB: Vector = Vectors.sparse(numShigles, arrayB, arrayB.map { x => 1.0 }) 
            
            val sim = jaccard(vectorA, vectorB)
            println("sim=" + sim) 
            val sim2 = ((sim*100.0).round).toDouble/100.0
            m(idDocBase, j) = sim2
            //guarda solo los que tegan una similitud mayor a 0.3
            if(sim > 0.3) listarray += new Tuple3(idDocBase, j, sim2)
        }
          
      }
      docA.unpersist(false)
    }      
    
    println(m)
    
    //Guarda string en archivo
    sc.parallelize(Seq(m.toString())).saveAsTextFile(outDirectoryPath+"matrix")
    sc.parallelize(listarray).saveAsTextFile(outDirectoryPath+"list")
    
    printlnWithTime("Fin")
    sc.stop   
    
  }  
  
  
}