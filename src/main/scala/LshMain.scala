import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.util.Calendar
import java.io.File

object LshMain {
  
  /**
   * Retorna si se va a correr el programa en modo depuracion.
   * En este modo se dejan codigo adicional que puede facetar el rendimiento. Ejemplo: guardar archivos
   */
    def isDebug(): Boolean = { true }
    
    /**
     * Imprime texto con la hora actual
     */
    def printlnWithTime(message: String): Unit = {
       val now = Calendar.getInstance()
       val time = now.get(Calendar.HOUR_OF_DAY).toString() + ":"+ now.get(Calendar.MINUTE).toString() + ":" + now.get(Calendar.SECOND).toString()
       println("["+time + "]:"+message)
    }    
    
    /**
     * Crea archivo con datos de prueba
     * (h0,D0,0), (h0,D1,0), ...
     */
    def createDataFile(sc: SparkContext, directoryOutput: String): Unit = {
       val array = new Array[(String, String)](200)     
      //key = hash,group,signature, h0, D1, 2
      for(i <- 0 until 20) {   
        for(j <- 0 until 10) { 
          array(i*10+j) = ("h"+i+",D"+j -> (i % 5).toString() )
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
      
      if(!hdfs){
        val file = new File(directoryOutput)
        file.listFiles().foreach { x => x.delete() }
        file.delete()       
      }else{
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)
        hdfs.delete(new org.apache.hadoop.fs.Path(directoryOutput), true)        
      }     
    }    
    
    def main(args: Array[String]): Unit = {    
      val conf = new SparkConf().setAppName("matrix-mult2").setMaster("local")
      val sc = new SparkContext(conf) 
            
      /* *********************************************************************************************/
      //CONFIGIRACION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

      //rutas de archivos y directorios
//      val mat1FilePath = "hdfs://localhost:8020/user/matrix/matriz_a.dat"
//      val mat2FilePath = "hdfs://localhost:8020/user/matrix/matriz_b-128.dat"
//      val directoryOutput = "hdfs://localhost:8020/user/matrix/out"
      val signaturesFilePath = "data/signaturesMat"
      val directoryOutput = "data/out"
//    val mat1FilePath = "/user/jonathan.medina/matrixMultiplication/matriz_a.dat"
//    val mat2FilePath = "/user/jonathan.medina/matrixMultiplication/matriz_b-128.dat"
//    val directoryOutput = "/user/jonathan.medina/out5"
//    val mat1FilePath = "/user/root/Matrices/matrizA.txt"
//    val mat2FilePath = "/user/root/Matrices/matrizB.txt"
//    val directoryOutput = "/user/jonathan.medina/out3"  
      /* *********************************************************************************************/
      

     //createDataFile(sc, directoryOutput) 
     deleteDirectoryOutput(directoryOutput, false)
     printlnWithTime("Inicia")
     
     val file = sc.textFile(signaturesFilePath)
     val map1 = file.zipWithIndex()
     if(isDebug) map1.foreach(println)
     
     map1.saveAsTextFile(directoryOutput)

     printlnWithTime("Fin")
      
     sc.stop       
      
    }
  
}