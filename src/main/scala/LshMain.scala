import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LshMain {
  
  /**
   * Retorna si se va a correr el programa en modo depuracion.
   * En este modo se dejan codigo adicional que puede facetar el rendimiento. Ejemplo: guardar archivos
   */
    def isDebug(): Boolean = { false }
    
    def main(args: Array[String]): Unit = {    
      val conf = new SparkConf().setAppName("matrix-mult2").setMaster("local")
      val sc = new SparkContext(conf) 
      
      println("hola")
            
      /* *********************************************************************************************/
      
    }
  
}