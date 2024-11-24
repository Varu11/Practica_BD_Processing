package examen

import examen.examen.{ejercicio1, ejercicio2, ejercicio3, ejercicio4, ejercicio5}
import org.apache.spark.sql.DataFrame
import Utils.TestInit

class examenTest extends TestInit
import  spark.implicits

//---------- EJERCICIO 1----------

val estudiantes = List(
  Row("Raúl", "Gonzalez", 26, 7.5),
  Row("Iker", "Casillas", 22, 6.2),
  Row("Amaya", "Valdemoro", 23, 8.1),
  Row("Alba", "Torrens", 25, 6.4),
  Row("Paula", "Badosa", 21, 7.1),
  Row("Pau", "Gasol", 23, 8.3),
  Row("Alexia", "Putellas", 22, 7.6),
  Row("Olga", "Redondo", 24, 6.6),
  Row("Aitana", "Bonmati", 24, 5.9),
  Row("Sergio", "Llull", 25, 7.0)
).toDF("nombre", "apellido", "edad", "nota")

"ejercicio1" should "Ordenar notas del DataFrame" in {
  val resultado = ejercicio1(estudiantes)(spark)
  
  resultado.show
}


//---------- EJERCICIO 2 ----------

val numerosDF = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12).toDF("numero")

"ejercicio2" should "Crear un DF que diga si cada número de otro DF es par o impar" in {

  val resultadoDF = ejercicio2(numerosDF)(spark)

  resultadoDF.show()
}


//---------- EJERCICIO 3 ----------

"ejercicio3" should "Crear un DF que muestre las medias de las calificaciones uniendo dos DF, una con nombres y otra con calificaciones" in {

  val nombreID = Seq(
    (1, "Raul"),
    (2, "Iker"),
    (3, "Amaya"),
    (4, "Alba"),
    (5, "Paula"),
    (6, "Pau"),
    (7, "Alexia"),
    (8, "Olga"),
    (9, "Aitana"),
    (10, "Sergio")
  ).toDF("ID", "Nombre")
  val asignaturasNotas = Seq(
    (1, "Ingles", 8.7),
    (1, "Lengua", 5.5),
    (2, "Ingles", 6.2),
    (2, "Lengua", 4.9),
    (3, "Ingles", 7.3),
    (3, "Lengua", 8.6),
    (4, "Ingles", 9.5),
    (4, "Lengua", 6.5),
    (5, "Ingles", 5.6),
    (5, "Lengua", 4.8),
    (6, "Ingles", 7.4),
    (6, "Lengua", 7.6),
    (7, "Ingles", 4.2),
    (7, "Lengua", 7.2),
    (8, "Ingles", 7.4),
    (8, "Lengua", 7.3),
    (9, "Ingles", 9.1),
    (9, "Lengua", 6.9),
    (10, "Ingles", 9.8),
    (10, "Lengua", 5.8),
  ).toDF("ID", "Asignatura", "Notas")

  val media = ejercicio3(nombreID, asignaturasNotas)

  media.show

}


//---------- EJERCICIO 4 ----------
"ejercicio4" should "Contar las palabras que se repiten en esta cadena" in {

  val palabras = List("perro", "gato", "caballo", "perro", "vaca", "aguila", "tiburon", "caballo", "oveja","gato", "perro", "cabra")

  val resultadoRDD = ejercicio4(palabras)(spark)


  resultadoRDD.collect().foreach { case (palabra, cuenta) =>
    println(s"$palabra: $cuenta")

  }
}


//---------- EJERCICIO 5 ----------

"ejercicio5" should "Calcula el ingreso total del csv ventas.csv" in {

  //Leemos el archivo csv y lo importamos en la variable path

  val ventas: DataFrame = spark.read.option("header", true)
    .csv("C:/Users/Usuario/Escritorio/KEEP_CODING/CURSOS/4.Procesamiento_datos/Practica/resources/examen/ventas.csv")

  //Ejecutamos

  val resultado = ejercicio5(ventas)(spark)

  resultado.show()
}
}