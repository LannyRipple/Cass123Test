package com.spotright.cass123test

import scala.util.Random

import java.util.UUID

object AddData {

  def main(av: Array[String]) {
    val count = av.lift(0).map{n => math.pow(2.0, n.toInt).toInt}.getOrElse(1)

    printFile(count)
  }

  def printFile(count: Int) {
    println("use " + Config.keyspace + ";")
    genColumns(count)
    println("quit;")
  }

  def genColumns(count: Int) {
    val key = new java.util.UUID(Random.nextLong(), Random.nextLong())

    for (i <- 0 until count) {
      val colname = new java.util.UUID(Random.nextLong(), Random.nextLong())

      val n = Random.nextGaussian().abs.toInt
      val value = (i :: List.fill(n){Random.nextInt()}).mkString(":")

      println(mkInsert(key, colname, value))
    }
  }

  def mkInsert(key: UUID, colname: UUID, value: String): String =
    "set " + Config.cfname + "['" + key + "']['" + colname + "'] = utf8('" + value + "');"
}
