package com.valuepartners.libs.spark.util

import java.nio.file.NoSuchFileException

import org.apache.spark.SparkContext

import scala.util.{Failure, Success, Try}

object Ut {

  def getRddFromFile(sc: SparkContext, path: String) = {
    Try {
      val filePath = this.getClass.getResource(path).getPath
      sc.textFile(filePath)
    } match {
      case Success(rdd) => rdd
      case Failure(err) => throw new NoSuchFileException(err.getMessage)
    }
  }
}
