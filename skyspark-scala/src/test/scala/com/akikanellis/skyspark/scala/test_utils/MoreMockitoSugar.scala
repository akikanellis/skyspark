package com.akikanellis.skyspark.scala.test_utils

import org.mockito.Mockito._
import org.mockito.mock.SerializableMode.ACROSS_CLASSLOADERS
import org.scalatest.mock.MockitoSugar

import scala.reflect.Manifest

/**
  * Trait that provides some extra syntax sugar for <a href="http://mockito.org/" target="_blank">Mockito</a>.
  */
trait MoreMockitoSugar extends MockitoSugar {

  /**
    * Mocks the object with serialization across classloaders for usage within an Apache Spark job.
    * <p>
    * When running the tests with an IDE or the command line there is a chance that the classes will be serialized and
    * deserialized across different classloaders. This can cause a ClassNotFoundException when Spark tried to
    * serialize and then deserialize the class.
    * <p>
    * Using this method prevents such an issue.
    */
  def mockForSpark[T <: AnyRef](implicit manifest: Manifest[T]): T = {
    mock[T](withSettings().serializable(ACROSS_CLASSLOADERS))
  }
}
