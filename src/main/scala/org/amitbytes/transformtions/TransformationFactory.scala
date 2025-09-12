package org.amitbytes.transformtions

object TransformationFactory {
  def getTransformation(transformationType: String): BaseTransformation = {
    transformationType.toLowerCase match {
      case "custom" => new CustomTransformation()
      case "customcity" => new CustomCityTransforamtion()
      case "default" => new DefaultTransformation()
      case _ => throw new IllegalArgumentException(s"Unknown transformation type: $transformationType")
    }
  }
}
