package com.joel.flink.model

/**+
 * Case class used as Apache Flink data type @{https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html}.
 *
 * @param vin Vehicle Identification Number
 * @param eseries E-Series indicating the product.
 * @param mileage Mileage of the vehicle.
 */
case class Vehicle(vin: String, eseries: String, mileage: Int)
