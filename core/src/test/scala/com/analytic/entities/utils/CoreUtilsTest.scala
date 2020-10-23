package com.analytic.entities.utils

import java.time.LocalDate
import org.scalatest.flatspec.AnyFlatSpec

class CoreUtilsTest extends AnyFlatSpec {

  "extractDate" should "return any date like pattern in the string" in {
    val expected = LocalDate.of(2000,12,24)
    val actual = CoreUtils.extractDate("any/url/2000/12/24/*/.avro")
    assertResult(expected)(actual)
  }
  it should "throw IllegalArgumentException on missing date like content" in {
    assertThrows[IllegalArgumentException]{
      CoreUtils.extractDate("any/url/20/12/24/*/.avro")
    }
  }

  "validateField" should "return a tuple with the same value and true if validator predicate holds [case String]" in {
    val inputField: String = "core/url"
    val fallback: String = "FallbackValue"
    val predicate: String => Boolean = _.startsWith("c")
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(inputField)(actual)
    assert(isValid)
  }
  it should "return a tuple with the same value and true if validator predicate holds [case Int]" in {
    val inputField: Int = 1
    val fallback: Int = -99
    val predicate: Int => Boolean = _ == 1
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(inputField)(actual)
    assert(isValid)
  }
  it should "return a tuple with the fallback value and false if validator predicate does not hold" in {
    val inputField: String = "core/url"
    val fallback: String = "FallbackValue"
    val predicate: String => Boolean = _.startsWith("h")
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(fallback)(actual)
    assert(!isValid)
  }
  it should "return a tuple with the fallback value and false if validator predicate throws Exception" in {
    val inputField: String = "core/url"
    val fallback: String = "FallbackValue"
    val predicate: String => Boolean = _ => { throw new Exception; true }
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(fallback)(actual)
    assert(!isValid)
  }
  it should "return a tuple with the same value [unwrapped] and true if validator predicate holds [case Option[String]]" in {
    val inputField: Option[String] = Some("core/url")
    val fallback: String = "FallbackValue"
    val predicate: String => Boolean = _.startsWith("c")
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(inputField.get)(actual)
    assert(isValid)
  }
  it should "return a tuple with the fallbackValue and true if input is None" in {
    val inputField: Option[Int] = None
    val fallback: Int = -99
    val predicate: Int => Boolean = _ == 1
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(fallback)(actual)
    assert(isValid)
  }
  it should "return a tuple with the fallback value if validator predicate does not hold [Option]" in {
    val inputField: Option[String] = Some("core/url")
    val fallback: String = "FallbackValue"
    val predicate: String => Boolean = _.startsWith("h")
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(fallback)(actual)
    assert(!isValid)
  }
  it should "return a tuple with the fallback value if validator predicate throws Exception [Option]" in {
    val inputField: Option[String] = Some("core/url")
    val fallback: String = "FallbackValue"
    val predicate: String => Boolean = _ => { throw new Exception; true }
    val (actual, isValid) = CoreUtils.validateField(inputField, predicate, fallback)
    assertResult(fallback)(actual)
    assert(!isValid)
  }

  "transformField" should "return a tuple with the transformed value and true on valid input [case String]" in {
    val inputField: String = "core/url"
    val expected = "url"
    val fallback: String = "FallbackValue"
    val transformer: String => String = _.split("/").last
    val (actual, isValid) = CoreUtils.transformField(inputField, transformer, fallback)
    assertResult(expected)(actual)
    assert(isValid)
  }
  it should "return a tuple with the transformed value and true on valid input [case Int]" in {
    val inputField: Int = 1
    val expected: Int = 13
    val fallback: Int = -99
    val transformer: Int => Int = _ + 12
    val (actual, isValid) = CoreUtils.transformField(inputField, transformer, fallback)
    assertResult(expected)(actual)
    assert(isValid)
  }
  it should "return a tuple with the fallback value and false if transformer throws Exception" in {
    val inputField: String = "core/url"
    val fallback: Char = 'f'
    val transformer: String => Char = _ => { throw new Exception; 'x' }
    val (actual, isValid) = CoreUtils.transformField(inputField, transformer, fallback)
    assertResult(fallback)(actual)
    assert(!isValid)
  }
  it should "return a tuple with the transformed value [unwrapped] and true on valid input [case Option[String]]" in {
    val inputField: Option[String] = Some("core/url")
    val expected = "url"
    val fallback: String = "FallbackValue"
    val transformer: String => String = _.split("/").last
    val (actual, isValid) = CoreUtils.transformField(inputField, transformer, fallback)
    assertResult(expected)(actual)
    assert(isValid)
  }
  it should "return a tuple with the fallbackValue and true if input is None" in {
    val inputField: Option[Int] = None
    val fallback: Char = 'z'
    val transformer: Int => Char = _ => 'x'
    val (actual, isValid) = CoreUtils.transformField(inputField, transformer, fallback)
    assertResult(fallback)(actual)
    assert(isValid)
  }
  it should "return a tuple with the fallback value if transformer throws Exception [Option]" in {
    val inputField: Option[String] = Some("core/url")
    val fallback: Char = 'p'
    val transformer: String => Char = _ => { throw new Exception; 'x' }
    val (actual, isValid) = CoreUtils.transformField(inputField, transformer, fallback)
    assertResult(fallback)(actual)
    assert(!isValid)
  }

  "printJSON" should "return case class as JSON formatted string" in {
    import io.circe.generic.auto._
    case class testClass(a: String, b: String)
    val output = CoreUtils.printJSON(testClass("fieldA", "fieldB"))
    val expected = """{"a":"fieldA","b":"fieldB"}"""
    assertResult(expected)(output.replaceAll(" +|\n", ""))
  }

  "prettyPathFiles" should "return valid list of partitions as string [orcl schema]" in {
    val expected: String = "gs://bucket-name/orcl/country/sid/schema/table/data/{2020-06-07,2020-06-08,2020-06-09,2020-06-10,2020-06-11,2020-06-12}"

    val paths: String = "gs://bucket-name/orcl/country/sid/schema/table/data/2020/06/07,gs://bucket-name/orcl/country/sid/schema/table/data/2020/06/08,gs://bucket-name/orcl/country/sid/schema/table/data/2020/06/09,gs://bucket-name/orcl/country/sid/schema/table/data/2020/06/10,gs://bucket-name/orcl/country/sid/schema/table/data/2020/06/11,gs://bucket-name/orcl/country/sid/schema/table/data/2020/06/12"
    val inputPaths: Array[String] = paths.split(",")
    val actual: String = CoreUtils.prettyPathFiles(paths)
    assertResult(expected)(actual)
  }
  it should "return valid list of partitions as string [adobe schema]" in {
    val expected: String = "gs://bucket-name/adobe/table/country/{2020-05-01,2020-05-02,2020-05-03,2020-05-04,2020-05-05,2020-05-06}"

    val paths: String = "gs://bucket-name/adobe/table/country/2020/05/01/,gs://bucket-name/adobe/table/country/2020/05/02/,gs://bucket-name/adobe/table/country/2020/05/03/,gs://bucket-name/adobe/table/country/2020/05/04/,gs://bucket-name/adobe/table/country/2020/05/05/,gs://bucket-name/adobe/table/country/2020/05/06/"
    val actual: String = CoreUtils.prettyPathFiles(paths)
    assertResult(expected)(actual)
  }

}
