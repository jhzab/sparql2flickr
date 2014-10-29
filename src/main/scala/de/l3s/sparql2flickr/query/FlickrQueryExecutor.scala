package de.l3s.sparql2flickr.query

import scala.util.matching.Regex

import de.l3s.flickr4scala.flickr._
import de.l3s.flickr4scala.ConfigParser
import java.io.File
import java.lang.RuntimeException
import com.mongodb.casbah.Imports._

/**
 * This will use the actual Flickr API via Flickr4Scala
 */
class FlickrQueryExecutor(queue: List[Op], flickrConfig: File, debug: Boolean = true) {
  var predicates = Map[String, Map[String, List[String]]]()

  val mongoClient = MongoClient("localhost", 27017)
  val flickrDB = mongoClient("flickr")
  val f = new ConfigParser(flickrConfig).init()

  val peopleSearchOptions = Map("flickr.people.findByEmail" -> List(
    "find_email"
  ),
  "flickr.people.findByUsername" -> List("username"))

  val photoSearchOptions = Map("flickr.photos.search" -> List(
    "user_id",
    "owner",
    "text",
    "min_taken_date",
    "max_taken_date",
    "min_upload_date",
    "max_upload_date",
    "group_id"
  ))

  val groupSearchOptions = Map("flickr.groups.search" -> List(
    "text"
  ))

  predicates += "people" -> peopleSearchOptions
  predicates += "photo" -> photoSearchOptions
  predicates += "group" -> groupSearchOptions

  var searchables = Map[String, List[String]]()
  var unsearchables = List[Op]()
  var filter = List[Op]()

  def isGetOrBind(op: Op): Boolean = {
    if (op.cmd equals "BIND")
      true
    else if (op.cmd equals "GET")
      true
    else false
  }

  def isGet(op: Op): Boolean = {
    if (op.cmd equals "GET")
      true
    else
      false
  }

  def isBind(op: Op): Boolean = {
    if (op.cmd equals "BIND")
      true
    else
      false
  }

  def getGETCommands = queue.filter(op => isGet(op))

  def getObjFromFunc(func: String): String = {
    val objExp = """\w+\.(\w+)\.\w""".r
    val obj = objExp.findFirstIn(func) match {
      case Some(objExp(obj)) => obj
      case None => ""
    }

    obj
  }

  def getSepPred(op: Op): (String, String) = {
    val predExp = """(.*)#(.*)""".r
    // check if the relevant part of the predicate is a
    // possible search option in the flickr API
    val (obj, member) = predExp.findFirstIn(op.pred) match {
      case Some(predExp(u, m)) => (u, m)
      // TODO: throw erroor here!
      case None => ("", "")
    }

    // TODO: obj needs to be only "people" etc.
    (obj, member)
  }

  /**
    * Returns true if there is at least one searchable element in the query,
    * false otherwise.
    */
  def hasSearchables: Boolean = getSearchablesByMethod.nonEmpty

  /**
    * getSearchblesByMethod will return a list of functions mapped to search elements.
    * This will allow the application to aggregate calls to this function
    */
  def getSearchablesByMethod: Map[String, List[String]] = {
    if (searchables.nonEmpty)
      return searchables

    for (op <- getGETCommands) {
      val (obj, member) = getSepPred(op)
      unsearchables :+ op
      // this goes through all the functions in predicates(obj)
      for (f <- predicates(obj).keys) {
        println(s"looking for ${f}")
        if (predicates(obj)(f).contains(member)) {
          if (searchables.contains(f))
            // FIXME: do we need this?
            searchables(f) +: member
          else {
            searchables += f -> List(member)
          }

          unsearchables = unsearchables.filterNot(elem => elem == op)
        }
      }
    }

    return searchables
  }

  def getUnsearchables: List[Op] = {
    if (unsearchables.nonEmpty) {
      return unsearchables
    } else
      getSearchablesByMethod
      return unsearchables
  }

  def getParamsForFunc(func: String, searchables: Map[String, List[String]]): Map[String, String] = {
    var result = Map[String, String]()

    for(param <- searchables(func)) {
      result += param -> getGETCommands.filter(c => c.pred.contains(param)).map(c => c.obj).head
    }

    result
  }

  /**
    *  This method will do the filtering for all GET commands that could not be
    *  searched for via Flickr API.
    */
  def filterUnsearchables: Unit = {
    /* build a select query here for all the data we need, output only the part in the BINDs */
    val builder = MongoDBObject.newBuilder
    builder += "username" -> "zabjanhendrik"
    val query = builder.result
    println("query: " + query)
  }

  def applyFilter: Unit = {

  }

  /**
    * execute() runs the commands in the given queue to obtain the result and
    *  save in the MongoDB database
    */
  def execute: Unit = {
    val getCommands = getGETCommands
    val searchables = getSearchablesByMethod

    if (!hasSearchables) {
      println("Searching isn't supported for any of the given predicates!")
      // FIXME: throw error!
      return
    }

    if (debug) {
      val numSearchables = searchables.toList.size
      println(s"We found ${numSearchables} searchables:")
      searchables.toList.foreach(x => println(s" - ${x}"))
    }

    /* automatically call all the functions corresponding to the GET ops */
    for (func <- searchables.keys) {
      val parameters = getParamsForFunc(func, searchables)

      if (debug)
        println(s"Calling function: ${func}")

      val resp = f.call(methodName = func, parameters)
      if (!resp.map.contains("code"))
        addRespToMongo(getObjFromFunc(func), resp)
      else
        println("Error occured, number: " + resp.code)

      if (debug)
        printResp(resp)

      // FILTER DATA
      filterUnsearchables
    }

    // TODO: apply FILTER and unsearchable GETs
  }

  def printResp(resp: FlickrResponse): Unit = {
    for (elem <- resp.map.keys) {
      println(s"elem: ${elem} value: ${resp.map(elem)}")
    }
  }

  def getUserId(username: String = "", email: String = ""): String = {
    try {
      if (!(username equals "")) {
        val resp = f.call(methodName = "flickr.people.findByUsername",
          parameters = Map("username" -> username))
        resp.id.asInstanceOf[String]
      } else if (!(email equals "")) {
        val resp = f.call(methodName = "flickr.people.findByEmail",
          parameters = Map("find_email" -> email))
        resp.id.asInstanceOf[String]
      } else {
        // FIXME: throw error
        ""
      }
    } catch {
      case e: RuntimeException => {
        e.printStackTrace
        e.toString
      }
    }
  }

  // fr is an element from the returned array
  def addRespToMongo(obj: String, fr: FlickrResponse) = {
    val coll = flickrDB(obj)
    val mongoObj = MongoDBObject()

    val filterList = List("stat")
    fr.map.filterNot(elem => filterList.contains(elem)).foreach{
      case (k,v) => mongoObj += k -> v.asInstanceOf[String]}
    if (debug)
      fr.map.filterNot(elem => filterList.contains(elem)).foreach{
        case (k,v) => println(s"k: ${k} v: ${v}")}
    coll.insert(mongoObj)
  }

  /*
   * This should return an iterator of some kind to give the user the possibility to fetch as much data as he wants.
   *
   */
  def getResults() {

  }
}
