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
  "flickr.people.finyByUsername" -> List("username"))

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

  val searchFunctions = Map()

  // TODO: create a map that has all searchable objects in a list mapped to the flickr method
  // funcs()
  var flickrFunctions : Map[String, String] = Map()
  flickrFunctions += "people#user_id" -> "flickr.people.getInfo"

  def isGetOrBind(cmd: String): Boolean = {
    if (cmd equals "BIND")
      true
    else if (cmd equals "GET")
      true
    else false
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

  def getSearchablesByMethod: Map[String, List[String]] = {
    var searchables = Map[String, List[String]]()
    val functions = predicates.flatMap(_._2).flatMap(_._1)
    // obj, member
    val gets = queue.filter(e => isGet(e.pred)).map(e => getSepPred(e.pred))

    for ((obj, member) <- gets) {
      if (predicates(obj).flatMap(_._2).contains(member)) {

      }
    }
    for (f <- functions) {

    }

    searchables
  }

  def getSearchables: List[(String, String)] = {
    val searchables = queue.filter(e => isGet(e.cmd)).map(
      e => getSepPred(e.pred)).filter(
        e => predicates(e._1).contains(e._2))
    searchables

  def hasSearchables: Boolean = getSearchablesByMethod.nonEmpty

  /**
    * getSearchblesByMethod will return a list of functions mapped to search elements.
    * This will allow the application to aggregate calls to this function
    */
  def getSearchablesByMethod: Map[String, List[String]] = {
    var searchables = Map[String, List[String]]()

    for (op <- getGETCommands) {
      val (obj, member) = getSepPred(op)

      for (f <- predicates(obj)) {
        println(s"looking for ${f}")
        if (flickrFunctions(f).contains(member))
          if (searchables.contains(f))
            // FIXME: do we need this?
            searchables(f) +: member
          else {
            searchables += f -> List(member)
          }
      }
    }

    return searchables
  }

  def getParamsForFunc(func: String): Map[String, String] = {
    var result = Map[String, String]()

    for(param <- getSearchablesByMethod(func)) {
//      getGETCommands.filter(c => c.pred.contains(param)).map(c => c.obj)
      result += param -> getGETCommands.filter(c => c.pred.contains(param)).map(c => c.obj).head
    }

    result
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
      return
    }

    if (debug) {
      val numSearchables = searchables.toList.size
      println(s"We found ${numSearchables} searchables:")
      searchables.toList.foreach(x => println(s" - ${x}"))
    }

    /* automatically call all the functions corresponding to the GET ops */
    for (s <- searchables.keys) {
      val parameters = getParamsForFunc(s)
      println(s"Calling function: ${s}")
      val resp = f.call(methodName = s, parameters)
      addRespToMongo(getObjFromFunc(s), resp)
      if (debug)
        printResp(resp)
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
    fr.map.filterNot(elem => filterList.contains(elem)).foreach{case (k,v) => mongoObj += k -> v.asInstanceOf[String]}
    if (debug)
      fr.map.filterNot(elem => filterList.contains(elem)).foreach{case (k,v) => println(s"k: ${k} v: ${v}")}
    coll.insert(mongoObj)
  }

  /*
   * This should return an iterator of some kind to give the user the possibility to fetch as much data as he wants.
   *
   */
  def getResults() {

  }
}
