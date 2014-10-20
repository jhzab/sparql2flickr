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
  var predicates : Map[String, List[String]] = Map()
  val mongoClient = MongoClient("localhost", 27017)
  val flickrDB = mongoClient("flickr")
  val f = new ConfigParser(flickrConfig).init()

  val peopleSearchOptions = List("username",
  "email"
  )

  val photoSearchOptions = List(
  "user_id",
  "owner",
  "min_taken_date",
  "max_taken_date",
  "group_id"
  )

  val groupSearchOptions = List(
    "text"
  )

  predicates += "people" -> peopleSearchOptions
  predicates += "photo" -> photoSearchOptions
  predicates += "group" -> groupSearchOptions

  var flickrFunctions : Map[String, String] = Map()
  flickrFunctions += "people#user_id" -> "flickr.people.getInfo"

  def isGet(cmd: String) = if (cmd equals "GET") true else false

  def isGetOrBind(cmd : String): Boolean = {
    if (cmd equals "BIND")
      true
    else if (cmd equals "GET")
      true
    else false
  }

  def getSepPred(pred : String): (String, String) = {
    val predExp = """(.*)#(.*)""".r
    // check if the relevant part of the predicate is a
    // possible search option in the flickr API
    val (obj, member) = predExp.findFirstIn(pred) match {
      case Some(predExp(u, m)) => (u, m)
      // TODO: throw erroor here!
      case None => ("", "")
    }

    // TODO: obj needs to be only "people" etc.
    (obj, member)
  }

  def getSearchables: List[(String, String)] = {
    var searchables = List[(String, String)]()

    for (elem <- queue) {
      if (isGet(elem.cmd)) {
        val (obj, member) = getSepPred(elem.pred)
        if (predicates(obj).contains(member)) {
          searchables = (obj, member) :: searchables
        }
      }
    }

    searchables
  }

  // return the corresponding search function in the Flickr API for the given
  // obj and member of the predicate
  def getFlickrFunc(obj : String, member : String): String = {
    "test"
  }

  def execute: Unit = {
    // Check that the predicates are searchable via Flickr API
    // TODO: and are actually correct predicates!
    val searchables = getSearchables
    if (searchables.size equals 0) {
      println("Searching isn't supported for any of the given predicates!")
      return
    }

    if (debug) {
      println(s"We found ${searchables.size} searchables:")
      searchables.foreach(x => println(s" - ${x}"))
    }

    var unsearchables: List[(String, String)] = Nil
    // execute the GET part and dump the data in the database
    for (e <- queue.filter(e => e.cmd equals "GET")) {
      // TODO: optimize so search for multiple members at once! :)
      if (searchables.contains(e.pred)) {
        val (obj, member) = getSepPred(e.pred)
        val func = getFlickrFunc(obj, member)

        if ((obj, member) equals ("people", "username")) {
          val fr = f.call(methodName = "flickr.people.getInfo",
            parameters = Map("user_id" -> getUserId(username = e.obj)))

          addRespToMongo(obj, fr)
        } else {

        }
      } else {
        // save unsearchable GETs to filter them later when the FILTER function is also applied
        unsearchables = getSepPred(e.pred) :: unsearchables
      }
    }

    // TODO: apply FILTER and unsearchable GETs
  }

  // get all predicates that can be searched with one flickr function together

  // do the actual search

  // filter the values
  // - either when saving
  // - or when returning the data from the database

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

    fr.map.foreach{case (k,v) => mongoObj += k -> v.asInstanceOf[String]}
    //debug
    fr.map.foreach{case (k,v) => println(s"k: ${k} v: ${v}")}
    coll.insert(mongoObj)
  }

  /*
   * This should return an iterator of some kind to give the user the possibility to fetch as much data as he wants.
   *
   */
  def getResults() {

  }
}
