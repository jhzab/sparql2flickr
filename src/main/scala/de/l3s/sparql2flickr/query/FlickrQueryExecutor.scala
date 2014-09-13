package de.l3s.sparql2flickr.query

import scala.util.matching.Regex

/**
 * This will use the actual Flickr API via Flickr4Scala
 */
class FlickrQueryExecutor(queue: List[Op]) {
  var predicates : Map[String, List[String]] = Map()

  val peopleSearchOptions = List(
  "username",
  "email"
  )

  val photoSearchOptions = List(
  "user_id",
  "owner",
  "min_taken_date",
  "max_taken_date",
  "group_id"
  )

  predicates += "people" -> peopleSearchOptions
  predicates += "photo" -> photoSearchOptions

  def isGetOrBind(cmd : String): Boolean = {
    if (cmd equals "BIND")
      true
    else if (cmd equals "GET")
      true
    else false
  }

  def getSepPred(pred : String) : (String, String) = {
    val predExp = """(.*)#(.*)""".r
    // check if the relevant part of the predicate is a
    // possible search option in the flickr API
    val (_obj, _member) = predExp.findFirstIn(pred) match {
      case Some(predExp(u, m)) => (u, m)
      // TODO: throw erroor here!
      case None => ("", "")
    }

    // TODO: obj needs to be only "people" etc.
    (_obj, _member)
  }

  def getSearchables : List[String] = {
    var searchables = List[String]()

    for (elem <- queue) {
      if (isGetOrBind(elem.cmd)) {
        val (obj, member) = getSepPred(elem.pred)
        if (predicates(obj).contains(member)) {
          searchables = elem.pred :: searchables
        }
      }
    }

    searchables
  }

  // return the corresponding search function in the Flickr API for the given
  // obj and member of the predicate
  def getFlickrFunc(obj : String, member : String) : String = {
    "test"
  }

  def execute(debug : Boolean = false) : Unit = {
    // Check that the predicates are searchable via Flickr API
    // TODO: and are actually correct predicates!
    val searchables = getSearchables
    if (searchables.size equals 0) {
      println("Searching isn't supported for any of the given predicates!")
      return
    }

    if (debug) {
      println(s"We found ${searchables.size} searchables:")
      searchables.foreach(x => println(s"\t${x}"))
    }

    // check that at least one GET command is a searchable
    // otherwise there wont be any string etc. to search for!
    var hasSearchableGet = false
    for (e <- queue.filter(e => e.cmd equals "GET")) {
      if (searchables.filter(m => m equals e.pred).size > 0) {
        hasSearchableGet = true
      }
    }

    if (!hasSearchableGet) {
      println("No searchable predicate found in any GET command!")
      return
    }

    // execute the GET part and dump the data in the database
    for (e <- queue.filter(e => e.cmd equals "GET")) {
      if (searchables.contains(e.pred)) {
        val (obj, member) = getSepPred(e.pred)
        val func = getFlickrFunc(obj, member)
      }
    }
  }

  /*
   * This should return an iterator of some kind to give the user the possibility to fetch as much data as he wants.
   *
   */
  def getResults() {

  }
}
