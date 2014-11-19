package de.l3s.sparql2flickr.query

import scala.util.matching.Regex

import de.l3s.flickr4scala.flickr._
import de.l3s.flickr4scala.ConfigParser
import java.io.File
import java.lang.RuntimeException
import com.mongodb.casbah.Imports._
import scala.util.Random

/**
 * This will use the actual Flickr API via Flickr4Scala
 */
class FlickrQueryExecutor(queue: List[Op], flickrConfig: File, debug: Boolean = true) {
  // identifier for the query, this might become a map, if we get
  // multiple queries in each other
  val ident = new Random().nextString(8)
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
    "group_id",
    "text"
  ))

  val groupSearchOptions = Map("flickr.groups.search" -> List(
    "text"
  ))

  val contactSearchoption = Map("flickr.contacts.getPublicList" -> List(
    "user_id"
  ))

  val gallerieSearchOption = Map("flickr.galleries.getLis" -> List(
    "user_id"
  ))

  val commentSearchOption = Map("flickr.photos.comments.getList" -> List(
    "photo_id"
  ))

  predicates += "people" -> peopleSearchOptions
  predicates += "photos" -> photoSearchOptions
  predicates += "groups" -> groupSearchOptions
  predicates += "contacts" -> contactSearchoption
  predicates += "gallerie" -> gallerieSearchOption
  predicates += "comment" -> commentSearchOption

  /* we basically pretend that i.e. username is an element of photo, even tho
   * you can only search for the "user_id" and get an "owner" returned
   *
   * this is for auto conversion from "useless" to usefull parameter for the Flickr API
   */
  val inputMapping = Map("username" -> "user_id")
  // this is a mapping to get the user the wished results
  val outputMapping = Map("username" -> "owner")

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

  def isGet(op: Op) = if (op.cmd equals "GET") true else false
  def isBind(op: Op) = if (op.cmd equals "BIND") true else false
  def isPrint(op: Op) = if (op.cmd equals "PRINT") true else false
  def isExtBind(op: Op) = if (op.cmd equals "EXTBIND") true else false
  def isAggr(op: Op) = if (op.cmd equals "AGGR") true else false

  def getGETCommands = queue.filter(op => isGet(op))
  def getAggrOps = queue.filter(op => isAggr(op))
  def getPrintOps = queue.filter(op => isPrint(op))

  def hasAggregates = getAggrOps.nonEmpty

  /**
    *  This method returns a list of all bind operations.
    */
  def getBindOps: List[Op] = queue.filter(op => isBind(op))

  /**
    * This method returns a list of all extend bind operations. Those
    * are operations that bind a variable to the output of an
    * aggregate function.
    */
  def getExtBindOps: List[Op] = queue.filter(op =>isExtBind(op))

  /**
    * The method will return the 'photos' part from the function
    * string 'flickr.photos.search'.
    */
  def getObjFromFunc(func: String): String = {
    val objExp = """\w+\.(\w+)\.\w""".r
    val obj = objExp.findFirstIn(func) match {
      case Some(objExp(obj)) => obj
      case None => ""
    }

    obj
  }

  /**
    * getSepPred returns a tuple consisting of the object and the
    * member of a predicate. i.e.: photos#text => (photos,text)
    */
  def getSepPred(op: Op): (String, String) = {
    val predExp = """(.*)#(.*)""".r
    // check if the relevant part of the predicate is a
    // possible search option in the flickr API
    val (obj, member) = predExp.findFirstIn(op.pred) match {
      case Some(predExp(u, m)) => (u, m)
      // TODO: throw error here!
      case None => ("", "")
    }

    // TODO: obj needs to be only "people" etc.
    (obj, member)
  }

  /**
    * Only return the name of the member specified in the SPARQL query, i.e.:
    * only return 'username' when 'people#username' was specified.
    */
  def getMemberName(op: Op): String = getSepPred(op)._2

  /**
    * Returns true if there is at least one searchable element in the query,
    * false otherwise.
    */
  def hasSearchables: Boolean = getSearchablesByMethod.nonEmpty

  /**
    * getSearchblesByMethod will return a list of functions mapped to search
    * elements.
    * This will allow the application to aggregate calls to this function.
    * It also takes into account mapping like username -> user_id.
    */
  def getSearchablesByMethod: Map[String, List[String]] = {
    if (searchables.nonEmpty)
      return searchables

    for (op <- getGETCommands) {
      val (obj, member) = getSepPred(op)
      // this goes through all the functions in predicates
      for (f <- predicates(obj).keys) {
        println(s"looking for ${f}")
        // or if a mapping for the member exists!
        if (predicates(obj)(f).contains(member)) {
          if (searchables.contains(f))
            // FIXME: do we need this?
            searchables(f) :+ member
          else
            searchables += f -> List(member)
        // mapping part
        } else if (predicates(obj)(f).contains(inputMapping(member))) {
          if (searchables.contains(f))
            // FIXME: do we need this?
            searchables(f) :+ member
          else
            searchables += f -> List(member)
        } else {
          if (debug) {
            println(s"Found unsearchable:")
            println(unsearchables)
          }
          unsearchables :+ op
        }
      }

      searchables
    }

    return searchables
  }

  /**
    * getUnsearchables returns a list of Ops that can't be searched
    * via Flickrs REST API.
    */
  def getUnsearchables: List[Op] = {
    if (unsearchables.isEmpty)
      getSearchablesByMethod

      unsearchables
  }

  /*
   * This method tranforms the input obj into a new obj value for another
   * (mapped) param.
   */
  def getMappedObj(param: String, obj: String) = (param, obj) match {
    case ("username", username) => getUserId(username=username)
    case (_, _) => obj
  }

  /*
   * This method gets the list of parameters for the Flickr API call and also
   * creates necessary mapping of members and executes additional API calls as
   * needed.
   */
  def getParamsForFunc(func: String, searchables: Map[String, List[String]]): Map[String, String] = {
    var result = Map[String, String]()

    for(param <- searchables(func)) {
      if (inputMapping.contains(param)) {
        if (debug)
          println(s"Execute mapping for: ${param}")
        val mappedParam = inputMapping(param)
        val obj = getGETCommands.filter(
          c => c.pred.contains(param)).map(c => c.obj).head
        val mappedObj = getMappedObj(param, obj)
        result += mappedParam -> mappedObj
      } else
        result += param -> getGETCommands.filter(
          c => c.pred.contains(param)).map(c => c.obj).head
    }

    result
  }

  /**
    * The getMappedparamsForFunc method returns a map of (param ->
    * value) arguments for a specific function of the Flickr REST API.
    */
  def getMappedParamsForFunc(func: String): Map[String,String] = {
    var result = Map[String, String]().empty

    for (param <- searchables(func)) {
      if (inputMapping.contains(param)) {
        val v = getGETCommands.filter(
          c => c.pred.contains(param)
        ).map(c => c.obj).head
        result += param -> v
      }
    }

    result
  }

  def checkForProperExtBind(printVar: String): Boolean = {
    // list of Ops binding a PRINT variable to a temp variable like ?.0
    val extBinds = getExtBindOps.filter(op => op.obj == printVar)

    for (bindOp <- extBinds) {
      // list of temp variables like ?.0 to a function and a variable
      // name from the where clause
      val aggrOps = getAggrOps.filter(op => op.subj == bindOp.subj)

      if (aggrOps.size > 1) {
        println("Too many bindings from EXTBIND to AGGR.")
        return false
      }

      if (aggrOps.size == 0)
        return false

      // FIXME: what if it isn't a bind to obj but to subj?
      if (!getBindOps.exists(op => op.obj == aggrOps.head.obj)) {
        return false
      }
    }

    true
  }

  /**
    * The checkOutputFields method checks if all the requested output
    * fields are actually bound to a value
    *
    * Is this actually part of the shitty SPARQL query standard?

    * TODO: Extend check to compensate for aggregate functions!
    *       basically search also in the EXTBIND part
    */
  def checkOutputFields: Boolean = {
    val binds = getBindOps
    val extBinds = getExtBindOps
    for (field <- getPrintOps.map(op => op.obj)) {
      if (!binds.exists(op => op.obj == field))
        if (!extBinds.exists(op => op.obj == field))
          return false
        else
          if (!checkForProperExtBind(field))
            return false
    }

    return true
  }

  /**
    * The getFieldsObj method returns the MongoDBObject needed to
    * limit the number of "elements" returned by the MongoDB query.
    * FIXME: This should also be dynamic in regards of the part of the
    * query for nested SELECT statements.
    */
  def getFieldsObj: MongoDBObject = {
    // list of fields, excluding all those that need to be mapped to another value
    val fields = getPrintOps.map(op => op.obj)

    if (debug) {
      println("Fields:")
      println(fields)
    }
    // also generally exclude the '_id' field, it's useless for us!
    MongoDBObject(fields.map(f => f -> 1) ++ Map("_id" -> 0))
  }

  /* getFilterObj returns a MongoDBObject which is configured for a
   * collection to search for all remaining subjects.
   */
  def getFilterObj(pred: String): MongoDBObject = {
    val queryData = unsearchables.filter(
      op => op.pred.contains(pred)).map(
      op => getMemberName(op) -> op.obj) :+ "ident" -> ident

    val mongoObj = MongoDBObject.empty
    queryData.foreach(d => mongoObj += d)

    if (debug) {
      println("Filters:")
      println(queryData.toMap.keys)
    }

    mongoObj
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

    if (!checkOutputFields) {
      println("Not all output fields are bound to a variable!")
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
      println("Get mapped params:")
      println(getMappedParamsForFunc(func))

      if (debug) {
        println("Parameters:")
        println(parameters)
        println(s"Calling function: ${func}")
      }

      val resp = f.call(methodName = func, parameters ++ getMappedParamsForFunc(func))
      if (!resp.map.contains("code"))
        addRespToMongo(getObjFromFunc(func), resp)
      else
        println("Error occured, number: " + resp.code)
    }
  }

  /*
   *  Get user_id from username or email address.
   */
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

  /**
    * 'fr' is the complete FlickrResponse received by the Flickr API.
    * addRespToMongo will etiher split the response if there is an
    * array with embedded FlickrResponse's or leave it as it is and
    * call insertResp(...)
    */
   def addRespToMongo(obj: String, fr: FlickrResponse) = {
    val coll = flickrDB(obj)
    val filterList = List("stat", "_id")

    // save resp with no array
    if (fr.data.isEmpty) {
      insertResp(coll, fr)
      // save resp with array
    } else {
      // FIXME: we assume that there is only one array, mostly true tho
      for (elem <- fr.data.head._2) {
        insertResp(coll, elem)
      }
    }
  }

  /**
    * The insertResp() method saves the returned FlickrResponse to the
    * database collection and filters out some useless elements.
    */
  def insertResp(coll: MongoCollection, fr: FlickrResponse) = {
    val filterList = List("stat", "_id")
    var mongoObj = MongoDBObject()

    if (fr.map.isEmpty)
      println("Received empty FlickrResponse to save to db!")

    // set the query identifier in the FlickrResponse
    fr.ident = ident

    fr.map.filterNot(elem => filterList.contains(elem._1)).foreach{
      case (k,v) => mongoObj += k -> getMongoDBObj(k, v.asInstanceOf[String]).get(k)
    }


    if (debug)
      fr.map.filterNot(elem => filterList.contains(elem._1)).foreach{
        case (k,v) => println(s"k: ${k} v: ${v}")}

    coll.insert(mongoObj)
  }

  /*
   * This should return an iterator of some kind to give the user the
   * possibility to fetch as much data as he wants.
   *
   * TODO: Take into account the aggregate functions!
   */
  def getResults(format: String = "json") {
    // if the query is a "SELECT * ..." fields will be empty which
    // will result in all the data being returned
    val fields = getFieldsObj
    val filter = getFilterObj("photos")
    val coll = flickrDB("photos")

    if (hasAggregates) {
      val ret = coll.aggregate(MongoDBObject(
        "$group" -> MongoDBObject(
          "_id" -> "$id",
          "test" -> MongoDBObject("$sum" -> "$server")
        )))

      println("print coll:")
      for (x <- ret.results) {
        println(x)
      }

    } else {
      for (f <- coll.find(filter, fields)) {
        println(f)
        println(f.get("tags")) }
    }
  }

  /* Type conversion with automatic conversion to Option[T] */
  case class ParseOp[T](op: String => T)
  implicit val popDouble = ParseOp[Double](_.toDouble)
  implicit val popInt = ParseOp[Int](_.toInt)

  def parse[T: ParseOp](s: String): Option[T] = try {
    Some(implicitly[ParseOp[T]].op(s))
  } catch {
    case _: RuntimeException => None
  }

  /**
    * getMongoDBObj will try to find the actual type of the data in
    * the data value and call createMongoDBObj to create the fitting
    * MongoDBObject with the obtained type information.
    */
  def getMongoDBObj(attr: String, data: String): MongoDBObject = {
    if (parse[Double](data) != None) {
      return createMongoDBObj[Double](attr, parse[Double](data).get)
    }

    if (parse[Int](data) != None) {
      return createMongoDBObj[Int](attr, parse[Int](data).get)
    }

    return createMongoDBObj[String](attr, data)
  }

  def createMongoDBObj[T](attr: String, data: T): MongoDBObject = {
//    println(data.getClass())
//    println(data)
    return MongoDBObject(attr -> data)
  }
}
