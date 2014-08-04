package de.l3s.sparql2flickr

/**
 * Created by gothos on 7/28/2014.
 */

import scala.collection.JavaConversions._

import com.hp.hpl.jena.sparql.algebra.op._
import com.hp.hpl.jena.graph.Triple

/* Implement a queue. GET data with x=$y, filter x for something and output z */
class FlickrQueryConstructor {
  var queue = List[String]()
  // if subject and object begin with '?' it's just naming a variable for later use
  // if object(?) is in quotation marks, it's a "filter". the actual filter comes later in the syntax tho,
  // but we might want to execute it before to minimize API load
  def addBGP(op : OpBGP) {

    def handleTriple(t : Triple) {
      // this is the
      //println(t.getSubject)
      // this is basically the field we want to get
      //println(t.getPredicate)
      val predExp = """(.*)#(.*)""".r
      // url defines what we operate on and elem tells us about the attribute we need to get/compare/filter
      val url, elem = predExp.findFirstIn(t.getPredicate.toString) match {
        case Some(predExp(u, e)) => (u, e)
        case None => ("","")
      }

      if (t.getMatchObject.toString.startsWith("\""))
        queue ::= "GET " + elem  + " " + t.getObject.toString.replace("\"", "")
      else
        queue ::= "BIND " + elem + " TO " + t.getObject.toString.replace("?","")

      // this is either a variable naming for later (output) or a comparision
      //println(t.getObject)
    }

    val patterns = op.getPattern.getList
    patterns.foreach(x => handleTriple(x))
  }

  def addProject(op : OpProject) {
    val vars = op.getVars
    vars.foreach(v => queue ::= "PRINT " + v.toString.replace("?", ""))
  }

  def addFilter(op : OpFilter) {
    val exprs = op.getExprs.getList
    exprs.foreach(e => queue ::= "FILTER " + e.getVarName + " " + e.getFunction + " " + e.getConstant)
  }

  def nextOp(op : Any) = op match  {
    case op:OpBGP => addBGP(op)
    case op:OpProject => addProject(op)
    case op:OpFilter => addFilter(op)
  }
}
