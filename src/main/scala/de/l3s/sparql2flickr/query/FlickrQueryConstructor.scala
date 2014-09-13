package de.l3s.sparql2flickr.query

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.op._

import scala.collection.JavaConversions._

/* Implement a queue. GET data with x=$y, filter x for something and output z */
class FlickrQueryConstructor {
  var queue = List[Op]()
  // if subject and object begin with '?' it's just naming a variable for later use
  // if object(?) is in quotation marks, it's a "filter". the actual filter comes later in the syntax tho,
  // but we might want to execute it before to minimize API load
  def addBGP(op : OpBGP) {
    def handleTriple(t : Triple) = {
      val predExp = """(.*)[#/](.*)""".r
      // FIXME: for now we only support predicates like:
      //        http://l3s.de/flickr/people#username
      // obj defines what we operate on and member tells us about the attribute
      // we need to get/compare/filter
      val predStart = "http://l3s.de/flickr/"
      if (!t.getPredicate.toString.startsWith(predStart)) {
        // FIXME: throw exception here
        println("ERROR: Predicate doesn't start with 's{predStart}")
      }

      val (obj, member) = predExp.findFirstIn(t.getPredicate.toString) match {
        case Some(predExp(u, e)) => (u.replace(predStart, ""), e)
        // TODO: throw error here!
        case None => ("", "")
      }

      //println(s"obj: ${obj} member: ${member}")
      if (t.getMatchObject.toString.startsWith("\""))
        queue ::= new Op("GET", pred = s"${obj}#${member}",
          obj = t.getObject.toString.replace("\"", ""))
      else
        queue ::= new Op("BIND", pred = s"${obj}#${member}",
          obj = t.getObject.toString.replace("?", ""),
          subj = t.getSubject.getName)
    }

    val patterns = op.getPattern.getList
      // println(t.getSubject)
      // this is basically the field we want to get
    patterns foreach (x => handleTriple(x))
  }

  def addProject(op : OpProject) {
    val vars = op.getVars
    vars foreach (v => queue ::= new Op("PRINT", obj = v.toString.replace("?", "")))
  }

  def addFilter(op : OpFilter) {
    val exprs = op.getExprs.getList
    exprs foreach (e => queue ::= new Op("FILTER",
      obj = e.getVarsMentioned.head.toString.replace("?", ""),
      func = e.getFunction.getOpName,
      value = e.getFunction.getArg(2).toString
      ))
  }

  def nextOp(op : Any) = op match  {
    case op:OpBGP => addBGP(op)
    case op:OpProject => addProject(op)
    case op:OpFilter => addFilter(op)
  }

  def handleGet() {

  }
}
