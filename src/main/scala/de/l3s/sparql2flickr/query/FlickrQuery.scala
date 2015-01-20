package de.l3s.sparql2flickr.query

import scala.reflect._

class FlickrQuery(debug: Boolean = false) {
  var queue: List[Op] = List.empty

  def add(op: Op): Unit = {
    queue ::= op

    if (debug)
      op.print()
  }

  /* TODO: add memoization */
  def getOps[T: ClassTag](): List[T] = queue.collect { case e:T => e }

  def getBindOps(): List[BindOp] = getOps[BindOp]()
  def getAggrOps(): List[AggrOp] = getOps[AggrOp]()
  def getPrintOps():List[PrintOp] = getOps[PrintOp]()
  def getGetOps(): List[GetOp] = getOps[GetOp]()
  def getFilterOps(): List[FilterOp] = getOps[FilterOp]()
  def getExtBindOps():List[ExtBindOp] = getOps[ExtBindOp]()
  def getGroupOps() = queue.collect { case e: GroupOp => e }
}
