package de.l3s.sparql2flickr.query


/**
 * We use our own Ops to abstract from Jena and to make it easier to get to relevant data.
 * This might get a bit tricky with very large querys that have JOINs in them tho.
 */

abstract sealed class Op() {
  def printHelper(args: Map[String,String], className: String) {
    if (args.nonEmpty)
      println(s"Arguments of ${className}")
    args.foreach{ case (k,v) => println(s"- arg: ${k} - val: ${v}") }
  }

  def print(): Unit = println("Abstract class Op.")
}

case class GroupOp(val member: String) extends Op {
  def print(): Unit = printHelper(Map("member" -> member), this.getClass().getName())
}
case class FilterOp(val member: String, val func: String, val arg: String) extends Op {
  def print(): Unit = printHelper(Map("member" -> member, "func" -> func, "arg" -> arg),
    this.getClass().getName())
}
case class AggrOp(val subj: String, val member: String, val func: String) extends Op {
  def print(): Unit = printHelper(Map("member" -> member, "subj" -> subj, "func" -> func),
    this.getClass().getName())
}
case class ExtBindOp(val subj: String, val member: String) extends Op {
  def print(): Unit = printHelper(Map("member" -> member, "subj" -> subj),
    this.getClass().getName())
}
case class PrintOp(val member: String) extends Op {
    def print(): Unit = printHelper(Map("member" -> member), this.getClass().getName())
}

// obj is always the part before the "#" like poeple, member is the
// name of a member of the obj like "username"
case class BindOp(val obj: String, val member: String, val variable: String,
  val value: String) extends Op
case class GetOp(val obj: String, val member: String, val variable: String) extends Op
