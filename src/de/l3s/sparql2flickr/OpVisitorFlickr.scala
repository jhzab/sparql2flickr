package de.l3s.sparql2flickr

import com.hp.hpl.jena.sparql.algebra.{AlgebraQuad, OpVisitorBase}
import com.hp.hpl.jena.sparql.algebra.op._
import scala.collection.JavaConversions._

//import de.l3s.sparql2flickr.FlickrQueryConstructor

/**
 * Created by gothos on 7/28/2014.
 */
class OpVisitorFlickr extends OpVisitorBase  {
  var queryConstructor = new FlickrQueryConstructor()

  /*
   * not possible :'(
  def visit(op : Any) = op match {
    case op : OpBGP => queryConstructor.nextOp(op)
  }
  */

  override def visit(opBGP : OpBGP) {
    queryConstructor.nextOp(opBGP)
  }

  override def visit(opTriple : OpTriple) {
    println("Triple: " + opTriple.getTriple().toString())
  }

  override def visit(opProject : OpProject) {
    queryConstructor.nextOp(opProject)
  }

  override def visit(opFilter : OpFilter) {
    queryConstructor.nextOp(opFilter)
  }

  override def visit(opList : OpList) {
    println("List: " + opList.getName)
  }

  override def visit(opPath : OpPath) {
    println("List: " + opPath.getName)
  }

  override def visit(opQuadBlock : OpQuadBlock) {
    println("QuadBlock: " + opQuadBlock.getName())
  }

  override def visit(opQuad : OpQuad) {
    println("Quad: " + opQuad.getName())
  }

  override def visit(quadPattern : OpQuadPattern) {
    println("QuadPattern: " + quadPattern.getName())
  }

  override def visit(opCond : OpConditional) {
    println("Cond: " + opCond.getName)
  }

  override def visit(opOrder : OpOrder) {
    println("Order: " + opOrder.getName())
    opOrder.getConditions.foreach(println)
  }

  override def visit(opLabel : OpLabel) {
    println("Label: " + opLabel.getName)
  }

  override def visit(opGraph : OpGraph) {
    println("Graph: " + opGraph.getName)
  }

  override def visit(opGroup : OpGroup) {
    println("Group: " + opGroup.getName)
  }

  override def visit(opExtend : OpExtend) {
    println("Extend: " + opExtend.getName)
  }

  override def visit(opSlice : OpSlice) {
    println("Slice: " + opSlice.getName)
  }

  override def visit(opExt : OpExt) {
    println("Ext: " + opExt.getName)
  }

  override def visit(opDSNames : OpDatasetNames) {
    println("DSNames: " + opDSNames.getName)
  }

  override def visit(opTopN : OpTopN) {
    println("TopN: " + opTopN.getName)
  }
}
