package de.l3s.sparql2flickr.query


/**
 * We use our own Ops to abstract from Jena and to make it easier to get to relevant data.
 * This might get a bit tricky with very large querys that have JOINs in them tho.
 */

class Op(val cmd : String, val pred : String = "", val subj : String = "",
         val obj : String = "", val value : String = "", val func : String = ""){
  println(s"cmd: ${cmd} pred: ${pred} subj: ${subj} obj: ${obj} value: ${value} func: ${func}")
}