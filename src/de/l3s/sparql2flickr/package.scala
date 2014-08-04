package de.l3s

/**
 * Created by gothos on 7/28/2014.
 */

import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.algebra.{OpVisitor, OpWalker, Algebra}
import com.hp.hpl.jena.sparql.sse.SSE

package object sparql2flickr {
  def main(args : Array[String]) {
    val query = "PREFIX vcard:      <http://www.l3s.de/sparql-flickr-people/1.0#> SELECT ?y ?givenName WHERE { ?y vcard:Family \"Smith\" . ?y vcard:Given  ?givenName . FILTER ( ?givenName < 20 ) }"

    val parsedQuery = QueryFactory.create(query)
    val op = Algebra.compile(parsedQuery)
    // print the sparql query in algebra notation
    //SSE.write(op)

    val visitor = new OpVisitorFlickr()
    OpWalker.walk(op, visitor)
    visitor.queryConstructor.queue.foreach(println)
  }
}
