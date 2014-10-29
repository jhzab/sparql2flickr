package de.l3s

/**
 * Created by gothos on 7/28/2014.
 */

import com.hp.hpl.jena.query._
import com.hp.hpl.jena.sparql.algebra.{OpVisitor, OpWalker, Algebra}
import com.hp.hpl.jena.sparql.sse.SSE
import com.mongodb.casbah.Imports._
import de.l3s.sparql2flickr.query.OpVisitorFlickr
import de.l3s.sparql2flickr.query.FlickrQueryExecutor
import java.io.File

package object sparql2flickr {
  def main(args : Array[String]) {
    //val query = "PREFIX vcard:      <http://www.l3s.de/sparql-flickr-people/1.0#> SELECT ?y ?givenName WHERE { ?y vcard:Family \"Smith\" . ?y vcard:Given  ?givenName . FILTER ( ?givenName < 20 ) }"
//    val query = "PREFIX  p:  <http://l3s.de/flickr/people#>\nSELECT  ?username ?price\nWHERE   { ?x p:price ?price .\n          FILTER (?price < 30.5)\n          ?x p:username ?username .\n ?x p:username \"zabjanhendrik\" . }"

    val query = "PREFIX p: <http://l3s.de/flickr/photos#>\nSELECT ?tags \n WHERE { ?x p:tags ?tags .\n?x p:username \"zabjanhendrik\" . }"

    val parsedQuery = QueryFactory.create(query)
    val op = Algebra.compile(parsedQuery)
    // print the sparql query in algebra notation
    SSE.write(op)

    val visitor = new OpVisitorFlickr()
    OpWalker.walk(op, visitor)
    //visitor.queryConstructor.queue.foreach(println)

    val queryExecutor = new FlickrQueryExecutor(visitor.queryConstructor.queue,
      new File("/home/gothos/flickr_api.json"), debug = true)
    queryExecutor.execute
  }
}
