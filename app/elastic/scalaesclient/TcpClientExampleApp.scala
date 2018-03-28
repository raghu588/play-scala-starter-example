package elastic.scalaesclient

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import play.libs.Json


object TcpClientExampleApp extends App {
  val uri = ElasticsearchClientUri("elasticsearch://localhost:9300")
  val client = ElasticClient.remote(uri)

  // now we can search for the document we just indexed

    val json =
      client.execute {
        search("final4/type4").matchAllQuery().aggs {
          termsAgg("termagg1", "colorfamily.keyword").subaggs(
            sumAgg("sumagg1", "net_sales"))
        }
      }.await

    /*client.execute {
      search("final4/type4").matchAllQuery().aggs {
        termsAgg("termagg1", "PROD_CTGY_CD").subAggregations(
          termsAgg("termagg2", "ITM_MSTY_CD").subAggregations(
            termsAgg("termagg3", "loc_id").subAggregations(
              termsAgg("termagg4", "nrf_week").subAggregations(
                sumAgg("sumagg1","net_sales")
              )
            )
          )
        )
      }
    }.await*/



 // var data=json.toString;
 // data=data.substring(19,data.length-1)
    println(json)


}