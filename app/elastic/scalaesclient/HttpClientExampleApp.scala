package elastic.scalaesclient

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchResponse

object HttpClientExampleApp extends App {

  // you must import the DSL to use the syntax helpers
  import com.sksamuel.elastic4s.http.ElasticDsl._

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

  val result: SearchResponse = client.execute {
    search("final4/type4").matchAllQuery().aggs {
      termsAgg("colorfamily", "colorfamily.keyword").subaggs(termsAgg("week", "nrf_week").subAggregations(
        sumAgg("netSales", "net_sales")))
    }
  }.await

  // prints out the original json
  println(result.took)
  println(result.aggregationsAsString)

  client.close()

}