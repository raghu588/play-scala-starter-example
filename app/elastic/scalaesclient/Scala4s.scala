package elastic.scalaesclient

import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri, TcpClient}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.index.RichIndexResponse
import com.sksamuel.elastic4s.searches.RichSearchResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.ElasticsearchClient
import org.elasticsearch.common.settings.Settings
import com.sksamuel.elastic4s.circe._
import io.circe.generic.auto._

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Scala4s extends App {
  val settings = Settings.builder().put("cluster.name", "elasticsearch").build()
  implicit val client = TcpClient.transport(settings, ElasticsearchClientUri("elasticsearch://localhost:9300"))

  import com.sksamuel.elastic4s.ElasticDsl._

  val json = client.execute {
    search("final4/type4").matchAllQuery().aggs {
      termsAgg("agg1", "colorfamily.keyword").subaggs(termsAgg("agg2", "nrf_week").subAggregations(
        sumAgg("agg3", "net_sales")))
    }
  }.await.original

  println(json)

}