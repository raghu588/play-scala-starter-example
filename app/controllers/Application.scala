package controllers


import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import elastic.scalaesclient.TcpClientExampleApp.{client, /*data,*/ json}
import org.elasticsearch.action.search.SearchResponse
import play.api.libs.json.JsonNaming
import play.api.mvc._

class Application extends Controller  {

  import play.api.libs.json.Json
  import org.json._


  val cols = Map("nrf_year" -> "Int", "nrf_season" -> "String", "nrf_quarter" -> "Int",
    "nrf_month" -> "Int" ,"nrf_week" -> "Int","nrf_day" -> "Int","loc_id" -> "Int",
    "rgn_id" -> "Int","itm_sku_id" -> "String", "colorfamily" -> "String","net_sales" -> "String",
    "net_sls_qty" -> "String","wt_unt_cst_amt" -> "String","itm_msty_cd" -> "long","prod_ctgy_cd" -> "Int",
    "sz1_cd" -> "String","SZ1_CD" -> "String","ITM_MSTY_CD"->"long","LOC_ID"->"Int","PROD_CTGY_CD"->"Int","loc_id" -> "Int")

  def simpleQuery = Action { request =>

    //request from extranl source
    val json = request.body.toString

    //removing characters from request
    val format_str = json.substring(17, json.length - 1)
    var splitObj = new JSONObject(format_str)


    var groupcols = splitObj.getJSONArray("groupBy")
    var sumcols = splitObj.getJSONArray("sum")


    val uri = ElasticsearchClientUri("elasticsearch://10.1.100.111:8300")
    val client = ElasticClient.remote(uri)

    var outputJsonArr = new JSONArray()
    if (groupcols.length() == 1 && sumcols.length() == 1) {


      // now we can search for the document we just indexed
     var groupcol = groupcols.get(0).toString


      if(cols.get(groupcol).get.equals("String")){

        groupcol =groupcol+".keyword"
      }

      val el_json = client.execute {
        search("demofinal/demotest").matchAllQuery().aggs {
          termsAgg("termagg1", groupcol).subaggs(
            sumAgg("sumagg1", sumcols.get(0).toString))
        }
      }.await
      //json parsing to get the KMM required output format
      var data = el_json.toString;
      data = data.substring(19, data.length - 1)
      val SRJSON = new JSONObject(data)
      val json2 = SRJSON.getJSONObject("aggregations").getJSONObject("termagg1").getJSONArray("buckets")

      for (i <- 0 until json2.length()) {

        val dataObj = json2.getJSONObject(i)
        val outpuObj = new JSONObject()
        outpuObj.put(groupcols.getString(0), dataObj.getString("key"))
        outpuObj.put(sumcols.getString(0), dataObj.getJSONObject("sumagg1").get("value"))
        outputJsonArr.put(outpuObj)


      }
    } else if (groupcols.length() == 2 && sumcols.length() == 1) {

      var groupcol = groupcols.get(0).toString
      var groupcol1 = groupcols.get(1).toString

      if(cols.get(groupcol).get.equals("String")){
        groupcol =groupcol+".keyword"
      }
      if(cols.get(groupcol1).get.equals("String")){

        groupcol1=groupcol1+".keyword"
      }

     // println("groupcol..............:"+groupcol)
      //println("groupcol..............:"+groupcol1)
      val el_json = client.execute {
        search("demofinal/demotest").matchAllQuery().aggs {
          termsAgg("termagg1", groupcol).subAggregations(
            termsAgg("termagg2", groupcol1).subAggregations(
              sumAgg("sumagg1", sumcols.get(0).toString)
            )
          )
        }
      }.await


      var data = el_json.toString;
      data = data.substring(19, data.length - 1)
      val SRJSON = new JSONObject(data)
      val termsmagg1Buckets = SRJSON.getJSONObject("aggregations").getJSONObject("termagg1").getJSONArray("buckets")

      for (i <- 0 until termsmagg1Buckets.length()) {
        val outpuObj = new JSONObject()
        val tersmagg1Bucket = termsmagg1Buckets.getJSONObject(i);
        outpuObj.put(groupcols.getString(0), tersmagg1Bucket.get("key"))
        val termAgg2 = tersmagg1Bucket.getJSONObject("termagg2")

        val termsmagg2Buckets = termAgg2.getJSONArray("buckets")

        for (i <- 0 until termsmagg2Buckets.length()) {

          val termsmagg2Bucket = termsmagg2Buckets.getJSONObject(i)
          outpuObj.put(groupcols.getString(1), termsmagg2Bucket.get("key"))
          outpuObj.put(sumcols.getString(0), termsmagg2Bucket.getJSONObject("sumagg1").get("value"))
          outputJsonArr.put(outpuObj)
        }

      }


    } else if (groupcols.length() == 3 && sumcols.length() == 1) {

      var groupcol = groupcols.get(0).toString
      var groupcol1 = groupcols.get(1).toString
      var groupcol2 = groupcols.get(2).toString

      if(cols.get(groupcol).get.equals("String")){
        groupcol =groupcol+".keyword"
      }
      if(cols.get(groupcol1).get.equals("String")){

        groupcol1=groupcol1+".keyword"
      }

      if(cols.get(groupcol2).get.equals("String")){

        groupcol2=groupcol2+".keyword"
      }

      val el_json = client.execute {
        search("demofinal/demotest").matchAllQuery().aggs {
          termsAgg("termagg1", groupcol).subAggregations(
            termsAgg("termagg2", groupcol1).subAggregations(
              termsAgg("termagg3", groupcol2).subAggregations(
                sumAgg("sumagg1", sumcols.get(0).toString)
              )
            )
          )
        }
      }.await


      var data = el_json.toString;
      data = data.substring(19, data.length - 1)
      val SRJSON = new JSONObject(data)
      val termsmagg1Buckets = SRJSON.getJSONObject("aggregations").getJSONObject("termagg1").getJSONArray("buckets")

      for (i <- 0 until termsmagg1Buckets.length()) {
        val outpuObj = new JSONObject()
        val termsmagg1Bucket = termsmagg1Buckets.getJSONObject(i);
        outpuObj.put(groupcols.getString(0), termsmagg1Bucket.get("key"))
        val termAgg2 = termsmagg1Bucket.getJSONObject("termagg2")

        val termsmagg2Buckets = termAgg2.getJSONArray("buckets")

        for (i <- 0 until termsmagg2Buckets.length()) {
          val termsmagg2Bucket = termsmagg2Buckets.getJSONObject(i)
          outpuObj.put(groupcols.getString(1), termsmagg2Bucket.get("key"))
          val termAgg3 = termsmagg2Bucket.getJSONObject("termagg3")

          val termsmagg3Buckets = termAgg3.getJSONArray("buckets")

          for (i <- 0 until termsmagg3Buckets.length()) {

            val termsmagg3Bucket = termsmagg3Buckets.getJSONObject(i)
            outpuObj.put(groupcols.getString(2), termsmagg3Bucket.get("key"))
            outpuObj.put(sumcols.getString(0), termsmagg3Bucket.getJSONObject("sumagg1").get("value"))

          }
        }

        outputJsonArr.put(outpuObj)

      }


    } else if (groupcols.length() == 4 && sumcols.length() == 1) {

      var groupcol = groupcols.get(0).toString
      var groupcol1 = groupcols.get(1).toString
      var groupcol2 = groupcols.get(2).toString
      var groupcol3 = groupcols.get(3).toString

      if(cols.get(groupcol).get.equals("String")){
        groupcol =groupcol+".keyword"
      }
      if(cols.get(groupcol1).get.equals("String")){

        groupcol1=groupcol1+".keyword"
      }

      if(cols.get(groupcol2).get.equals("String")){

        groupcol2=groupcol2+".keyword"
      }
      if(cols.get(groupcol3).get.equals("String")){

        groupcol3=groupcol3+".keyword"
      }

      val el_json = client.execute {
        search("demofinal/demotest").matchAllQuery().aggs {
          termsAgg("termagg1", groupcol).subAggregations(
            termsAgg("termagg2", groupcol1).subAggregations(
              termsAgg("termagg3",groupcol2).subAggregations(
                termsAgg("termagg4", groupcol3).subAggregations(
                  sumAgg("sumagg1", sumcols.get(0).toString)
                )
              )
            )
          )
        }
      }.await


      var data = el_json.toString;
      data = data.substring(19, data.length - 1)
      val SRJSON = new JSONObject(data)
      val termsmagg1Buckets = SRJSON.getJSONObject("aggregations").getJSONObject("termagg1").getJSONArray("buckets")

      for (i <- 0 until termsmagg1Buckets.length()) {
        val outpuObj = new JSONObject()
        val termsmagg1Bucket = termsmagg1Buckets.getJSONObject(i);
        outpuObj.put(groupcols.getString(0), termsmagg1Bucket.get("key"))
        val termAgg2 = termsmagg1Bucket.getJSONObject("termagg2")

        val termsmagg2Buckets = termAgg2.getJSONArray("buckets")

        for (i <- 0 until termsmagg2Buckets.length()) {
          val termsmagg2Bucket = termsmagg2Buckets.getJSONObject(i);
          outpuObj.put(groupcols.getString(1), termsmagg2Bucket.get("key"))
          val termAgg3 = termsmagg2Bucket.getJSONObject("termagg3")

          val termsmagg3Buckets = termAgg3.getJSONArray("buckets")

          for (i <- 0 until termsmagg3Buckets.length()) {
            val termsmagg3Bucket = termsmagg3Buckets.getJSONObject(i)
            outpuObj.put(groupcols.getString(2), termsmagg3Bucket.get("key"))
            val termAgg3 = termsmagg3Bucket.getJSONObject("termagg4")
            val termsmagg4Buckets = termAgg3.getJSONArray("buckets")

            for (i <- 0 until termsmagg4Buckets.length()) {

              val termsmagg4Bucket = termsmagg4Buckets.getJSONObject(i)
              outpuObj.put(groupcols.getString(3), termsmagg4Bucket.get("key"))
              outpuObj.put(sumcols.getString(0), termsmagg4Bucket.getJSONObject("sumagg1").get("value"))

            }
          }
        }
        outputJsonArr.put(outpuObj)

      }


    }


    val SRJSON2 = Json.parse(outputJsonArr.toString)

    //  val personReads = Json.reads[SRJSON]
    println("Response generated successfully")
    Ok((SRJSON2))


  }


}