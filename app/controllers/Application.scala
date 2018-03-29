package controllers


import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import play.api.mvc._

class Application extends Controller {

  import org.json._
  import play.api.libs.json.Json


  val cols = Map("nrf_year" -> "Int", "nrf_season" -> "String", "nrf_quarter" -> "Int",
    "nrf_month" -> "Int", "nrf_week" -> "Int", "nrf_day" -> "Int", "loc_id" -> "Int",
    "rgn_id" -> "Int", "itm_sku_id" -> "String", "colorfamily" -> "String", "net_sales" -> "String",
    "net_sls_qty" -> "String", "wt_unt_cst_amt" -> "String", "itm_msty_cd" -> "long", "prod_ctgy_cd" -> "Int",
    "sz1_cd" -> "String", "SZ1_CD" -> "String", "ITM_MSTY_CD" -> "long", "LOC_ID" -> "Int", "PROD_CTGY_CD" -> "Int", "loc_id" -> "Int")

  def simpleQuery = Action { request =>

    //request from extranl source
    val json = request.body.toString

    //removing characters from request
    val format_str = json.substring(17, json.length - 1)
    var splitObj = new JSONObject(format_str)

    var cqlqry = splitObj.getString("cql")
    var whrindx = cqlqry.indexOf("WHERE")

    var cols_match = Array.empty[String]
    var cols_match2 = Array.empty[String]
    var cols_match3 = Array.empty[String]
    var cols_match4 = Array.empty[String]
    if (whrindx > 0) {

      cqlqry = cqlqry.substring(whrindx + 5)


      cqlqry = cqlqry.replace(";", "")

      cols_match = cqlqry.split("and") //.map(x => x.replace("\"", ""))

      cols_match2 = cols_match(0).split("=")

      cols_match3 = cols_match(1).split("=")

      cols_match4 = cols_match(2).split("In").map(x => x.replace("\"", ""))

    }

    //    println("........:"+cols_match2(0).toString+"kkkkk:"+cols_match2(1).toString)
    //    println("........:"+cols_match3(0).toString+"kkkkk:"+cols_match3(1).toString)
    //    println("........:"+cols_match4(0).toString+"kkkkk:"+cols_match4(1).toString)

    //println("................:"+cols_match4(0).toString+".......:"+cols_match4(1).replace("('","").replace("')","").trim.toString)


    var groupcols = splitObj.getJSONArray("groupBy")
    var sumcols = splitObj.getJSONArray("sum")



    //    val uri = ElasticsearchClientUri("elasticsearch://localhost:9300")
    //        val client = ElasticClient.remote(uri)


    var str = ""
    if (sumcols.getString(0).equals("net_sales")) {
      str = sumcols.getString(0).replace("net_sales", "sales")

    }
    if (sumcols.getString(0).equals("nrf_week")) {
      str = sumcols.getString(0).replace("nrf_week", "week")

    }
    if (sumcols.getString(0).equals("colorfamily")) {
      str = sumcols.getString(0).replace("colorfamily", "color family")

    }
    if (sumcols.getString(0).equals("net_sls_qty")) {
      str = sumcols.getString(0).replace("net_sls_qty", "sales volume")

    }

    import org.elasticsearch.common.settings.Settings

    val settings = Settings.builder().put("cluster.name", "Test Cluster").build()
    val client = TcpClient.transport(settings, ElasticsearchClientUri("elasticsearch://10.1.100.111:8300"))
    var outputJsonArr = new JSONArray()
    if (groupcols.length() == 1 && sumcols.length() == 1) {


      // now we can search for the document we just indexed
      var groupcol = groupcols.get(0).toString


      if (cols.get(groupcol).get.equals("String")) {

        groupcol = groupcol + ".keyword"
      }

      //    println("........"+groupcol)
      val el_json = /*client.execute {
        search("final4/type4").matchQuery(colname,condition).aggs {
          termsAgg("termagg1", groupcol).subaggs(
            sumAgg("sumagg1", sumcols.get(0).toString))
        }
      }.await*/
      //        client.execute {
      //          search("final4/type4").matchQuery(cols_match(0).trim, cols_match(1).trim).aggregations {
      //            termsAgg("termagg1", groupcol).subaggs(
      //              sumAgg("sumagg1", sumcols.get(0).toString))
      //          }
      //        }.await

        client.execute {
          search("demofinal/demotest").query(must(matchQuery(cols_match2(0).trim, cols_match2(1).trim), matchQuery(cols_match3(0).trim, cols_match3(1).trim), matchQuery(cols_match4(0).trim, cols_match4(1).replace("('", "").replace("')", "").trim))).aggregations {
            termsAgg("termagg1", groupcol).subaggs(
              sumAgg("sumagg1", sumcols.get(0).toString))
          }
        }.await


      println("response......:" + el_json)
      //json parsing to get the KMM required output format
      var data = el_json.toString;
      data = data.substring(19, data.length - 1)
      val SRJSON = new JSONObject(data)
      val json2 = SRJSON.getJSONObject("aggregations").getJSONObject("termagg1").getJSONArray("buckets")

      var grpstr = ""
      if (sumcols.getString(0).equals("net_sales")) {
        grpstr = sumcols.getString(0).replace("net_sales", "sales")

      }
      if (groupcols.getString(0).equals("nrf_week")) {
        grpstr = groupcols.getString(0).replace("nrf_week", "week")

      }
      if (groupcols.getString(0).equals("colorfamily")) {
        grpstr = groupcols.getString(0).replace("colorfamily", "color family")

      }
      if (groupcols.getString(0).equals("net_sls_qty")) {
        grpstr = groupcols.getString(0).replace("net_sls_qty", "sales volume")

      }
      //println(".....:"+json2.length())
      for (i <- 0 until json2.length()) {

        val dataObj = json2.getJSONObject(i)
        val outpuObj = new JSONObject()
        outpuObj.put(grpstr, dataObj.get("key").toString)
        outpuObj.put(str, dataObj.getJSONObject("sumagg1").get("value").toString)
        //outpuObj.put(sumcols.getString(0), "\""+json2.getJSONObject("sumagg").get("value")+"\"")


        // println("lllllllllllll......:"+str)
        ///outpuObj.put(str, json2.getJSONObject("sumagg").get("value").toString)
        outputJsonArr.put(outpuObj)


      }
    } else if (groupcols.length() == 2 && sumcols.length() == 1) {

      var groupcol = groupcols.get(0).toString
      var groupcol1 = groupcols.get(1).toString

      if (cols.get(groupcol).get.equals("String")) {
        groupcol = groupcol + ".keyword"
      }
      if (cols.get(groupcol1).get.equals("String")) {

        groupcol1 = groupcol1 + ".keyword"
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
          outpuObj.put(str, termsmagg2Bucket.getJSONObject("sumagg1").get("value"))
          outputJsonArr.put(outpuObj)
        }

      }


    } else if (groupcols.length() == 3 && sumcols.length() == 1) {

      var groupcol = groupcols.get(0).toString
      var groupcol1 = groupcols.get(1).toString
      var groupcol2 = groupcols.get(2).toString

      if (cols.get(groupcol).get.equals("String")) {
        groupcol = groupcol + ".keyword"
      }
      if (cols.get(groupcol1).get.equals("String")) {

        groupcol1 = groupcol1 + ".keyword"
      }

      if (cols.get(groupcol2).get.equals("String")) {

        groupcol2 = groupcol2 + ".keyword"
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
            outpuObj.put(str, termsmagg3Bucket.getJSONObject("sumagg1").get("value"))

          }
        }
        outputJsonArr.put(outpuObj)
      }
    } else if (groupcols.length() == 4 && sumcols.length() == 1) {

      var groupcol = groupcols.get(0).toString
      var groupcol1 = groupcols.get(1).toString
      var groupcol2 = groupcols.get(2).toString
      var groupcol3 = groupcols.get(3).toString

      if (cols.get(groupcol).get.equals("String")) {
        groupcol = groupcol + ".keyword"
      }
      if (cols.get(groupcol1).get.equals("String")) {

        groupcol1 = groupcol1 + ".keyword"
      }

      if (cols.get(groupcol2).get.equals("String")) {

        groupcol2 = groupcol2 + ".keyword"
      }
      if (cols.get(groupcol3).get.equals("String")) {

        groupcol3 = groupcol3 + ".keyword"
      }

      val el_json = client.execute {
        search("demofinal/demotest").matchAllQuery().aggs {
          termsAgg("termagg1", groupcol).subAggregations(
            termsAgg("termagg2", groupcol1).subAggregations(
              termsAgg("termagg3", groupcol2).subAggregations(
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
              outpuObj.put(str, termsmagg4Bucket.getJSONObject("sumagg1").get("value"))

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