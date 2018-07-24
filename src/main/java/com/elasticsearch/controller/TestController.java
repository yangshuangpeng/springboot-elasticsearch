package com.elasticsearch.controller;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by baishuai on 2017/8/24.
 */
@RestController
public class TestController {

    @Resource
    TransportClient client;//注入es操作对象

    @RequestMapping(value = "local/test")
    public String localTest() {
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("like","篮球");

        SearchResponse response=client.prepareSearch("weather").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).
                setQuery(queryBuilder).setSize(10).execute().actionGet();

//        GetResponse sr=client.prepareGet("weather","person","").execute().actionGet();
        return response.toString();
    }
    @RequestMapping(value = "local/update")
    public  void upMethod1() {
        try {
            String[] ss = {};
            // 方法一:创建一个UpdateRequest,然后将其发送给client.
            UpdateRequest uRequest = new UpdateRequest();
            uRequest.index("weather");
            uRequest.type("person");
            uRequest.id("AWRV5ctVyEBOvoFDWlBb");
            uRequest.doc(jsonBuilder().startObject().field("user", ss).endObject());
            client.update(uRequest).get();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
    private void searchFunction(QueryBuilder queryBuilder) {
        SearchResponse response = client.prepareSearch("weather")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(new TimeValue(60000))
                .setQuery(queryBuilder)
                .setSize(100).execute().actionGet();

        while(true) {
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            for (SearchHit hit : response.getHits()) {
                Iterator<Map.Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
                while(iterator.hasNext()) {
                    Map.Entry<String, Object> next = iterator.next();
                    System.out.println(next.getKey() + ": " + next.getValue());
                    if(response.getHits().hits().length == 0) {
                        break;
                    }
                }
            }
            break;
        }
//        testResponse(response);
    }

    @RequestMapping(value = "/test",method = RequestMethod.GET)
    public String test(){

        Map<String,Object> map = Collections.emptyMap();

        Script script = new Script(ScriptType.INLINE, "painless","params._value0 > 0",map);  //提前定义好查询销量是否大于1000的脚本，类似SQL里面的having

        long beginTime = System.currentTimeMillis();

        SearchResponse sr = client.prepareSearch("dm_di").setTypes("sale") //要查询的表
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("store_name.keyword", "xxx旗舰店"))  //挨个设置查询条件，没有就不加，如果是字符串类型的，要加keyword后缀
                        .must(QueryBuilders.termQuery("department_name.keyword", "玎"))
                        .must(QueryBuilders.termQuery("category_name.keyword", "T恤"))
                        .must(QueryBuilders.rangeQuery("pay_date").gt("2017-03-07").lt("2017-07-09"))
                ).addAggregation(
                        AggregationBuilders.terms("by_product_code").field("product_code.keyword").size(500) //按货号分组，最多查500个货号.SKU直接改字段名字就可以
                                .subAggregation(AggregationBuilders.terms("by_store_name").field("store_name.keyword").size(50) //按店铺分组，不显示店铺可以过滤掉这一行，下边相应减少一个循环
                                        .subAggregation(AggregationBuilders.sum("total_sales").field("quantity"))  //分组计算销量汇总
                                        .subAggregation(AggregationBuilders.sum("total_sales_amount").field("amount_actual"))  //分组计算实付款汇总，需要加其他汇总的在这里依次加
                                        .subAggregation(PipelineAggregatorBuilders.bucketSelector("sales_bucket_filter",script,"total_sales")))//查询是否大于指定值
                                .order(Terms.Order.compound(Terms.Order.aggregation("total_calculate_sale_amount",false)))) //分组排序

                .execute().actionGet();

        Terms terms = sr.getAggregations().get("by_product_code");   //查询遍历第一个根据货号分组的aggregation

        System.out.println(terms.getBuckets().size());
        for (Terms.Bucket entry : terms.getBuckets()) {
            System.out.println("------------------");
            System.out.println("【 " + entry.getKey() + " 】订单数 : " + entry.getDocCount() );

            Terms subTerms = entry.getAggregations().get("by_store_name");    //查询遍历第二个根据店铺分组的aggregation
            for (Terms.Bucket subEntry : subTerms.getBuckets()) {
                Sum sum1 = subEntry.getAggregations().get("total_sales"); //取得销量的汇总
                double total_sales = sum1.getValue();
                System.out.println(subEntry.getKey() + " 订单数:  " + subEntry.getDocCount() + "  销量: " + total_sales); //店铺和订单数量和销量
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println("查询耗时" + ( endTime - beginTime ) + "毫秒");

        return "Hello,elasticsearch";
    }

}
