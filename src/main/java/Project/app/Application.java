package Project.app;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.common.unit.Fuzziness;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.json.JSONException;

import Project.tools.*;

import java.io.IOException;

import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.util.Scanner;


//Test sync for remote connection

@SpringBootApplication

public class Application {
    private static String[] _args;


    public static void main(String[] args)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, NoSuchProviderException {

        SpringApplication.run(Application.class, args);

        //Initialization of the high level client as well as the low level client
        //We also create some additional services for providing information about the indexes
        ESManager EsManager = new ESManager();
        RestHighLevelClient HighClient = EsManager.CreateHighLevelClient();
        RestClient LowLevelClient = EsManager.CreateLowLevelClient(HighClient);
        InfoService infoService = new InfoService(HighClient);
        IndexService indexService = new IndexService(HighClient);
        System.out.println(ConsoleColors.BLUE_BRIGHT + "Number of Indexes    ----> " + ConsoleColors.RESET
                + infoService.CountIndexes());

        //Since we're not using multiple sets of data, we directly input the correct index name that represents the beat we're working with
        String request = "filebeat-7.12.1-2021.07.14-000001";

        //Next we print some information about the above beat
        try
        {
            System.out.println(ConsoleColors.BLUE_BRIGHT + "Indexes    ----> " + ConsoleColors.RESET
                    + infoService.GetIndexName(request));

            System.out.print("\n\n\n");
            System.out.println(ConsoleColors.BLUE_BRIGHT + "All Indexes    ----> " + ConsoleColors.RESET);
            infoService.ShowAllIndexes();
        }
        catch(IOException e1)
        {
            e1.printStackTrace();
        }
        catch(JSONException e2)
        {
            e2.printStackTrace();
        }

        System.out.print("\n\n\n");
        System.out.println(ConsoleColors.BLUE_BRIGHT + "Search   ----> " + ConsoleColors.RESET);

        //We start the request by creating a searchRequest item with the beat name as the parameter
        //Also we create the searchSourceBuilder that'll carry on the request
        SearchRequest searchRequest = new SearchRequest(request);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //some utility variables
        boolean moreSearches = false;
        Scanner userInput = new Scanner(System.in);

        String fieldname;
        String searchvalue;
        String userReply;
        do {
            //First off we ask the user the desired field name and the corresponding value they're interesed in
            do{
                System.out.println(ConsoleColors.GREEN_BRIGHT + "Please enter the field name: " + ConsoleColors.RESET);
                fieldname = userInput.nextLine();
            }while(fieldname.contains("*") || fieldname.contains("?") || fieldname.length() == 0);


            do{
                System.out.println(ConsoleColors.GREEN_BRIGHT + "Please enter the search value (can contain wildcards): "
                        + ConsoleColors.RESET);

                searchvalue = userInput.nextLine();
            }while(searchvalue.length() == 0);

            //We set the starting point from the first record, up to the 100th (or up to, if there are less results)
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(100);

            //Next we determine if the value we're searching contains wildcards or not, and treat in accordingly
            if (searchvalue.contains("*") || searchvalue.contains("?")) {
                //In the event we have wildcards we need to use the WildcardQueryBuilder to deal with the search request
                WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(fieldname, searchvalue);

                //then we forward the query to the searchSourceBuilder that'll perform the search operation
                searchSourceBuilder.query(wildcardQueryBuilder);
            } else {
                //If we don't have wildcards then we use the matchQueryBuilder to form the query
                MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(fieldname, searchvalue);

                //we use some fuzziness in our searches to maximise the number of hits, especially when handling simiar data
                matchQueryBuilder.fuzziness(Fuzziness.AUTO);
                matchQueryBuilder.prefixLength(3);
                matchQueryBuilder.maxExpansions(12);

                searchSourceBuilder.query(matchQueryBuilder);
            }

            //Regardless of the type of search, this is the point where we form the query
            searchRequest.source(searchSourceBuilder);


            int hitsCounter = 0;
            try {
                //This is the point where we feed the query to the HighClient in order to get the results in the searchResponse
                SearchResponse searchResponse = HighClient.search(searchRequest, RequestOptions.DEFAULT);

                //We store each result in the SearchHit array for easier handling
                SearchHit[] values = searchResponse.getHits().getHits();

                //and we get the number of the results in the hitsCounter, and then report the number of hits to the user
                hitsCounter = values.length;
                System.out.println(ConsoleColors.BLUE_BRIGHT + "Number of hits = " +
                        "[ " + ConsoleColors.RESET + hitsCounter + ConsoleColors.BLUE_BRIGHT + " ]" + ConsoleColors.RESET);

                //If we got hits, we display them, or else we simply inform the user that nothing came up
                if (values.length > 0) {
                    int i = 0;
                    for (SearchHit s : values) {
                        System.out.println(ConsoleColors.GREEN_BRIGHT + " Result [ " + i + " ]" + ConsoleColors.RESET);
                        System.out.println(s.getSourceAsString());
                        System.out.println();
                        i++;
                    }
                } else {
                    System.out.println(ConsoleColors.YELLOW + "No results found!" + ConsoleColors.RESET);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            //Finally we ask the user if they wish to perform another search and if they do we repeat the process
            System.out.println(ConsoleColors.GREEN_BRIGHT + "Would you like to perform another search? Y/N" + ConsoleColors.RESET);
            userReply = userInput.nextLine();

            if(userReply.equalsIgnoreCase("Y") || userReply.contains("Y"))
                moreSearches = true;
            else
                moreSearches = false;


        }while(moreSearches);
    }

}






