## Methodology
> Model/Algorithm/Method (30%): This is where you give a detailed description of your primary contribution. It is especially important that this part be clear and well written so that we can fully understand what you did.

> Style and writing (10%): Overall writing, grammar, organization and neatness.

---

## Notes From MS03 Statement
* Collaborative Filtering

* Bidirectional search to search for keyword in nodes starting at goal node and starting node [1](#03_ref_01)

    ![](https://github.com/WSU-CPTS415-ParquetParkour/Amazon-CoPurchasing/blob/seandev/bidirectkey.png?raw=true)

    ![](https://github.com/WSU-CPTS415-ParquetParkour/Amazon-CoPurchasing/blob/seandev/bidirct.png?raw=true)

* Backward Expanding search [2](#03_ref_02)

    ![](https://github.com/WSU-CPTS415-ParquetParkour/Amazon-CoPurchasing/blob/seandev/backward.png?raw=true)


* Our baseline algorithm will be BFS to search the nodes for keywords to return items to the user. The algorithms that we will test are the following.
* Collaborative Filtering uses similarities between users and items simultaneously to provide recommendations. This allows for serendipitous recommendations; that is, collaborative filtering models can recommend an item to user B based on the interests of a similar user A.
    * Input: User A purchase history
    * Output: 5 recommendations for User B, based off the purchases of user A
    * Goal: Our goal is to implement a recommender system so we can provide recommendations for amazon users. This will hopefully encourage users to buy more products and ultimately boost revenue for Amazon.
    * Our technique falls under heuristic optimization where we have no guarantee that an optimal solution is found. Since we are recommending other products, we cannot assume every recommendation will suit a user perfectly.
* Bidirectional Search: Bidirectional expansion for keyword search on graph databases[2](#03_ref_02).
* Backward Expanding search: Keyword searching and browsing in databases using BANKS[1](#03_ref_01).


* Collaborative Filtering
    * Expected Result: A query-defined-length set of recommeneded products for a given user.
    * Performance: Since this is relying on a KNN algorithm at its core, running the analysis live may present some latency concerns for large datasets with many products to rank/group.
    * Factors:
        * The size of the query: Query size is closely coupled with considerations for the subset of data being examined - the more specific the query is, the smaller the dataset and confidence in the suitability of recommendations, with the converse holding true as well.
        * The size of the data: While larger datasets are expected to give greater confidence in the association of products to users, this will also increase processing time for generating and testing models.
        * Our technique falls under heuristic optimization where we have no guarantee that an optimal solution is found. Since we are recommending other products, we cannot assume every recommendation will suit a user perfectly. However we can also predict user ratings to justify why the system recommended certain products as a way to fact check our recommendations.

* Bidirectional Search
    * Expected Result: a rooted directed tree containing at least one node.
    * Performance: we expect to have a execution time of a few seconds and have accuracy between 3 and 6 incorrect/missing nodes. These performance metrics are expected to be similar to the stats given from the article Bidirectional Expansion For Keyword Search on Graph Databases.
    * Factors:
        * The size of the query: the amount of keywords to search for may significantly impact the performance of the algorithms since it will need to check every additional word and add it to the returned answer.
        * The size of the data: since this algorithm starts at two different spots, the amount of nodes between the nodes varies greatly between different queries.

* Backward Expanding Search
    * Expected Result: a rooted directed tree containing at least one node.
    * Performance: we expect to have a execution time of a few seconds and have accuracy between 8 and 12 incorrect/missing nodes. I expected these performance metrics to be similar to the stats given from the article Keyword Searching and Browsing in Databases using BANKS.
    * Factors:
        * The size of the query: the amount of keywords to search for may significantly impact the performance of the algorithms since it will need to check every additional word and add it to the returned answer.
        * The size of the data: the data used in the article was about 100,000 nodes. Our data set is much larger and may impact the performance from a few seconds to into the ten's of seconds 



## References
* <a name = "03_ref_01">[1]</a> G. Bhalotia, C. Nakhe, A. Hulgeri, S. Chakrabarti, and S. Sudarshan. Keyword searching and browsing in databases using
BANKS. In ICDE, 2002

* <a name = "03_ref_02">[2]</a> V. Kacholia, S. Pandit, S. Chakrabarti, S. Sudarshan, R. Desai, and H. Karambelkar. Bidirectional expansion for keyword
search on graph databases. In VLDB, 2005.
