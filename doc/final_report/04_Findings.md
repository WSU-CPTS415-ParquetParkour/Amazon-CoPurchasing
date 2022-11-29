## Findings
> Results and findings (35%): How do you evaluate your solution to whatever empirical, algorithmic or theoretical question you have addressed and what do these evaluation methods tell you about your solution. It is not so important how well your method performs but rather how interesting and clever your experiments and analysis are. We are interested in seeing a clear and conclusive set of experiments which successfully evaluate the problem you set out to solve. Make sure to interpret the results and talk about what can we conclude and learn from your evaluations. Even if you have a theoretical project you should have something here to demonstrate the validity or value of your project (for example, proofs or runtime analysis).

> Style and writing (10%): Overall writing, grammar, organization and neatness.

---



## Caveats and Hurdles
* Network considerations
    * Enabling jumbo frames and/or increasing MTU cap
* Server considerations
    * Run queries in `CALL{...}` transaction batches and limit the inner subquery while testing
    * While containers made some things more difficult (i.e.: troubleshooting, primarily networking issues), it made many others easier (i.e.: restarting/rebuilding a container with a hung process or with out of memory errors)
    * Neo4j required a fair amount of tuning for memory usage parameters
* Software changes
    * Neo4j still actively being developed, updating from 4.4 to 5.2 altered many of our Cypher queries when experimenting with loading data
        * e.g.: `LOAD CSV` switching from `:auto USING PERIODIC COMMIT 1000` as a preamble to `:auto LOAD CSV WITH HEADERS FROM 'file:///nodes_with_edges.csv' AS line` followed by the Cypher statement wrapped in a subquery via `CALL {...}`.
        * This has also resulted in conflicting documentation in Neo4j's knowledge base articles

