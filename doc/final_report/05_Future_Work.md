## Future Work/Improvements
> Style and writing (10%): Overall writing, grammar, organization and neatness.

* Further parallelization of data parsing
    * Splitting into multiple files
        * Separated by product via MapReduce analogue
        * Process by stream and append to files directly without building internal objects (potential disk thrash risk, write in batches?)
    * Kafka producer with multiple consumers
* Database performance and flexibility improvements expected with Neo4j Enterprise
* Distribute database across multiple machines as shards

