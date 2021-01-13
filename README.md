### Recommender engine
Implementation of content based recommender engine based on item similarity.
Requirements and the custom similarity metrics are described in [instructions](./instructions.md)

### Similarity metrics and weights
The calculated similarity metric between two items is a tuple where the first element is the number of
matching attributes (integer from 0 to 10) and the second element is calculated as follows. We take the binary
array of matches and give weights to each attribute match by converting the array to the decimal number
(the least important attribute `j` has the weight `2^0`, attribute `i` is weighted `2^1` etc. until `a` that weights `2^9`).
Attribute precedence is then calculated by summing the weights for each attribute match.
Final ordering sorts by the number of matches first and by attribute precedence next
(e.g. (5, 710) is greater than (4, 912), (5, 653) is greater than (5, 611)).

### Current prototype solution
Prototype solution is given in `HelloSparkPlayground`. It takes a sku id, calculates recommendations
and prints them to the console. This script was used during the development of the recommendation algorithm.
It is far from being production ready.

### Extended solution with web service


### Improvements and future work
Given that input data is static (assuming item attributes are updated or new items added daily in best case),
the efficient solution would pre-calculate the recommendations for all items and store them to the external datastore.
On recommendation request the result then just needs to be read and returned (no spark session involved and
no computations happening for each request). The (nightly) batch job could be set up to update recommendations. Frequency of
recomputations depends on the dynamic of the data set.

Another alternative route to explore is to possibly use [mleap](https://github.com/combust/mleap)
  to remove the dependency from the spark session and load the pre-computed model.

There is a number of minor improvements to be explored.

Currently the all-pairs similarity matrix contains pairs with the similarity score (0, 0), i.e. zero matches.
With the given data set the number of these pairs is approx. 100M which is half the size of the similarity matrix.
Perhaps reducing the size of the similarity matrix by filtering out zero-matches pairs helps improve the performance of
computation. Naturally, filtering is done just once and the resulting similarity matrix memoized so that it can be used
for the subsequent recommendations.

Next, the spark query that takes the sku id and similarity matrix in order to filter, sort and take top n recommendations
could possibly be optimized (function `getRecommendations` in `SparkRecommender`). Currently it is slow. 

Of course, many code improvements are due. The most important one is to add tests that are currently missing (sorry!).
In addition, error handling has to be improved significantly (at least the input parameter checks and the more
informative message in the server response than just internal error with the status code `500`).

Regarding the web service, consider adding blocking dispatcher to avoid starving the routing infrastructure.
More information can be found [here](https://doc.akka.io/docs/akka-http/current/handling-blocking-operations-in-akka-http-routes.html).

These points are marked with `//TODO` in the code.
