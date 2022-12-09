import os
import sys
import re
import random as rnd
import pandas as pd
import numpy as np
import heapq as hq
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import KFold

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))

# Add reference path to access files in /lib/ (JR)
sys.path.insert(0, os.path.join(project_root, 'lib'))

from acpN4J import N4J


class CollaborativeFilter:
  # Refactor of CD's initial collaborative filtering implementation to be called by the application (JR)
  def __init__(self, adj_mtx_wtd, uid, neighbors=3):
    # Expects to receive an adjacency matrix stored in a DataFrame (JR)
    self.adj_mtx = adj_mtx_wtd
    self.knn = NearestNeighbors(metric='cosine', algorithm='brute')

    self.knn.fit(self.adj_mtx.values)
    self.distances, self.indices = self.knn.kneighbors(self.adj_mtx.values, n_neighbors=neighbors)

    # convert user_name to user_index (CD)
    self.customer_index = self.adj_mtx.columns.tolist().index(uid)

    self.collab_filter()

  def collab_filter(self):

    # t: movie_title, m: the row number of t in df
    for m,t in list(enumerate(self.adj_mtx.index)):

      # find movies without ratings by user_4
      if self.adj_mtx.iloc[m, self.customer_index] == 0:
        sim_products = self.indices[m].tolist()
        product_distances = self.distances[m].tolist()

        # Generally, this is the case: indices[3] = [3 6 7]. The movie itself is in the first place.
        # In this case, we take off 3 from the list. Then, indices[3] == [6 7] to have the nearest NEIGHBORS in the list.
        if m in sim_products:
          id_product = sim_products.index(m)
          sim_products.remove(m)
          product_distances.pop(id_product)

        else:
            sim_products = sim_products[:3-1]
            product_distances = product_distances[:3-1]

        # movie_similarty = 1 - movie_distance
        product_similarity = [1-x for x in product_distances]
        product_similarity_copy = product_similarity.copy()
        nominator = 0

        for s in range(0, len(product_similarity)):

          # check if the rating of a similar movie is zero
          if self.adj_mtx.iloc[sim_products[s], self.customer_index] == 0:

            # if the rating is zero, ignore the rating and the similarity in calculating the predicted rating
            if len(product_similarity_copy) == (3 - 1):
              product_similarity_copy.pop(s)

            else:
              product_similarity_copy.pop(s-(len(product_similarity)-len(product_similarity_copy)))

          # if the rating is not zero, use the rating and similarity in the calculation
          else:
            nominator = nominator + product_similarity[s]*self.adj_mtx.iloc[sim_products[s], self.customer_index]

        if len(product_similarity_copy) > 0:

          if sum(product_similarity_copy) > 0:
            predicted_r = nominator/sum(product_similarity_copy)

          else:
            predicted_r = 0

        # if all the ratings of the similar products are zero, then predicted rating should be zero
        else:
          predicted_r = 0

        self.adj_mtx.iloc[m, self.customer_index] = predicted_r
    return

  def recommend_product(self, user, n_recs=3):
      recommendations = dict()

      for m in self.adj_mtx[self.adj_mtx[user] == 0].index.tolist():
          index_df = self.adj_mtx.index.tolist().index(m)
          predicted_rating = self.adj_mtx.iloc[index_df, self.adj_mtx.columns.tolist().index(user)]
          # recommended_products.append((m, predicted_rating))
          if m not in recommendations:
            recommendations[m] = predicted_rating

      # sorted_recs_idx = sorted(recommendations.items(), key=lambda k:{k[1]: k[0]}, reverse=True)
      top_recs = dict(hq.nlargest(n_recs, recommendations.items()))
      # print('The list of the Recommended Products \n')
      # rank = 1
      # for recommended_product in sorted_rm[:num_recommended_products]:
      #     print('{}: {} - predicted rating:{}'.format(rank, recommended_product[0], recommended_product[1]))
      #     rank = rank + 1
      return top_recs


#TODO: Just for temporary testing, will need to be removed when ready to be sourced by other files (JR)
def main():
  n4 = N4J()

# subset workflow (JR)
# [DONE] 1: get random user (a)
# [DONE] 2a: get categories & groups for products user reviewed
# [DONE] 2b: get 1 to n randomly selected categories & groups not in set from 2a
# [DONE] 3: get random set of users who also reviewed those products
# [PENDING] 4: get products and review ratings for all sampled users
# [PENDING] 5: generate weighted edgelist

# Switching to using data from UI query

  # try:
  #   cid = n4.get_random_customer_node()
  #   all_groups = n4.get_product_groups()
  #   all_categories = n4.get_product_categories()
  #   user_groups = n4.get_user_product_groups(cid)
  #   user_cats = n4.get_user_product_categories(cid)

  #   groups_diff = set(all_groups).symmetric_difference(set(user_groups))
  #   cats_diff = set(all_categories).symmetric_difference(set(user_cats))

  #   groups_comp = rnd.sample(list(groups_diff), rnd.randint(1, len(groups_diff)))
  #   cats_comp = rnd.sample(list(cats_diff), rnd.randint(1, len(cats_diff)))

  # finally:
  #   n4.close()


  n4 = N4J()
  try:
    products = n4.get_rating_greater(rating='4', operand='>', limit=50)
    # wtd_mtx = n4.get_user_product_ratings()
    wtd_mtx = n4.get_cf_set_from_asins([x['asin'] for x in products])
    # cid = n4.get_random_customer_node()
    cid = rnd.sample(list(wtd_mtx.columns.values), 1)[0]
  finally:
    n4.close()

  cf = CollaborativeFilter(wtd_mtx, cid)
  cf.recommend_product(cid, 3)

  return

if __name__ == "__main__":
    main()