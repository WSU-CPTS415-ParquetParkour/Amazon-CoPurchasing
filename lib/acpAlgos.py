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
    # Expects to receive an adjacency matrix stored in a DataFrame where the row indices are ASINs and column names are customer IDs (JR)
    self.adj_mtx = adj_mtx_wtd
    self.knn = NearestNeighbors(metric='cosine', algorithm='brute')

    self.knn.fit(self.adj_mtx.values)
    self.distances, self.indices = self.knn.kneighbors(self.adj_mtx.values, n_neighbors=neighbors)

    # convert user_name to user_index (CD)
    self.customer_index = self.adj_mtx.columns.tolist().index(uid)

    self.collab_filter()

  def collab_filter(self):
    # t: movie_title, m: the row number of t in df (CD)
    for m,t in list(enumerate(self.adj_mtx.index)):

      # find movies without ratings by user_4 (CD)
      if self.adj_mtx.iloc[m, self.customer_index] == 0:
        sim_products = self.indices[m].tolist()
        product_distances = self.distances[m].tolist()

        # Generally, this is the case: indices[3] = [3 6 7]. The movie itself is in the first place. (CD)
        # In this case, we take off 3 from the list. Then, indices[3] == [6 7] to have the nearest NEIGHBORS in the list. (CD)
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

          # check if the rating of a similar movie is zero (CD)
          if self.adj_mtx.iloc[sim_products[s], self.customer_index] == 0:

            # if the rating is zero, ignore the rating and the similarity in calculating the predicted rating (CD)
            if len(product_similarity_copy) == (3 - 1):
              product_similarity_copy.pop(s)

            else:
              product_similarity_copy.pop(s-(len(product_similarity)-len(product_similarity_copy)))

          # if the rating is not zero, use the rating and similarity in the calculation (CD)
          else:
            nominator = nominator + product_similarity[s]*self.adj_mtx.iloc[sim_products[s], self.customer_index]

        if len(product_similarity_copy) > 0:

          if sum(product_similarity_copy) > 0:
            predicted_r = nominator/sum(product_similarity_copy)

          else:
            predicted_r = 0

        # if all the ratings of the similar products are zero, then predicted rating should be zero (CD)
        else:
          predicted_r = 0

        self.adj_mtx.iloc[m, self.customer_index] = predicted_r
    return

  def recommend_product(self, user, n_recs=3):
      # Switching to dictionary to reduce time costs some (JR)
      recommendations = dict()

      for m in self.adj_mtx[self.adj_mtx[user] == 0].index.tolist():
          index_df = self.adj_mtx.index.tolist().index(m)
          predicted_rating = self.adj_mtx.iloc[index_df, self.adj_mtx.columns.tolist().index(user)]
          # recommended_products.append((m, predicted_rating))
          if m not in recommendations:
            recommendations[m] = predicted_rating

      # Returning the top n recommendations from the intermediate dictionary (JR)
      # Converting to DataFrame for downstream merging with other data (JR)
      top_recs = pd.DataFrame([{'asin': str(x), 'score': y} for x,y in hq.nlargest(n_recs, recommendations.items())])
      return top_recs


# Just for temporary testing, may be removed when ready to be sourced by other files (JR)
def main():
  n4 = N4J()

  try:
    products = n4.get_rating_greater(rating='4', operand='>')
    wtd_mtx = n4.get_cf_set_from_asins([x['asin'] for x in products])
    cid = rnd.sample(list(wtd_mtx.columns.values), 1)[0]

    cf = CollaborativeFilter(wtd_mtx, cid)
    recs = cf.recommend_product(cid, self.ui.spb_cf_recs_n.value())
    rec_titles = self.n4.get_titles_from_asins(recs['asin'])

    cf_recs = rec_titles.merge(recs, on='asin').sort_values('score', ascending=False)
  finally:
    n4.close()

  return

if __name__ == "__main__":
    main()