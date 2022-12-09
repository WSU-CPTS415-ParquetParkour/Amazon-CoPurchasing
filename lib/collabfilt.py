import pandas as pd
import numpy as np

df = pd.read_excel('TempMatrix.xlsx')
df=df.replace(np.nan, 0, regex=True)


from sklearn.neighbors import NearestNeighbors
knn = NearestNeighbors(metric='cosine', algorithm='brute')
knn.fit(df.values)
distances, indices = knn.kneighbors(df.values, n_neighbors=3)

# convert user_name to user_index
customer_index = df.columns.tolist().index(6)

# t: movie_title, m: the row number of t in df
for m,t in list(enumerate(df.index)):

  # find movies without ratings by user_4
  if df.iloc[m, customer_index] == 0:
    sim_products = indices[m].tolist()
    product_distances = distances[m].tolist()

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
      if df.iloc[sim_products[s], customer_index] == 0:

        # if the rating is zero, ignore the rating and the similarity in calculating the predicted rating
        if len(product_similarity_copy) == (3 - 1):
          product_similarity_copy.pop(s)

        else:
          product_similarity_copy.pop(s-(len(product_similarity)-len(product_similarity_copy)))

      # if the rating is not zero, use the rating and similarity in the calculation
      else:
        nominator = nominator + product_similarity[s]*df.iloc[sim_products[s],customer_index]

    if len(product_similarity_copy) > 0:

      if sum(product_similarity_copy) > 0:
        predicted_r = nominator/sum(product_similarity_copy)

      else:
        predicted_r = 0

    # if all the ratings of the similar products are zero, then predicted rating should be zero
    else:
      predicted_r = 0

    df.iloc[m,customer_index] = predicted_r


def recommend_product(user, num_recommended_products):
    recommended_products = []

    for m in df[df[user] == 0].index.tolist():
        index_df = df.index.tolist().index(m)
        predicted_rating = df.iloc[index_df, df.columns.tolist().index(user)]
        recommended_products.append((m, predicted_rating))

    sorted_rm = sorted(recommended_products, key=lambda x:x[1], reverse=True)
    print('The list of the Recommended Products \n')
    rank = 1
    for recommended_product in sorted_rm[:num_recommended_products]:
        print('{}: {} - predicted rating:{}'.format(rank, recommended_product[0], recommended_product[1]))
        rank = rank + 1

recommend_product(6, 3)
