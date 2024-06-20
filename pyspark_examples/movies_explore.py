from pyspark import SparkContext

sc = SparkContext()
movies = sc.textFile('/dataset/ml-latest-small/movies.csv')
result = movies.take(10)
for r in result:
    print(r)


movies_data = movies.filter(lambda s: not s.startswith('mov'))
c = movies_data.count()
print('The movies count is:' , c)


ratings = sc.textFile('/dataset/ml-latest-small/ratings.csv')
result = ratings.take(10)
for r in result:
    print(r)


ratings_data = ratings.filter(lambda s: not s.startswith('user'))
c = ratings_data.count()
print('The ratings count is:' , c)

movies_data_id = movies_data.map(lambda s: s.split(',')).map(lambda s: (int(s[0]), str(s[1])))
print(movies_data_id.first())

ratings_data_id = ratings_data.map(lambda s: s.split(',')).map(lambda s: (int(s[1]), float(s[2])))
print(ratings_data_id.first())

top10 = movies_data_id.join(ratings_data_id)
print(top10.take(10))

top10 = movies_data_id.join(ratings_data_id).map(lambda s: ((s[0], s[1][0]), s[1][1]))
print(top10.take(10))

top10 = movies_data_id.join(ratings_data_id).map(lambda s: ((s[0], s[1][0]), s[1][1])).groupByKey().mapValues(lambda s: (sum(s) / len(s), len(s)))
print(top10.take(10))

top10 = movies_data_id.join(ratings_data_id).map(lambda s: ((s[0], s[1][0]), s[1][1])).groupByKey().mapValues(lambda s: (sum(s) / len(s), len(s))).filter(lambda s: s[1][1] > 100).sortBy(lambda s: s[1][0], False)
print(top10.take(10))