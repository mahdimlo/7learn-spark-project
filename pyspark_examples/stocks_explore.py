from pyspark import SparkContext


def toCSV(data):
    return ','.join(data)

sc = SparkContext()
stocks = sc.textFile('/dataset/stocks.csv')
print(stocks.first())
stocks.cache()

stocks_filtered = stocks.filter(lambda s: not s.startswith('symbol'))
stocks_filtered_data = stocks_filtered.map(lambda s: s.split(','))
stocks_filtered_data.cache()

result = stocks_filtered_data.filter(lambda s: s[1].split(' ')[-1] == '2003')
result.cache()
print('The count is:')
print(result.count())

print('the data is:')
print(result.take(10))

result.map(toCSV).saveAsTextFile('/result2.csv')