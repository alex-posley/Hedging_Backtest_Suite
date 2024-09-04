from elasticsearch import Elasticsearch

from elasticsearch import Elasticsearch

client = Elasticsearch(
  "http://elastic.posley.capital:80/_application/search_application/CEX_Hedger/_search",
  api_key="MTNTdHU1RUJ6NV9yajB1NGg2NU06RUptT2FNaDJUdmlHX1JmYnV0UklTdw=="
)
print('test')
# API key should have cluster monitor rights
client.info()