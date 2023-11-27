from opensearchpy import OpenSearch, exceptions

host = 'localhost'
port = 9200
auth = ('admin', 'admin')

def insert_data_to_index(client):
    document = {
    'title': 'Moneyball',
    'director': 'Bennett Miller',
    'year': '2011'
    }

    response_insert_to_index = client.index(
        index = 'python-test-index',
        body = document,
        id = '1',
        refresh = True
    )
    return response_insert_to_index

# Create the client with SSL/TLS and hostname verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    use_ssl = True,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

print(client.info())

index_name = 'python-test-index'
index_body = {
    "mappings":{
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"}
        }
    }
}

try:
  response_create_index = client.indices.create(index_name, body=index_body)
except exceptions.RequestError as e:
  print(e.status_code, '|', e.info, '|', e.error)
  #raise e
  if e.status_code == 400 and e.error == 'resource_already_exists_exception':
      print(insert_data_to_index(client=client))