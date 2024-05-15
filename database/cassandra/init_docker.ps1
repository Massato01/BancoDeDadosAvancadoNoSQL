# Listar containers
docker ps

# Executa o container do cassandra
docker exec -it meu-cassandra cqlsh

# NÃO RODA NO TERMINAL, inserir dentro do cqlsh manualmente
# CREATE KEYSPACE cc6240 WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor' :1};
# USE cc6240;