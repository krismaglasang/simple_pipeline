My first "data pipeline" (if you can call it that).

I have 3 DAGs where I am just playing around with different implementations and operator combinations, all of which just sources data from a Postgres DB (launched locally via Docker) and prints out the result of the following query: 

```sql
SELECT * FROM imdb_dataset LIMIT 3
```
