
## Grant accessed and create views

### SQL files can be found in `tourism-dwh/airflow/include/secrets/`

### 1) Roles & users
`docker exec -i clickhouse bash -lc 'clickhouse-client -n' < airflow/include/security/01_roles_and_users.sql`

### 2) Full-access views
`docker exec -i clickhouse bash -lc 'clickhouse-client -n' < airflow/include/security/02_views_full.sql`

### 3) Masked table + limited view
`docker exec -i clickhouse bash -lc 'clickhouse-client -n' < airflow/include/security/03_views_masked.sql`

### 4) Grants
`docker exec -i clickhouse bash -lc 'clickhouse-client -n' < airflow/include/security/04_grants_on_views.sql`

### Login to analyst_full_user
```
docker exec -it clickhouse clickhouse-client \
  --user analyst_full_user --password full123
```
### Login to analyst_limited_user
```
docker exec -it clickhouse clickhouse-client \
  --user analyst_limited_user --password limited123
```
### Query masked table
Note: not actually sensitive data columns – company email, phone, address
```
SELECT email, phone, address
      FROM default_gold.dim_company
      WHERE email!='' OR phone!='' OR address!=''
      LIMIT 5
```
### Query limited table (with both users)
```
SELECT email, phone, address FROM default_gold.dim_company LIMIT 1
```

## Pictures of results
### Full analyst query results:
![](screenshots/full_analyst.png "Full analyst query results")
### Limited analyst query results:
![](screenshots/limited_analyst.png "Limited analyst query results")
