This folder contains a synthetic dataset in a relational database format about a jaffle shop (1 year of data), generated with https://github.com/dbt-labs/jaffle-shop-generator. A Dockerfile is included, so you can generate different data yourself.

Run below commands.
```
docker build --progress plain -t jaffle-generator
docker run --rm -it -v my-path-to\stack-t\event_data\jaffle_shop\:/jaffle-data jaffle-generator
jafgen 3 --pre jaffle
```
This will generate 3 years worth of synthetic purchasing data in below files:
* jaffle_customers.csv
* jaffle_items.csv
* jaffle_orders.csv
* jaffle_products.csv
* jaffle_stores.csv
* jaffle_suppliers.csv
* jaffle_tweets.csv
