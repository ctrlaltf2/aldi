COPY (WITH price_changes AS (
        SELECT   sku,
                 name,
                 price_amount                             AS before_price,
                 next_price                               AS after_price,
                 CAST(next_price AS FLOAT) / price_amount AS pdiff,
                 timestamp                                AS before_date,
                 next_timestamp                           AS after_date,
                 ROW_NUMBER() OVER (PARTITION BY sku ORDER BY after_date DESC) AS rn,
                 store_code
        FROM (
                 SELECT   sku,
                          priceAmount AS price_amount,
                          name,
                          LEAD (priceAmount) OVER ( partition BY sku, storeCode ORDER BY timestamp ) AS next_price,
                          timestamp,
                          LEAD (timestamp) OVER ( partition BY sku, storeCode ORDER BY timestamp ) AS next_timestamp,
                          storeCode AS store_code
                 FROM     products )
        WHERE    price_amount != next_price
        AND      next_price IS NOT NULL
        ORDER BY next_timestamp DESC
)
SELECT
  sku,
  name,
  before_price,
  after_price,
  pdiff,
  before_date,
  after_date,
FROM
  price_changes
WHERE
  rn = 1
ORDER BY
  pdiff ASC) to 'latest.csv';
