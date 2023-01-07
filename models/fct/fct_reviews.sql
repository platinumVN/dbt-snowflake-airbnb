{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)
SELECT 
  {{ dbt_utils.surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }}
    AS review_id,
  * 
  FROM src_reviews
WHERE review_text is not null

{% if is_incremental() %} -- if there is incremental load 
    -- AND: only insert record that review date (in original table) is > latest in this table (fct_reviews)
  AND review_date > (select max(review_date) from {{ this }}) 
{% endif %}
