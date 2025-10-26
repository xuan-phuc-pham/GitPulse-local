{{ config(
    tags = ['daily']
) }}

SELECT * FROM {{ref('stg_repos')}}