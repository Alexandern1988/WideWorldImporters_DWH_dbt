{% snapshot dimCustomer_stg_scd %}

{{
   config(
       target_database='WideWorldImporters',
       target_schema='dev',
       unique_key='CustomerID',
       strategy='timestamp',
       updated_at='StartDate',
   )
}}

select * from {{ ref('dimCustomer_stg') }}

{% endsnapshot %}