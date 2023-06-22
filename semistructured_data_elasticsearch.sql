create or replace table creative_analytics_prod.aepr.adwords_ad_metadata_flatten_i1(
  select advideo, ad, campaign , fp.account, min(date) as start_date, max(date) as end_date
  from import_management_prod.silver.analytics_record_v4_adwords_ad fp 
  where fp.advideo in (SELECT distinct advideo FROM table_changes('import_management_prod.silver.analytics_record_v4_adwords_ad', CURRENT_timestamp + INTERVAL - 6 hours) where _change_type != 'update_preimage')
  group by advideo, ad, campaign, fp.account 
);

create or replace table creative_analytics_prod.aepr.adwords_ad_metadata_flatten_i2 (
  select advideo, account, collect_set(ad) as ads, collect_set(campaign) as campaigns, min(start_date) as start_date, max(end_date) as  end_date
  from creative_analytics_prod.aepr.adwords_ad_metadata_flatten_i1
  group by advideo, account
);


create or replace table creative_analytics_prod.aepr.adwords_adasset_metadata_flatten_i1(
  select advideo, ad, campaign , account, fieldtype, min(date) as start_date, max(date) as end_date
  from import_management_prod.silver.analytics_record_v4_adwords_ad_asset fp 
  where fieldtype in ('YOUTUBE_VIDEO', 'MARKETING_IMAGE') 
  and fp.advideo in (SELECT distinct advideo FROM table_changes('import_management_prod.silver.analytics_record_v4_adwords_ad_asset', CURRENT_timestamp + INTERVAL - 6 hours) where _change_type != 'update_preimage')
  group by advideo, ad, campaign, account, fieldtype
);

create or replace table creative_analytics_prod.aepr.adwords_adasset_metadata_flatten_i2 (
  select advideo, account, collect_set(ad) as ads, collect_set(campaign) as campaigns, collect_set(fieldtype) as fieldtypes, min(start_date) as start_date, max(end_date) as  end_date
  from creative_analytics_prod.aepr.adwords_adasset_metadata_flatten_i1
  group by advideo, account
);


create or replace table creative_analytics_prod.aepr.adwords_ad_metadata_flatten_final_i1 (
  select
	coalesce(f1.advideo,f2.advideo) as advideo , coalesce(f1.account,f2.account) as account,
	flatten(collect_set(array_union(ifnull(f1.ads, array()), ifnull(f2.ads, array())))) as ads,
	flatten(collect_set(f2.fieldtypes)) as fieldtypes,
	flatten(collect_set(array_union(ifnull(f1.campaigns, array()), ifnull(f2.campaigns, array())))) as campaigns,
  array_min(array(first(f1.start_date),first(f2.start_date))) as start_date, array_max(array(first(f1.end_date),first(f2.end_date))) as end_date
  from creative_analytics_prod.aepr.adwords_ad_metadata_flatten_i2 f1
  full outer join creative_analytics_prod.aepr.adwords_adasset_metadata_flatten_i2 f2 on f1.advideo=f2.advideo and f1.account=f2.account
  group by f1.advideo,f2.advideo,f1.account,f2.account
);
