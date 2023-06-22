create or replace table creative_analytics_prod.aepr.all_platforms_metadata_flatten_i1 (
	select 
		fb.start_date as start_date, fb.end_date as end_date, fb.advideo as advideo, fb.account as account, fb.ads as ads,
		fb.campaigns as campaigns, fb.objectives as objectives, fb.placements as placements,
		fb.adsets as adsets, fb.assettypes as assettypes, array() as fieldtypes, array() as serving_locations,
		array() as targeting_values, array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.facebook_ad_metadata_flatten_final_i1 fb
	
	union all
	
	select 
		aw.start_date as start_date, aw.end_date as end_date, aw.advideo as advideo, aw.account as account, aw.ads as ads,
		aw.campaigns as campaigns, array() as objectives, array() as placements, array() as adsets,
		array() as assettypes, aw.fieldtypes as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.adwords_ad_metadata_flatten_final_i1 aw
	
	union all
	
	select 
		amz.start_date as start_date, amz.end_date as end_date, amz.advideo as advideo, amz.account as account, array() as ads,
		amz.campaigns as campaigns, array() as objectives, amz.placements as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.amazonadvertising_ad_metadata_flatten_final_i1 amz

	union all
	
	select 
		dsp.start_date as start_date, dsp.end_date as end_date, dsp.advideo as advideo, dsp.account as account, array() as ads,
		dsp.campaigns as campaigns, array() as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.amazonadvertisingdsp_ad_metadata_flatten_i2 dsp
	
  union all
    
    select 
		dv.start_date as start_date, dv.end_date as end_date, dv.advideo as advideo, dv.account as account, dv.ads as ads,
		dv.campaigns as campaigns, dv.objectives as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, dv.lineitems as lineitems, dv.insertionorders as insertionorders, array() as posts
	from creative_analytics_prod.aepr.dv360_ad_metadata_flatten_i2 dv

	union all
	
	select 
		fo.start_date as start_date, fo.end_date as end_date, fo.advideo as advideo, fo.account as account, array() as ads,
		array() as campaigns, array() as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, fo.posts as posts
	from creative_analytics_prod.aepr.facebook_page_ad_metadata_flatten fo

	union all
	
	select 
		io.start_date as start_date, io.end_date as end_date, io.advideo as advideo, io.account as account, array() as ads,
		array() as campaigns, array() as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, io.posts as posts
	from creative_analytics_prod.aepr.instagram_page_ad_metadata_flatten io

	union all
	
	select 
		li.start_date as start_date, li.end_date as end_date, li.advideo as advideo, li.account as account, li.ads as ads,
		li.campaigns as campaigns, li.objectives as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, li.serving_locations as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.linkedin_ad_metadata_flatten_i2 li

	union all
	
	select 
		pin.start_date as start_date, pin.end_date as end_date, pin.advideo as advideo, pin.account as account, pin.ads as ads,
		pin.campaigns as campaigns, pin.objectives as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, pin.targeting_values as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.pinterest_ad_metadata_flatten_i2 pin
    
  union all
    
    select 
		sn.start_date as start_date, sn.end_date as end_date, sn.advideo as advideo, sn.account as account, sn.ads as ads,
		sn.campaigns as campaigns, sn.objectives as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.snapchat_ad_metadata_flatten_i2 sn
    
  union all
    
    select 
		tk.start_date as start_date, tk.end_date as end_date, tk.advideo as advideo, tk.account as account, array() as ads,
		tk.campaigns as campaigns, array() as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.tiktok_ad_metadata_flatten tk

	union all
	
	select 
		tw.start_date as start_date, tw.end_date as end_date, tw.advideo as advideo, tw.account as account, array() as ads,
		tw.campaigns as campaigns, tw.objectives as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		tw.tweets as tweets, tw.lineitems as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.twitter_ad_metadata_flatten_i2 tw
    
    union all
    
    select 
		vz.start_date as start_date, vz.end_date as end_date, vz.advideo as advideo, vz.account as account, array() as ads,
		vz.campaigns as campaigns, array() as objectives, array() as placements, array() as adsets,
		array() as assettypes, array() as fieldtypes, array() as serving_locations, array() as targeting_values,
		array() as tweets, array() as lineitems, array() as insertionorders, array() as posts
	from creative_analytics_prod.aepr.verizon_ad_metadata_flatten vz

);
