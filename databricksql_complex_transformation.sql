create or replace table creative_analytics_prod.aepr.media_tag_data_aggregated_i1 (
select 
    s.media_id,
    case 
        when pm.platform = 'BACKGROUND_FACEBOOK_AD_ACCOUNT' then 'facebook'
        when pm.platform = 'BACKGROUND_SNAPCHAT' then 'snap'
        when pm.platform = 'BACKGROUND_TWITTER' then 'twitter'
        when pm.platform = 'BACKGROUND_ADWORDS' then 'adwords'
        when pm.platform = 'BACKGROUND_DV360' then 'dv360'
        when pm.platform = 'BACKGROUND_PINTEREST' then 'pinterest'
        when pm.platform = 'BACKGROUND_LINKEDIN' then 'linkedin'
        when pm.platform = 'BACKGROUND_VERIZON_NATIVE' then 'verizonnative'
        when pm.platform = 'BACKGROUND_TIKTOK' then 'tiktok'
        when pm.platform = 'BACKGROUND_INSTAGRAM_PAGE' then 'instagrampage'
        when pm.platform = 'BACKGROUND_FACEBOOK_PAGE' then 'facebookpage'
        when pm.platform = 'BACKGROUND_AMAZONADVERTISING' then 'amazonadvertising'
        when pm.platform = 'BACKGROUND_AMAZONADVERTISINGDSP' then 'amazonadvertisingdsp'
        else 'unknown'
    end as network,
    case 
        when pm.platform = 'BACKGROUND_FACEBOOK_AD_ACCOUNT' then substring(pm.platform_account_id, 5) 
        else pm.platform_account_id
    end as account_id,
    pm.platform_media_id,
    m.file_type,
    m.duration,
    case 
      when pm.created_by_vidmob = 1 then 'true' 
      else 'false'
    end as created_by_vidmob,
    CASE
        WHEN t.type in ('COLOR:DOMINANT_COLORS:CATEGORY:0', 'COLOR:DOMINANT_COLORS:CATEGORY:1', 'COLOR:DOMINANT_COLORS:CATEGORY:2', 'COLOR:DOMINANT_COLORS:CATEGORY:3', 'COLOR:DOMINANT_COLORS:CATEGORY:4', 'COLOR:DOMINANT_COLORS:CATEGORY:5') THEN 'COLOR:DOMINANT_COLORS:CATEGORY'
        ELSE t.type
    END as clean_type,
    CASE
        WHEN t.type = 'LABEL:CUSTOM' THEN 'custom element'
        WHEN t.type in ('LOGO', 'HIVE:LOGO') THEN 'branding'
        WHEN t.type in ('COLOR:VIBRANCY:CATEGORY', 'COLOR:TEMPERATURE:CATEGORY', 'COLOR:CONTRAST:CATEGORY', 'COLOR:TEXT_CONTRAST:CATEGORY', 'COLOR:DOMINANT_COLORS:CATEGORY:0', 'COLOR:DOMINANT_COLORS:CATEGORY:1', 'COLOR:DOMINANT_COLORS:CATEGORY:2', 'COLOR:DOMINANT_COLORS:CATEGORY:3', 'COLOR:DOMINANT_COLORS:CATEGORY:4', 'COLOR:DOMINANT_COLORS:CATEGORY:5') THEN 'color'
        WHEN t.type in ('FACE:GENDER', 'LABEL', 'LABEL:PARENT') THEN 'object'
        WHEN t.type in ('TEXT:LINE_EMPHASIS', 'TEXT:WORD_EMPHASIS', 'CTA:BY_WORD', 'CTA:BY_LINE', 'CTA:TRANSCRIPTS') THEN 'messaging'
        WHEN t.type = 'CELEBRITY:NAME' THEN 'celebrity'
        WHEN t.type in ('FACE:GAZE_DIRECTION', 'FACE:SMILE', 'FACE:EMOTION') THEN 'gaze and emotion'
        ELSE t.type
    END as generic_type,
    CASE
        WHEN t.type in ('LOGO', 'HIVE:LOGO') THEN CONCAT('Logo: ', t.value)
        WHEN t.type in ('FACE:SMILE') THEN 'Smile'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'right' THEN 'Gaze Right'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'left' THEN 'Gaze Left'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'down' THEN 'Gaze Down'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'up' THEN 'Gaze Up'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'middle' THEN 'Gaze Straight Ahead'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'up-right' THEN 'Gaze Up Right'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'down-right' THEN 'Gaze Down Right'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'up-left' THEN 'Gaze Up Left'
        WHEN t.type in ('FACE:GAZE_DIRECTION') and t.value = 'down-left' THEN 'Gaze Down Left'
        WHEN t.type in ('COLOR:VIBRANCY:CATEGORY') and t.value = 'most_colorful' THEN 'High Color Saturation'
        WHEN t.type in ('COLOR:VIBRANCY:CATEGORY') and t.value = 'somewhat_colorful' THEN 'Medium Color Saturation'
        WHEN t.type in ('COLOR:VIBRANCY:CATEGORY') and t.value = 'least_colorful' THEN 'Low Color Saturation'
        WHEN t.type in ('COLOR:TEMPERATURE:CATEGORY') and t.value = 'warm' THEN 'Warm Color Temperature'
        WHEN t.type in ('COLOR:TEMPERATURE:CATEGORY') and t.value = 'neutral' THEN 'Neutral Color Temperature'
        WHEN t.type in ('COLOR:TEMPERATURE:CATEGORY') and t.value = 'cool' THEN 'Cool Color Temperature'
        WHEN t.type in ('COLOR:CONTRAST:CATEGORY') and t.value = 'high' THEN 'High Color Contrast'
        WHEN t.type in ('COLOR:CONTRAST:CATEGORY') and t.value = 'medium' THEN 'Medium Color Contrast'
        WHEN t.type in ('COLOR:CONTRAST:CATEGORY') and t.value = 'low' THEN 'Low Color Contrast'
        WHEN t.type in ('COLOR:TEXT_CONTRAST:CATEGORY') and t.value = 'high' THEN 'High Text Contrast'
        WHEN t.type in ('COLOR:TEXT_CONTRAST:CATEGORY') and t.value = 'medium' THEN 'Medium Text Contrast'
        WHEN t.type in ('COLOR:TEXT_CONTRAST:CATEGORY') and t.value = 'low' THEN 'Low Text Contrast'
        WHEN t.type in (
                'COLOR:DOMINANT_COLORS:CATEGORY:0', 
                'COLOR:DOMINANT_COLORS:CATEGORY:1', 
                'COLOR:DOMINANT_COLORS:CATEGORY:2', 
                'COLOR:DOMINANT_COLORS:CATEGORY:3', 
                'COLOR:DOMINANT_COLORS:CATEGORY:4', 
                'COLOR:DOMINANT_COLORS:CATEGORY:5'
            ) THEN REPLACE(SUBSTRING(t.value, 1, LOCATE(':', t.value) - 1), '_', ' ')
        ELSE t.value
    END as clean_value,
    COALESCE(max(cast(t.confidence as float)), 100) as confidence,
    
    collect_list(
    map (
    'appearance_start_time',
    case when mcl.type = 'TEMPORAL' then cast(mctl.start_time as float)
        when mcl.type = 'TEMPORAL_RECTANGULAR' then cast(mctrl.start_time as float)
        else 0 end,
        
        'inverted_appearance_start_time',
    case when mcl.type = 'TEMPORAL' then (COALESCE(cast(m.duration_precise as float), m.duration) - cast(mctl.start_time as float) - cast(mctl.duration as float))
        when mcl.type = 'TEMPORAL_RECTANGULAR' then (COALESCE(cast(m.duration_precise as float), m.duration) - cast(mctrl.start_time as float) - cast(mctrl.duration as float))
        else 0 end ,
        
        'appearance_duration',
    case when mcl.type = 'TEMPORAL' then cast(mctl.duration as float)
        when mcl.type = 'TEMPORAL_RECTANGULAR' then cast(mctl.duration as float)
        else 0 end ,
        
        'appearance_start_time_perc',
    case 
        when m.duration is not null and m.duration > 0 then
            case when mcl.type = 'TEMPORAL' then cast(mctl.start_time as float)
                when mcl.type = 'TEMPORAL_RECTANGULAR' then cast(mctl.start_time as float)
                else 0 end *
            100 / COALESCE(cast(m.duration_precise as float), m.duration)
    else 0 end ,
    
    'inverted_appearance_start_time_perc',
    case 
        when m.duration is not null and m.duration > 0 then
            case when mcl.type = 'TEMPORAL' then (COALESCE(cast(m.duration_precise as float), m.duration) - cast(mctl.start_time as float) - cast(mctl.duration as float))
                when mcl.type = 'TEMPORAL_RECTANGULAR' then (COALESCE(cast(m.duration_precise as float), m.duration) - cast(mctl.start_time as float) - cast(mctl.duration as float))
                else 0 end *
            100 / COALESCE(cast(m.duration_precise as float), m.duration)
    else 0 end ,
    
    'appearance_duration_perc',
    case 
        when m.duration is not null and m.duration > 0 then
            case when mcl.type = 'TEMPORAL' then cast(mctl.duration as float)
                when mcl.type = 'TEMPORAL_RECTANGULAR' then cast(mctl.duration as float)
                else 0 end *
            100 / COALESCE(cast(m.duration_precise as float), m.duration)
    else 0 end 
    )) as appearances
    
from import_management_prod.rds.media_recognition_tag t
    inner join import_management_prod.rds.media_recognition_session s on (t.session_id = s.id) 
    inner join import_management_prod.rds.media m on (s.media_id = m.id)
    inner join import_management_prod.rds.platform_media pm on (m.id = pm.media_id)
    left outer join import_management_prod.rds.media_clip_location mcl on (t.media_clip_location_id = mcl.id)
    left outer join import_management_prod.rds.media_clip_temporal_location mctl on (t.media_clip_location_id = mctl.id)
    left outer join import_management_prod.rds.media_clip_temporal_rectangular_location mctrl on (t.media_clip_location_id = mctrl.id)
where 
      (t.type in (
            'TEXT:WORD_EMPHASIS',
            'LOGO',
            'LABEL',
            'CELEBRITY:NAME',
            'FACE:GENDER',
            'FACE:EMOTION',
            'FACE:GAZE_DIRECTION',
            'LABEL:CUSTOM',
            'COLOR:VIBRANCY:CATEGORY',
            'COLOR:TEMPERATURE:CATEGORY',
            'COLOR:CONTRAST:CATEGORY',
            'COLOR:TEXT_CONTRAST:CATEGORY',
            'COLOR:DOMINANT_COLORS:CATEGORY:0',
            'COLOR:DOMINANT_COLORS:CATEGORY:1',
            'COLOR:DOMINANT_COLORS:CATEGORY:2',
            'COLOR:DOMINANT_COLORS:CATEGORY:3',
            'COLOR:DOMINANT_COLORS:CATEGORY:4',
            'COLOR:DOMINANT_COLORS:CATEGORY:5',
            'CTA:BY_WORD',
            'CTA:BY_LINE',
            'CTA:TRANSCRIPTS'
        ) 
    or
        (
            t.type = 'FACE:SMILE' and t.value = 'true'
        ))
    and  (t.confidence is null or t.confidence >= 65.0)
group by s.media_id, network, account_id, pm.platform_media_id, m.file_type,
m.duration, created_by_vidmob, clean_type, clean_value, generic_type

);



create or replace table creative_analytics_prod.aepr.media_tag_data_aggregated_i2(
select 
  media_id, network, account_id, platform_media_id, file_type, duration, created_by_vidmob, 
  collect_list(
  	named_struct(
  		'clean_type', clean_type,
  		'generic_type', generic_type,
  		'clean_value', clean_value,
  		'confidence', confidence,
  		'appearances', appearances 
  	)
  ) as tags

from creative_analytics_prod.aepr.media_tag_data_aggregated_i1
group by media_id, network, account_id, platform_media_id, file_type, duration, created_by_vidmob
);
