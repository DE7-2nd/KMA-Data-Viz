drop table if exists analytics.observation_forecast_location_vw;

create table analytics.observation_forecast_location_vw as
(
    with forecast_region as
    (
        select distinct region, region_code
        from raw_data.forecast_halfday
    )
    
    select *
    from raw_data.observation_location l
    join forecast_region fr
    on case
        when left(l.fct_id,3) = '11B' then fr.region_code = '11B00000'
        when left(l.fct_id,4) = '11C1' then fr.region_code = '11C10000'
        when left(l.fct_id,4) = '11C2' then fr.region_code = '11C20000'
        when left(l.fct_id,4) = '11D1' then fr.region_code = '11D10000'
        when left(l.fct_id,4) = '11D2' then fr.region_code = '11D20000'
        when left(l.fct_id,4) = '11F1' then fr.region_code = '11F10000'
        when left(l.fct_id,4) = '11F2' then fr.region_code = '11F20000'
        when left(l.fct_id,3) = '11G' then fr.region_code = '11G00000'
        when left(l.fct_id,4) = '11H1' then fr.region_code = '11H10000'
        when left(l.fct_id,4) = '11H2' then fr.region_code = '11H20000'
    end
)