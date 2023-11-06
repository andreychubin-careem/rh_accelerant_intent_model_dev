try:
    from ._utils import EVENTNAMES, SERVICES
except ImportError:
    from _utils import EVENTNAMES, SERVICES


def _like_filter() -> str:
    return """
            (
                lower(cct_name) like '%food%'
                or lower(cct_name) like '%quik%'
                or lower(cct_name) like '%box%'
                or lower(cct_name) like '%b2b%'
                or lower(cct_name) like '%order%'
                or lower(cct_name) like '%shop%'
                or lower(cct_name) like '%test%'
                or lower(cct_name) like '%bot%'
                or lower(cct_name) like '%hrs%'
            )
    """


def get_intents(date: str, days_back: int = 60, percentile: float = 0.8) -> str:
    eventnames = ', '.join([f"'{x}'" for x in EVENTNAMES])
    services = ', '.join([f"'{x}'" for x in SERVICES])

    return f"""
        with base_bookings as (
            select
                booking_id,
                customer_id,
                day,
                dropoff_lat,
                dropoff_long
            from prod_dwh.booking
            where 1=1
                and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and customer_id is not null
                and booking_country in ('UAE', 'Jordan')
                and not {_like_filter()}
                and is_trip_ended
                and not is_intercity
                and not is_later
                and dropoff_lat != 0
                and dropoff_long != 0
                and pickup_lat != 0
                and pickup_long != 0
                and booking_platform in ('ICMA','ACMA')
                and customer_id != 20567159
                and business_type in ('Ride Hailing','jv')
        ),

        quantiles as (
            select customer_id
            from (
                select
                    customer_id,
                    round(percent_rank() over (order by num_trips asc), 2) as quantile
                from (
                    select 
                        customer_id,
                        count(distinct booking_id) as num_trips
                    from base_bookings
                    where day != date('{date}')
                    group by 1
                )
            )
            where quantile >= {percentile}
        ),

        bookings as (
            select
                booking_id,
                1 as in_bookings,
                is_trip_ended,
                dropoff_lat,
                dropoff_long
            from prod_dwh.booking
            where 1=1
                and day = date('{date}')
                and booking_id != 0
                and customer_id is not null
                and booking_country in ('UAE', 'Jordan')
                and not {_like_filter()}
                and not is_intercity
                and not is_later
                and pickup_lat != 0
                and pickup_long != 0
                and booking_platform in ('ICMA','ACMA')
                and customer_id != 20567159
                and business_type in ('Ride Hailing','jv')
        ),

        app_bookings as (
            select
                ts,
                sessionuuid,
                customer_id,
                booking_id,
                service_area_id,
                country_name,
                cast(latitude as decimal(38, 5)) as latitude,
                cast(longitude as decimal(38, 5)) as longitude
            from (
                select
                    sessionuuid,
                    cast(timestamp as bigint)/1000 as ts,
                    cast(userid as integer) as customer_id,
                    cast(cast(booking_id as decimal(38, 1)) as bigint) as booking_id,
                    latitude,
                    longitude,
                    country_name,
                    service_area_id,
                    row_number() over(partition BY cast(cast(booking_id as decimal(38, 1)) as bigint) ORDER BY timestamp asc) as rank
                from app_events.acma as android
                where exists (
                        select *
                        from quantiles
                        where cast(quantiles.customer_id as varchar) = android.userid
                    )
                    and cast(date as date) = date('{date}')
                    and country_name in ('United Arab Emirates', 'Jordan')
                    and event_source = 'superapp_android'
                    and latitude is not null
                    and longitude is not null
                    and booking_id is not null
                    and booking_id != ''

                union all

                select
                    sessionuuid,
                    cast(timestamp as bigint)/1000 as ts,
                    cast(userid as integer) as customer_id,
                    cast(cast(booking_id as decimal(38, 1)) as bigint) as booking_id,
                    latitude,
                    longitude,
                    country_name,
                    service_area_id,
                    row_number() over(partition BY cast(cast(booking_id as decimal(38, 1)) as bigint) ORDER BY timestamp asc) as rank
                from app_events.icma as ios
                where exists (
                        select *
                        from quantiles
                        where cast(quantiles.customer_id as varchar) = ios.userid
                    )
                    and cast(date as date) = date('{date}')
                    and country_name in ('United Arab Emirates', 'Jordan')
                    and event_source = 'superapp_ios'
                    and latitude is not null
                    and longitude is not null
                    and booking_id is not null
                    and booking_id != ''
            )
            where rank = 1
        ),

        app_sessions as (
            select
                ts,
                sessionuuid,
                customer_id,
                0 as booking_id,
                service_area_id,
                country_name,
                cast(latitude as decimal(38, 5)) as latitude,
                cast(longitude as decimal(38, 5)) as longitude
            from (
                select
                    cast(timestamp as bigint)/1000 as ts,
                    cast(userid as integer) as customer_id,
                    sessionuuid,
                    event_source,
                    service_area_id,
                    country_name,
                    latitude,
                    longitude,
                    row_number() over(partition BY sessionuuid ORDER BY timestamp asc) as rank
                from app_events.superapp_android as android
                where exists (
                        select *
                        from quantiles
                        where cast(quantiles.customer_id as varchar) = android.userid
                    )
                    and cast(date as date) = date('{date}')
                    and event_source = 'superapp_android'
                    and eventname in ({eventnames})
                    and country_name in ('United Arab Emirates', 'Jordan')
                    and latitude is not null
                    and longitude is not null
                    and replace(replace(contentid, '_rebranded'), '_rebrand') in ({services})

                union all

                select
                    cast(timestamp as bigint)/1000 as ts,
                    cast(userid as integer) as customer_id,
                    sessionuuid,
                    event_source,
                    service_area_id,
                    country_name,
                    latitude,
                    longitude,
                    row_number() over(partition BY sessionuuid ORDER BY timestamp asc) as rank
                from app_events.superapp_ios as ios
                    where exists (
                        select *
                        from quantiles
                        where cast(quantiles.customer_id as varchar) = ios.userid
                    )
                    and cast(date as date) = date('{date}')
                    and event_source = 'superapp_ios'
                    and eventname in ({eventnames})
                    and country_name in ('United Arab Emirates', 'Jordan')
                    and latitude is not null
                    and longitude is not null
                    and replace(replace(contentid, '_rebranded'), '_rebrand') in ({services})
            )
            where rank = 1
        )

        select
            '{date}' as valid_date,
            ts,
            sessionuuid,
            customer_id,
            case when in_bookings = 1 then s.booking_id else 0 end as booking_id,
            service_area_id,
            country_name,
            latitude,
            longitude,
            dropoff_lat,
            dropoff_long,
            case when is_trip_ended is null then 0 else cast(is_trip_ended as integer) end as is_trip_ended
        from (
            select * from app_bookings
            union all
            select * from app_sessions
        ) as s
        left join bookings as b
        on s.booking_id = b.booking_id
    """


def get_rh_features(date: str, days_back: int = 60, percentile: float = 0.8) -> str:
    return f"""
        with base_bookings as (
            select
                booking_id,
                customer_id,
                ts,
                pickup_lat,
                pickup_long,
                dropoff_lat,
                dropoff_long
            from (
                select
                    booking_id,
                    customer_id,
                    case when booking_country = 'UAE' then at_timezone(
                            cast(booking_creation_date as timestamp), 'Asia/Dubai'
                        )
                    when booking_country = 'Jordan' then at_timezone(
                            cast(booking_creation_date as timestamp), 'Asia/Amman'
                        )
                    else null end as ts,
                    pickup_lat,
                    pickup_long,
                    dropoff_lat,
                    dropoff_long,
                    row_number() over(partition BY customer_id, booking_id ORDER BY booking_creation_date asc) as rank
                from prod_dwh.booking
                where 1=1
                    and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                    and customer_id is not null
                    and booking_country in ('UAE', 'Jordan')
                    and not {_like_filter()}
                    and is_trip_ended
                    and not is_intercity
                    and not is_later
                    and dropoff_lat != 0
                    and dropoff_long != 0
                    and pickup_lat != 0
                    and pickup_long != 0
                    and booking_platform in ('ICMA','ACMA')
                    and customer_id != 20567159
                    and business_type in ('Ride Hailing','jv')
            )
            where rank = 1
        ),

        quantiles as (
            select
                customer_id,
                num_trips,
                round(percent_rank() over (order by num_trips asc), 2) as quantile
            from (
                select 
                    customer_id,
                    count(booking_id) as num_trips
                from base_bookings
                group by 1
            )
        ),

        bookings as (
            select
                booking_id,
                b.customer_id,
                ts,
                concat(
                    format('%.3f', round(pickup_lat, 3)), '|', format('%.3f', round(pickup_long, 3))
                ) as pickup,
                concat(
                    format('%.3f', round(dropoff_lat, 3)), '|', format('%.3f', round(dropoff_long, 3))
                ) as dropoff
            from base_bookings as b
            left join quantiles as q
            on b.customer_id = q.customer_id
            where quantile >= {percentile}
        ),

        locations_features as (
            select 
                customer_id,
                array_agg(location) as freq_locations,
                map(array_agg(location), array_agg(num_bookings)) as locations
            from (
                select
                    customer_id,
                    location,
                    count(distinct booking_id) as num_bookings
                from (
                    select
                        customer_id,
                        booking_id,
                        pickup as location
                    from bookings

                    union all

                    select
                        customer_id,
                        booking_id,
                        dropoff as location
                    from bookings
                )
                group by 1, 2
                having count(distinct booking_id) >= 3
            )
            group by 1
            having cardinality(array_agg(location)) >= 2
        ),

        historical_bookings as (
            select
                a.customer_id,
                booking_id,
                ts
                -- cast(contains(freq_locations, dropoff) as integer) as is_recom
            from bookings as a
            left join locations_features as b
                on a.customer_id = b.customer_id
            where 1=1
                and freq_locations is not null
                and contains(freq_locations, dropoff) = true
        ),

        week_ts_all as (
            select
                customer_id,
                map(array_agg(day_of_week), array_agg(num_trips)) as week_stats
            from (
                select
                    customer_id,
                    extract(DOW from ts) as day_of_week,
                    count(distinct booking_id) as num_trips
                from historical_bookings
                group by 1, 2
            )
            group by 1
        ),


        hour_ts_all as (
            select
                customer_id,
                map(array_agg(hour), array_agg(num_trips)) as hour_stats
            from (
                select
                    customer_id,
                    extract(HOUR from ts) as hour,
                    count(distinct booking_id) as num_trips
                from historical_bookings
                group by 1, 2
            )
            group by 1
        ),

        transactions as (
           -- mop bookings
            select
                customer_id as userid,
                cast(booking_id as varchar) as transaction_id,
                'ride_hailing' as service
            from base_bookings

            union all 

           -- mot bookings
            select
                customer_id as userid,
                cast(booking_id as varchar) as transaction_id,
                case 
                    when lower(merchant_name) like '%quik%' then 'quik' 
                    when lower(order_type) in ('box','anything') then 'delivery'
                    else order_type end as service
            from now_prod_dwh.orders
            where 1=1
                and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and country in ('United Arab Emirates', 'Jordan')
                and lower(order_status) = 'delivered'

            union all

           -- p2p transactions
            select
                sender_id as userid,
                transaction_id,
                'send_money' as service
            from pay_prod_agg.p2p_holistic ph
            left join p2p.p2p_cash_out_pilot_users pcc 
                on pcc.user_id = ph.sender_id
            where 1=1
                and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and (cash_out_invite_id is null or cash_out_invite_id < 1)
                and transaction_status_id = 1
                and country in ('United Arab Emirates', 'Jordan')

            union all 

           -- bills transactions
            select
                customer_id as userid,
                transaction_id,
                'pay_bills' as service
            from pay_prod_agg.on_deck_holistic 
            where 1=1
                and transaction_date between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and successful_transactions = 1

            union all 

           -- wellness transactions
            select
                cast(pi.sub as bigint) as userid,
                "appointment ref code" as transaction_id,
                (case when lower("appointment attributes") like '%duration%' or lower("service type") like '%home cleaning%' then 'justmop'
                    when lower("appointment attributes") like '%pcr%' or lower("service type") like '%pcr%' or lower("assigned professional") like '%nurse%' then 'pcr'
                    when lower("service type") like '%premium men%salon%' or lower("service type") like '%women%spa%' or lower("service type") like '%women%salon%' or lower("service type") like '%men%spa%'
                    then 'wellness' else 'na' end) as service
            from dev_pricing.tenants_justlife_transactions_oct22  a 
                inner join idp.pairwise_identifier_ts pi
                    on pi.identifier = a."careem user id"
            left join prod_helper.service_area_cluster c
                on a."client city" = c.service_area
            where 1=1
                and lower(cancellation) = 'no'
                and date(cast("appointment create date" as timestamp )) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day

            union all

           -- car rental monthly transactions
            select
                cast(pi.sub as bigint) as userid,
                cast(booking_id as varchar) as transaction_id,
                'swapp_monthlydaily|monthly' as service
            from dev_pricing.tenants_swapp_booking_details a
            inner join idp.pairwise_identifier_ts pi
                on pi.identifier = a.careem_identifier
            where 1=1
                and lower(status) not in ('failed', 'cancelled')
                and cast(booked_date as date) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day

            union all

           -- car rental daily transactions
            select 
                cast(pi.sub as bigint) as userid,
                cast(booking_id as varchar) as transaction_id,
                'swapp_monthlydaily|daily' as service
            from dev_pricing.tenants_swapp_daily_booking_details  a
            inner join idp.pairwise_identifier_ts pi
                on pi.identifier = a.careem_identifier
            where 1=1
                and lower(status) not in ('pending_payment', 'cancelled', 'pending_approval')
                and cast(booked_date as date) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day

            union all

           -- laundry transactions
            select
                cast(pi.sub as bigint) as userid,
                appointment_ref_code as transaction_id,
                'laundryshoes' as service
            from  dev_pricing.tenants_washmen_transactions a
            inner join idp.pairwise_identifier_ts pi
                on pi.identifier = a.careem_customer_id
            where 1=1
                and date(cast("booking_creation_date" as timestamp)) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and lower(order_status) = 'order completed'

            union all

           -- tickets_and_passes transactions
            select 
                cast(careem_user_id as bigint) as userid,
                tickitto_order_id as transaction_id,
                'tickets_and_passes' as service
            from dev_pricing.tenants_tikety_transactions a
            where 1=1
                and txn_date between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and lower(order_status) = 'success'

            union all 

           -- send_abroad_remittance transactions
            select
                try_cast(sender_user_id as bigint) as userid,
                id as transaction_id,
                'send_abroad_remittance' as service
            from cashout_service.remittance_transactions 
            where 1=1
                and cast(created_at as date) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and lower(invoice_status) = 'paid'
                and lower(status) in ('paid', 'completed')
                and sender_country in ('AE', 'JO')
        ),

        trx_features as (
            select
                userid as customer_id,
                sum(trx_amt) as trx_amt
            from (
                select
                    userid,
                    service,
                    count(distinct transaction_id) as trx_amt
                from transactions
                where userid is not null
                group by 1, 2

                union all

                select
                    customer_id as userid,
                    'bike' as service,
                    sum(trip_cnt) as trx_amt
                from prod_stg.customer_bike_stats_daily 
                where 1=1
                    and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                    and trip_cnt > 0
                    and customer_id is not null
                group by 1, 2
            )
            group by 1
        ),
        
        user_saved_locations as (
            select
                user_id as customer_id,
                provider_id
            from prod_dwh.bookmark_user_location
            where 1=1
                and last_updated < date('{date}')
                and country_id in (1, 19)
                and user_id in (select customer_id from locations_features)
        ),
        
        saved_locations_info as (
            select 
                provider_reference_id,
                last_updated,
                latitude,
                longitude,
                case when lower(search_display_name) like 'home%' then 'home'
                    when lower(search_display_name) like 'work%' then 'work' 
                    else null end as location_type
            from careem.location
            where year > 2019
                and date < date('{date}')
                and country_id in (1, 19)
                and (
                    lower(search_display_name) like 'home%'
                    or lower(search_display_name) like 'work%'
                )
        ),
        
        user_saved_locations_info as (
            select
                customer_id,
                map(array_agg(location_type), array_agg(coords)) as home_work_coords
            from (
                select
                    customer_id,
                    location_type,
                    map_from_entries(array[('lat', latitude), ('long', longitude)]) as coords,
                    row_number() over(partition BY customer_id, location_type ORDER BY last_updated desc) as rank
                 from user_saved_locations as usl
                 left join saved_locations_info as sli
                    on usl.provider_id = sli.provider_reference_id
                 where location_type is not null
            )
            where rank = 1
            group by 1
         ),

        features as (
            select
                '{date}' as valid_date,
                'rh' as service,
                a.customer_id,
                num_trips,
                quantile,
                trx_amt,
                cast(week_stats as json) as week_stats,
                cast(hour_stats as json) as hour_stats,
                cast(locations as json) as locations,
                cast(home_work_coords as json) as home_work_coords
            from locations_features as a
            left join week_ts_all as b
                on a.customer_id = b.customer_id
            left join hour_ts_all as c
                on a.customer_id = c.customer_id
            left join trx_features as d
                on a.customer_id = d.customer_id
            left join quantiles as e
                on a.customer_id = e.customer_id
            left join user_saved_locations_info as f
                on a.customer_id = f.customer_id
            where 1=1
                and week_stats is not null
                and hour_stats is not null
        )

        select * from features 
    """


def get_food_features(date: str, days_back: int = 60, percentile: float = 0.8) -> str:
    return f"""
        with base_bookings as (
            select
                booking_id,
                customer_id,
                ts,
                dropoff_lat,
                dropoff_long
            from (
                select
                    order_id as booking_id,
                    customer_id,
                    case when country = 'United Arab Emirates' then at_timezone(
                                cast(booking_creation_timestamp as timestamp), 'Asia/Dubai'
                            )
                        when country = 'Jordan' then at_timezone(
                                cast(booking_creation_timestamp as timestamp), 'Asia/Amman'
                            )
                        else null end as ts,
                    drop_off_latitude as dropoff_lat,
                    drop_off_longitude as dropoff_long,
                    row_number() over(partition BY customer_id, booking_id ORDER BY booking_creation_timestamp asc) as rank
                from now_prod_dwh.orders
                where day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                    and order_status = 'delivered'
                    and order_type = 'food'
                    and country in ('United Arab Emirates', 'Jordan')
                    and drop_off_latitude != 0
                    and drop_off_longitude != 0
                    and drop_off_latitude is not null
                    and drop_off_longitude is not null
                    and order_id is not null
            )
            where rank = 1
                and ts is not null
        ),
        
        quantiles as (
            select
                customer_id,
                num_orders,
                round(percent_rank() over (order by num_orders asc), 2) as quantile
            from (
                select 
                    customer_id,
                    count(booking_id) as num_orders
                from base_bookings
                group by 1
            )
        ),
        
        bookings as (
            select
                booking_id,
                b.customer_id,
                ts,
                concat(
                    format('%.3f', round(dropoff_lat, 3)), '|', format('%.3f', round(dropoff_long, 3))
                ) as dropoff
            from base_bookings as b
            left join quantiles as q
            on b.customer_id = q.customer_id
            where quantile >= {percentile}
        ),
        
        locations_features as (
            select 
                customer_id,
                array_agg(location) as freq_locations,
                map(array_agg(location), array_agg(num_bookings)) as locations
            from (
                select
                    customer_id,
                    dropoff as location,
                    count(distinct booking_id) as num_bookings
                from bookings
                group by 1, 2
                having count(distinct booking_id) >= 3
            )
            group by 1
            having cardinality(array_agg(location)) >= 1
        ),
        
        historical_bookings as (
            select
                a.customer_id,
                booking_id,
                ts,
                cast(contains(freq_locations, dropoff) as integer) as is_freq
            from bookings as a
            left join locations_features as b
                on a.customer_id = b.customer_id
            where freq_locations is not null
        ),
        
        week_ts_all as (
            select
                customer_id,
                map(array_agg(day_of_week), array_agg(num_orders)) as week_stats
            from (
                select
                    customer_id,
                    extract(DOW from ts) as day_of_week,
                    count(distinct booking_id) as num_orders
                from historical_bookings
                group by 1, 2
            )
            group by 1
        ),
        
        hour_ts_all as (
            select
                customer_id,
                map(array_agg(hour), array_agg(num_orders)) as hour_stats
            from (
                select
                    customer_id,
                    extract(HOUR from ts) as hour,
                    count(distinct booking_id) as num_orders
                from historical_bookings
                group by 1, 2
            )
            group by 1
        ),
        
        transactions as (
           -- mop bookings
           select distinct
                customer_id as userid,
                cast(booking_id as varchar) as transaction_id,
                'ride_hailing' as service
            from prod_dwh.booking
            where 1=1
                and customer_id in (select customer_id from locations_features)
                and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and service_area_id in (1, 21, 64, 68, 111, 87, 49, 47)  -- TODO: add dynamic list
                and customer_id is not null
                and booking_country in ('UAE', 'Jordan')
                and not {_like_filter()}
                and is_trip_ended
                and dropoff_lat != 0
                and dropoff_long != 0
                and pickup_lat != 0
                and pickup_long != 0
                and booking_platform in ('ICMA','ACMA')
                and customer_id != 20567159
                and business_type in ('Ride Hailing','jv')
                
            union all
                
           -- food bookings
            select
                customer_id as userid,
                cast(booking_id as varchar) as transaction_id,
                'food' as service
            from base_bookings
        
            union all 
        
           -- rest mot bookings
            select
                customer_id as userid,
                cast(booking_id as varchar) as transaction_id,
                case 
                    when lower(merchant_name) like '%quik%' then 'quik' 
                    when lower(order_type) in ('box','anything') then 'delivery'
                    else order_type end as service
            from now_prod_dwh.orders
            where 1=1
                and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and country in ('United Arab Emirates', 'Jordan')
                and lower(order_status) = 'delivered'
                and order_type != 'food'
                and customer_id in (select customer_id from locations_features)
        
            union all
        
           -- p2p transactions
            select
                sender_id as userid,
                transaction_id,
                'send_money' as service
            from pay_prod_agg.p2p_holistic ph
            left join p2p.p2p_cash_out_pilot_users pcc 
                on pcc.user_id = ph.sender_id
            where 1=1
                and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and (cash_out_invite_id is null or cash_out_invite_id < 1)
                and transaction_status_id = 1
                and country in ('United Arab Emirates', 'Jordan')
        
            union all 
        
           -- bills transactions
            select
                customer_id as userid,
                transaction_id,
                'pay_bills' as service
            from pay_prod_agg.on_deck_holistic 
            where 1=1
                and transaction_date between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and successful_transactions = 1
        
            union all 
        
           -- wellness transactions
            select
                cast(pi.sub as bigint) as userid,
                "appointment ref. code" as transaction_id,
                (case when lower("appointment attributes") like '%duration%' or lower("service type") like '%home cleaning%' then 'justmop'
                    when lower("appointment attributes") like '%pcr%' or lower("service type") like '%pcr%' or lower("assigned professional") like '%nurse%' then 'pcr'
                    when lower("service type") like '%premium men%salon%' or lower("service type") like '%women%spa%' or lower("service type") like '%women%salon%' or lower("service type") like '%men%spa%'
                    then 'wellness' else 'na' end) as service
            from dev_pricing.tenants_justlife_transactions_oct22  a 
                inner join idp.pairwise_identifier_ts pi
                    on pi.identifier = a."careem user id"
            left join prod_helper.service_area_cluster c
                on a."client city" = c.service_area
            where 1=1
                and lower(cancellation) = 'no'
                and date(cast("appointment create date" as timestamp )) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
        
            union all
        
           -- car rental monthly transactions
            select
                cast(pi.sub as bigint) as userid,
                cast(booking_id as varchar) as transaction_id,
                'swapp_monthlydaily|monthly' as service
            from dev_pricing.tenants_swapp_booking_details a
            inner join idp.pairwise_identifier_ts pi
                on pi.identifier = a.careem_identifier
            where 1=1
                and lower(status) not in ('failed', 'cancelled')
                and cast(booked_date as date) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
        
            union all
        
           -- car rental daily transactions
            select 
                cast(pi.sub as bigint) as userid,
                cast(booking_id as varchar) as transaction_id,
                'swapp_monthlydaily|daily' as service
            from dev_pricing.tenants_swapp_daily_booking_details  a
            inner join idp.pairwise_identifier_ts pi
                on pi.identifier = a.careem_identifier
            where 1=1
                and lower(status) not in ('pending_payment', 'cancelled', 'pending_approval')
                and cast(booked_date as date) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
        
            union all
        
           -- laundry transactions
            select
                cast(pi.sub as bigint) as userid,
                appointment_ref_code as transaction_id,
                'laundryshoes' as service
            from  dev_pricing.tenants_washmen_transactions a
            inner join idp.pairwise_identifier_ts pi
                on pi.identifier = a.careem_customer_id
            where 1=1
                and date(cast("booking_creation_date" as timestamp)) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and lower(order_status) = 'order completed'
        
            union all
        
           -- tickets_and_passes transactions
            select 
                cast(careem_user_id as bigint) as userid,
                tickitto_order_id as transaction_id,
                'tickets_and_passes' as service
            from dev_pricing.tenants_tikety_transactions a
            where 1=1
                and txn_date between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and lower(order_status) = 'success'
        
            union all 
        
           -- send_abroad_remittance transactions
            select
                try_cast(sender_user_id as bigint) as userid,
                id as transaction_id,
                'send_abroad_remittance' as service
            from cashout_service.remittance_transactions 
            where 1=1
                and cast(created_at as date) between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                and lower(invoice_status) = 'paid'
                and lower(status) in ('paid', 'completed')
                and sender_country in ('AE', 'JO')
        ),
        
        trx_features as (
            select
                userid as customer_id,
                sum(trx_amt) as trx_amt
            from (
                select
                    userid,
                    service,
                    count(distinct transaction_id) as trx_amt
                from transactions
                where userid is not null
                group by 1, 2
        
                union all
        
                select
                    customer_id as userid,
                    'bike' as service,
                    sum(trip_cnt) as trx_amt
                from prod_stg.customer_bike_stats_daily 
                where 1=1
                    and day between date('{date}') - interval '{days_back + 1}' day and date('{date}') - interval '1' day
                    and trip_cnt > 0
                    and customer_id is not null
                group by 1, 2
            )
            group by 1
        ),
        
        features as (
            select
                '{date}' as valid_date,
                a.customer_id,
                num_orders,
                quantile,
                trx_amt,
                cast(week_stats as json) as week_stats,
                cast(hour_stats as json) as hour_stats,
                cast(locations as json) as locations
            from locations_features as a
            left join week_ts_all as b
            on a.customer_id = b.customer_id
            left join hour_ts_all as c
            on a.customer_id = c.customer_id
            left join trx_features as d
            on a.customer_id = d.customer_id
            left join quantiles as e
            on a.customer_id = e.customer_id
        )
        
        select * from features
    """


if __name__ == '__main__':
    print(get_intents('2023-08-10', 90))
