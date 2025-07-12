CREATE OR REPLACE TABLE
  `ga4_customer_journey_sample.journey_grouped` AS (
  WITH
    items AS (
    SELECT
      event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      user_pseudo_id,
      (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id') AS session_id,
      event_name,
      item.item_id,
      item.promotion_name,
      ecommerce.purchase_revenue AS revenue,
      ecommerce.total_item_quantity AS items
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
      UNNEST(items) AS item
    WHERE
      _TABLE_SUFFIX BETWEEN '20201101'
      AND '20210131'),
    sessions AS (
    SELECT
      event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      user_pseudo_id,
      device.category,
      geo.region,
      geo.city,
      (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id') AS session_id,
      (
      SELECT
        LOWER(value.string_value)
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_title'
        AND event_name='session_start') AS page_title,
      (
      SELECT
        LOWER(value.string_value)
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location'
        AND event_name='session_start') AS page_location,
      event_name,
      CONCAT(traffic_source.source, traffic_source.medium) AS source_medium,
      traffic_source.name AS campaign_name,
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20201101'
      AND '20210131'),
    landPLP AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      'landPLP' AS disco_action,
      '' AS funnel_action,
      '' AS sku,
    FROM
      sessions
    WHERE
      event_name="session_start"
      AND page_title LIKE "%google merchandise store%"),
    landPDP AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      'landPDP' AS disco_action,
      '' AS funnel_action,
      '' AS sku,
    FROM
      sessions
    WHERE
      event_name="session_start"
      AND page_location LIKE "%google+redesign%"
      AND page_title NOT LIKE "%google merchandise store%"),
    select_promotion AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      'clicked_promotion' AS disco_action,
      '' AS funnel_action,
      '' AS sku,
    FROM
      sessions
    WHERE
      event_name="select_promotion"),
    search_results AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      'search' AS disco_action,
      '' AS funnel_action,
      '' AS sku,
    FROM
      sessions
    WHERE
      event_name="view_search_results"),
    impression_events AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      '' AS disco_action,
      '1.impression' AS funnel_action,
      item_id,
    FROM
      items
    WHERE
      event_name IN ("view_item_list",
        "view_promotion")),
    click_events AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      '' AS disco_action,
      '2.click' AS funnel_action,
      item_id,
    FROM
      items
    WHERE
      event_name="select_item"),
    pdpView_events AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      '' AS disco_action,
      '3.pdpView' AS funnel_action,
      item_id,
    FROM
      items
    WHERE
      event_name="view_item"),
    atw_events AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      '' AS disco_action,
      '4.atw' AS funnel_action,
      item_id,
    FROM
      items
    WHERE
      event_name="add_to_wishlist"),
    atb_events AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      '' AS disco_action,
      '5.atb' AS funnel_action,
      item_id,
    FROM
      items
    WHERE
      event_name="add_to_cart"),
    purchase_events AS (
    SELECT
      user_pseudo_id,
      session_id,
      event_timestamp AS hit_time,
      '' AS disco_action,
      '6.purchase' AS funnel_action,
      item_id,
    FROM
      items
    WHERE
      event_name="purchase"),
    joined AS (
    SELECT
      *
    FROM
      landPLP
    UNION ALL
    SELECT
      *
    FROM
      landPDP
    UNION ALL
    SELECT
      *
    FROM
      select_promotion
    UNION ALL
    SELECT
      *
    FROM
      search_results
    UNION ALL
    SELECT
      *
    FROM
      impression_events
    UNION ALL
    SELECT
      *
    FROM
      pdpView_events
    UNION ALL
    SELECT
      *
    FROM
      atw_events
    UNION ALL
    SELECT
      *
    FROM
      atb_events
    UNION ALL
    SELECT
      *
    FROM
      click_events
    UNION ALL
    SELECT
      *
    FROM
      purchase_events),
    lvl_1_actions AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY hit_time) AS lvl_1_rn,
    FROM
      joined
    WHERE
      NOT(disco_action = '') ),
    lvl_1_ranking_all AS (
    SELECT
      a.*,
      CASE
        WHEN lvl_1_rn IS NOT NULL THEN lvl_1_rn
        WHEN MAX(lvl_1_rn) OVER(PARTITION BY a.session_id ORDER BY a.hit_time ASC ROWS UNBOUNDED PRECEDING) IS NOT NULL THEN MAX(lvl_1_rn) OVER(PARTITION BY a.session_id ORDER BY a.hit_time ASC ROWS UNBOUNDED PRECEDING)
        ELSE 0
    END
      AS lvl_1_rank,
    FROM
      joined a
    LEFT JOIN
      lvl_1_actions b
    ON
      a.session_id=b.session_id
      AND a.hit_time=b.hit_time
    WHERE
      a.session_id IS NOT NULL
      AND NOT(a.funnel_action='') )
  SELECT
    a.session_id,
    a.user_pseudo_id,
    a.hit_time,
    category,
    region,
    city,
    source_medium,
    campaign_name,
    CASE
      WHEN b.disco_action IS NULL THEN 'navigation'
      ELSE b.disco_action
  END
    AS disco_action,
    a.funnel_action,
    CASE
      WHEN a.funnel_action="1.impression" THEN a.session_id
  END
    AS impressions,
    CASE
      WHEN a.funnel_action="2.click" THEN a.session_id
  END
    AS clicks,
    CASE
      WHEN a.funnel_action="3.pdpView" THEN a.session_id
  END
    AS pdpViews,
    CASE
      WHEN a.funnel_action="4.atw" THEN a.session_id
  END
    AS atw,
    CASE
      WHEN a.funnel_action="5.atb" THEN a.session_id
  END
    AS atb,
    CASE
      WHEN a.funnel_action="6.purchase" THEN a.session_id
  END
    AS purchase,
    a.sku,
    revenue,
    items,
  FROM
    lvl_1_ranking_all AS a
  LEFT JOIN (
    SELECT
      session_id,
      lvl_1_rn,
      disco_action
    FROM
      lvl_1_actions
    GROUP BY
      1,
      2,
      3) b
  ON
    a.session_id=b.session_id
    AND a.lvl_1_rank=b.lvl_1_rn
  LEFT JOIN (
    SELECT
      session_id,
      MAX(category) AS category,
      MAX(region) AS region,
      MAX(city) AS city,
      MAX(source_medium) AS source_medium,
      MAX(campaign_name) AS campaign_name,
    FROM
      sessions
    GROUP BY
      session_id) AS c
  ON
    a.session_id = c.session_id
  LEFT JOIN (
    SELECT
      session_id,
      item_id,
      SUM(revenue) AS revenue,
      COUNT(items) AS items
    FROM
      items
    WHERE
      event_name='purchase'
    GROUP BY
      session_id,
      item_id) AS d
  ON
    a.session_id = d.session_id
    AND a.sku=d.item_id
  ORDER BY
    a.session_id,
    a.hit_time ASC)