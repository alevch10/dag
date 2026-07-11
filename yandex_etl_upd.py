from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging

# ---------- Переменные из Airflow (создайте в UI: Admin → Variables) ----------
DWH_HELPER_URL = Variable.get("dwh_helper_url", default_var="http://dwh-helper:8000")
BEARER_TOKEN = Variable.get("BEARER", default_var="")
HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/x-yaml"
}

# ---------- Шаблоны YAML (с плейсхолдерами {start_date} и {end_date}) ----------

APPMETRICA_YAML = """
source:
  type: appmetrica
  application_id: 4777504
  date_since: "{start_date}"
  date_until: "{end_date}"
  export_format: csv
  fields:
    - profile_id
    - application_id
    - app_build_number
    - ios_ifa
    - ios_ifv
    - android_id
    - google_aid
    - os_name
    - os_version
    - device_manufacturer
    - device_model
    - original_device_model
    - device_type
    - device_locale
    - device_ipv6
    - app_version_name
    - event_name
    - event_json
    - event_datetime
    - event_receive_datetime
    - connection_type
    - operator_name
    - mcc
    - mnc
    - country_iso_code
    - city
    - appmetrica_device_id
    - installation_id
    - session_id
    - windows_aid
  chunk_days: 7
  generate_uuid: true

tables:
  - name: appmetrica.events
    primary_key: [uuid]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: uuid
        sources: [ "__uuid__" ]
        type: uuid
      - target: profile_id
        sources: ["profile_id"]
        type: integer
      - target: application_id
        sources: ["application_id"]
        type: integer
      - target: event_datetime
        sources: ["event_datetime"]
        type: datetime
      - target: event_receive_datetime
        sources: ["event_receive_datetime"]
        type: datetime
      - target: event_name
        sources: ["event_name"]
        type: string
      - target: session_id
        sources: ["session_id"]
        type: integer
      - target: appmetrica_device_id
        sources: ["appmetrica_device_id"]
        type: integer
      - target: installation_id
        sources: ["installation_id"]
        type: uuid
      - target: app_version_name
        sources: ["app_version_name"]
        type: string
      - target: app_build_number
        sources: ["app_build_number"]
        type: integer
      - target: windows_aid
        sources: ["windows_aid"]
        type: string

  - name: appmetrica.event_params
    primary_key: [uuid]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: uuid
        sources: [ "__uuid__" ]
        type: uuid
      - target: profile_id
        sources: ["profile_id"]
        type: integer
      - target: event_datetime
        sources: ["event_datetime"]
        type: datetime
      - target: event_name
        sources: ["event_name"]
        type: string
      - target: session_id
        sources: ["session_id"]
        type: integer
      - target: event_json
        sources: ["event_json"]
        type: json

  - name: appmetrica.devices
    primary_key: [uuid]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: uuid
        sources: [ "__uuid__" ]
        type: uuid
      - target: profile_id
        sources: ["profile_id"]
        type: integer
      - target: event_datetime
        sources: ["event_datetime"]
        type: datetime
      - target: device_manufacturer
        sources: ["device_manufacturer"]
        type: string
      - target: device_model
        sources: ["device_model"]
        type: string
      - target: original_device_model
        sources: ["original_device_model"]
        type: string
      - target: device_type
        sources: ["device_type"]
        type: string
      - target: device_locale
        sources: ["device_locale"]
        type: string
      - target: device_ipv6
        sources: ["device_ipv6"]
        type: inet
      - target: os_name
        sources: ["os_name"]
        type: string
      - target: os_version
        sources: ["os_version"]
        type: string
      - target: connection_type
        sources: ["connection_type"]
        type: string
      - target: operator_name
        sources: ["operator_name"]
        type: string
      - target: mcc
        sources: ["mcc"]
        type: integer
      - target: mnc
        sources: ["mnc"]
        type: integer
      - target: ios_ifa
        sources: ["ios_ifa"]
        type: uuid
      - target: ios_ifv
        sources: ["ios_ifv"]
        type: uuid
      - target: android_id
        sources: ["android_id"]
        type: string
      - target: google_aid
        sources: ["google_aid"]
        type: uuid
        null_values: ["0000-0000", ""]

  - name: appmetrica.location
    primary_key: [uuid]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: uuid
        sources: [ "__uuid__" ]
        type: uuid
      - target: profile_id
        sources: ["profile_id"]
        type: integer
      - target: event_datetime
        sources: ["event_datetime"]
        type: datetime
      - target: country_iso_code
        sources: ["country_iso_code"]
        type: string
      - target: city
        sources: ["city"]
        type: string
"""

BOOKING_YAML = """
source:
  type: yandex_metrika
  counter_id: 53128117
  date_from: "{start_date}"
  date_to: "{end_date}"
  source: hits
  fields:
    - ym:pv:watchID
    - ym:pv:pageViewID
    - ym:pv:visitID
    - ym:pv:clientID
    - ym:pv:dateTime
    - ym:pv:title
    - ym:pv:goalsID
    - ym:pv:URL
    - ym:pv:referer
    - ym:pv:UTMCampaign
    - ym:pv:UTMContent
    - ym:pv:UTMMedium
    - ym:pv:UTMSource
    - ym:pv:UTMTerm
    - ym:pv:lastTrafficSource
    - ym:pv:lastSearchEngineRoot
    - ym:pv:lastSearchEngine
    - ym:pv:lastAdvEngine
    - ym:pv:lastSocialNetwork
    - ym:pv:lastSocialNetworkProfile
    - ym:pv:recommendationSystem
    - ym:pv:messenger
    - ym:pv:operatingSystem
    - ym:pv:browser
    - ym:pv:browserMajorVersion
    - ym:pv:browserMinorVersion
    - ym:pv:browserCountry
    - ym:pv:browserEngine
    - ym:pv:browserEngineVersion1
    - ym:pv:browserEngineVersion2
    - ym:pv:browserEngineVersion3
    - ym:pv:browserEngineVersion4
    - ym:pv:browserLanguage
    - ym:pv:cookieEnabled
    - ym:pv:deviceCategory
    - ym:pv:javascriptEnabled
    - ym:pv:mobilePhone
    - ym:pv:mobilePhoneModel
    - ym:pv:operatingSystemRoot
    - ym:pv:physicalScreenHeight
    - ym:pv:physicalScreenWidth
    - ym:pv:screenColors
    - ym:pv:screenFormat
    - ym:pv:screenHeight
    - ym:pv:screenOrientation
    - ym:pv:screenOrientationName
    - ym:pv:screenWidth
    - ym:pv:windowClientHeight
    - ym:pv:windowClientWidth
    - ym:pv:ipAddress
    - ym:pv:regionCity
    - ym:pv:regionCountry
    - ym:pv:isPageView
    - ym:pv:link
    - ym:pv:download
    - ym:pv:notBounce
    - ym:pv:artificial
    - ym:pv:httpError
    - ym:pv:shareService
    - ym:pv:shareURL
    - ym:pv:shareTitle
    - ym:pv:params
    - ym:pv:offlineCallTalkDuration
    - ym:pv:offlineCallHoldDuration
    - ym:pv:offlineCallMissed
    - ym:pv:offlineCallTag
    - ym:pv:offlineCallFirstTimeCaller
    - ym:pv:offlineCallURL
  chunk_days: 1

tables:
  - name: yandex_metrika_booking.events
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: title
        sources: ["title"]
        type: string
      - target: goals_id
        sources: ["goalsID"]
        type: json
      - target: url
        sources: ["URL"]
        type: string
      - target: referer
        sources: ["referer"]
        type: string
      - target: is_page_view
        sources: ["isPageView"]
        type: boolean
      - target: link
        sources: ["link"]
        type: boolean
      - target: download
        sources: ["download"]
        type: boolean
      - target: not_bounce
        sources: ["notBounce"]
        type: boolean
      - target: artificial
        sources: ["artificial"]
        type: boolean
      - target: http_error
        sources: ["httpError"]
        type: integer
      - target: share_service
        sources: ["shareService"]
        type: string
      - target: share_url
        sources: ["shareURL"]
        type: string
      - target: share_title
        sources: ["shareTitle"]
        type: string

  - name: yandex_metrika_booking.event_params
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: params
        sources: ["params"]
        type: json

  - name: yandex_metrika_booking.marketing_info
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: utm_campaign
        sources: ["UTMCampaign"]
        type: string
      - target: utm_content
        sources: ["UTMContent"]
        type: string
      - target: utm_medium
        sources: ["UTMMedium"]
        type: string
      - target: utm_source
        sources: ["UTMSource"]
        type: string
      - target: utm_term
        sources: ["UTMTerm"]
        type: string
      - target: last_traffic_source
        sources: ["lastTrafficSource"]
        type: string
      - target: last_search_engine_root
        sources: ["lastSearchEngineRoot"]
        type: string
      - target: last_search_engine
        sources: ["lastSearchEngine"]
        type: string
      - target: last_adv_engine
        sources: ["lastAdvEngine"]
        type: string
      - target: last_social_network
        sources: ["lastSocialNetwork"]
        type: string
      - target: last_social_network_profile
        sources: ["lastSocialNetworkProfile"]
        type: string
      - target: recommendation_system
        sources: ["recommendationSystem"]
        type: string
      - target: messenger
        sources: ["messenger"]
        type: string

  - name: yandex_metrika_booking.devices
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: operating_system
        sources: ["operatingSystem"]
        type: string
      - target: browser
        sources: ["browser"]
        type: string
      - target: browser_major_version
        sources: ["browserMajorVersion"]
        type: integer
      - target: browser_minor_version
        sources: ["browserMinorVersion"]
        type: integer
      - target: browser_country
        sources: ["browserCountry"]
        type: string
      - target: browser_engine
        sources: ["browserEngine"]
        type: string
      - target: browser_engine_version1
        sources: ["browserEngineVersion1"]
        type: integer
      - target: browser_engine_version2
        sources: ["browserEngineVersion2"]
        type: integer
      - target: browser_engine_version3
        sources: ["browserEngineVersion3"]
        type: integer
      - target: browser_engine_version4
        sources: ["browserEngineVersion4"]
        type: integer
      - target: browser_language
        sources: ["browserLanguage"]
        type: string
      - target: cookie_enabled
        sources: ["cookieEnabled"]
        type: boolean
      - target: device_category
        sources: ["deviceCategory"]
        type: string
      - target: javascript_enabled
        sources: ["javascriptEnabled"]
        type: boolean
      - target: mobile_phone
        sources: ["mobilePhone"]
        type: string
      - target: mobile_phone_model
        sources: ["mobilePhoneModel"]
        type: string
      - target: operating_system_root
        sources: ["operatingSystemRoot"]
        type: string
      - target: physical_screen_height
        sources: ["physicalScreenHeight"]
        type: integer
      - target: physical_screen_width
        sources: ["physicalScreenWidth"]
        type: integer
      - target: screen_colors
        sources: ["screenColors"]
        type: integer
      - target: screen_format
        sources: ["screenFormat"]
        type: string
      - target: screen_height
        sources: ["screenHeight"]
        type: integer
      - target: screen_orientation
        sources: ["screenOrientation"]
        type: integer
      - target: screen_orientation_name
        sources: ["screenOrientationName"]
        type: string
      - target: screen_width
        sources: ["screenWidth"]
        type: integer
      - target: window_client_height
        sources: ["windowClientHeight"]
        type: integer
      - target: window_client_width
        sources: ["windowClientWidth"]
        type: integer

  - name: yandex_metrika_booking.location
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: ip_address
        sources: ["ipAddress"]
        type: string
      - target: region_city
        sources: ["regionCity"]
        type: string
      - target: region_country
        sources: ["regionCountry"]
        type: string

  - name: yandex_metrika_booking.offline_calls
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: offline_call_talk_duration
        sources: ["offlineCallTalkDuration"]
        type: integer
      - target: offline_call_hold_duration
        sources: ["offlineCallHoldDuration"]
        type: integer
      - target: offline_call_missed
        sources: ["offlineCallMissed"]
        type: integer
      - target: offline_call_tag
        sources: ["offlineCallTag"]
        type: string
      - target: offline_call_first_time_caller
        sources: ["offlineCallFirstTimeCaller"]
        type: integer
      - target: offline_call_url
        sources: ["offlineCallURL"]
        type: string
"""

WEBLK_YAML = """
source:
  type: yandex_metrika
  counter_id: 106613500
  date_from: "{start_date}"
  date_to: "{end_date}"
  source: hits
  fields:
    # Основные
    - ym:pv:watchID
    - ym:pv:pageViewID
    - ym:pv:visitID
    - ym:pv:clientID
    - ym:pv:dateTime
    - ym:pv:title
    - ym:pv:goalsID
    - ym:pv:URL
    - ym:pv:referer
    # UTM / трафик
    - ym:pv:UTMCampaign
    - ym:pv:UTMContent
    - ym:pv:UTMMedium
    - ym:pv:UTMSource
    - ym:pv:UTMTerm
    - ym:pv:lastTrafficSource
    - ym:pv:lastSearchEngineRoot
    - ym:pv:lastSearchEngine
    - ym:pv:lastAdvEngine
    - ym:pv:lastSocialNetwork
    - ym:pv:lastSocialNetworkProfile
    - ym:pv:recommendationSystem
    - ym:pv:messenger
    # Устройства
    - ym:pv:operatingSystem
    - ym:pv:browser
    - ym:pv:browserMajorVersion
    - ym:pv:browserMinorVersion
    - ym:pv:browserCountry
    - ym:pv:browserEngine
    - ym:pv:browserEngineVersion1
    - ym:pv:browserEngineVersion2
    - ym:pv:browserEngineVersion3
    - ym:pv:browserEngineVersion4
    - ym:pv:browserLanguage
    - ym:pv:cookieEnabled
    - ym:pv:deviceCategory
    - ym:pv:javascriptEnabled
    - ym:pv:mobilePhone
    - ym:pv:mobilePhoneModel
    - ym:pv:operatingSystemRoot
    - ym:pv:physicalScreenHeight
    - ym:pv:physicalScreenWidth
    - ym:pv:screenColors
    - ym:pv:screenFormat
    - ym:pv:screenHeight
    - ym:pv:screenOrientation
    - ym:pv:screenOrientationName
    - ym:pv:screenWidth
    - ym:pv:windowClientHeight
    - ym:pv:windowClientWidth
    # География
    - ym:pv:ipAddress
    - ym:pv:regionCity
    - ym:pv:regionCountry
    # Событие
    - ym:pv:isPageView
    - ym:pv:link
    - ym:pv:download
    - ym:pv:notBounce
    - ym:pv:artificial
    - ym:pv:httpError
    - ym:pv:shareService
    - ym:pv:shareURL
    - ym:pv:shareTitle
    # Параметры
    - ym:pv:params
    # Звонки
    - ym:pv:offlineCallTalkDuration
    - ym:pv:offlineCallHoldDuration
    - ym:pv:offlineCallMissed
    - ym:pv:offlineCallTag
    - ym:pv:offlineCallFirstTimeCaller
    - ym:pv:offlineCallURL
  chunk_days: 7

tables:
  # --------------------------------------------------------------------------
  # events
  # --------------------------------------------------------------------------
  - name: yandex_metrika_web_lk.events
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: title
        sources: ["title"]
        type: string
      - target: goals_id
        sources: ["goalsID"]
        type: json
      - target: url
        sources: ["URL"]
        type: string
      - target: referer
        sources: ["referer"]
        type: string
      - target: is_page_view
        sources: ["isPageView"]
        type: boolean
      - target: link
        sources: ["link"]
        type: boolean
      - target: download
        sources: ["download"]
        type: boolean
      - target: not_bounce
        sources: ["notBounce"]
        type: boolean
      - target: artificial
        sources: ["artificial"]
        type: boolean
      - target: http_error
        sources: ["httpError"]
        type: integer
      - target: share_service
        sources: ["shareService"]
        type: string
      - target: share_url
        sources: ["shareURL"]
        type: string
      - target: share_title
        sources: ["shareTitle"]
        type: string

  # --------------------------------------------------------------------------
  # event_params
  # --------------------------------------------------------------------------
  - name: yandex_metrika_web_lk.event_params
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: params
        sources: ["params"]
        type: json

  # --------------------------------------------------------------------------
  # marketing_info
  # --------------------------------------------------------------------------
  - name: yandex_metrika_web_lk.marketing_info
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: utm_campaign
        sources: ["UTMCampaign"]
        type: string
      - target: utm_content
        sources: ["UTMContent"]
        type: string
      - target: utm_medium
        sources: ["UTMMedium"]
        type: string
      - target: utm_source
        sources: ["UTMSource"]
        type: string
      - target: utm_term
        sources: ["UTMTerm"]
        type: string
      - target: last_traffic_source
        sources: ["lastTrafficSource"]
        type: string
      - target: last_search_engine_root
        sources: ["lastSearchEngineRoot"]
        type: string
      - target: last_search_engine
        sources: ["lastSearchEngine"]
        type: string
      - target: last_adv_engine
        sources: ["lastAdvEngine"]
        type: string
      - target: last_social_network
        sources: ["lastSocialNetwork"]
        type: string
      - target: last_social_network_profile
        sources: ["lastSocialNetworkProfile"]
        type: string
      - target: recommendation_system
        sources: ["recommendationSystem"]
        type: string
      - target: messenger
        sources: ["messenger"]
        type: string

  # --------------------------------------------------------------------------
  # devices
  # --------------------------------------------------------------------------
  - name: yandex_metrika_web_lk.devices
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: operating_system
        sources: ["operatingSystem"]
        type: string
      - target: browser
        sources: ["browser"]
        type: string
      - target: browser_major_version
        sources: ["browserMajorVersion"]
        type: integer
      - target: browser_minor_version
        sources: ["browserMinorVersion"]
        type: integer
      - target: browser_country
        sources: ["browserCountry"]
        type: string
      - target: browser_engine
        sources: ["browserEngine"]
        type: string
      - target: browser_engine_version1
        sources: ["browserEngineVersion1"]
        type: integer
      - target: browser_engine_version2
        sources: ["browserEngineVersion2"]
        type: integer
      - target: browser_engine_version3
        sources: ["browserEngineVersion3"]
        type: integer
      - target: browser_engine_version4
        sources: ["browserEngineVersion4"]
        type: integer
      - target: browser_language
        sources: ["browserLanguage"]
        type: string
      - target: cookie_enabled
        sources: ["cookieEnabled"]
        type: boolean
      - target: device_category
        sources: ["deviceCategory"]
        type: string
      - target: javascript_enabled
        sources: ["javascriptEnabled"]
        type: boolean
      - target: mobile_phone
        sources: ["mobilePhone"]
        type: string
      - target: mobile_phone_model
        sources: ["mobilePhoneModel"]
        type: string
      - target: operating_system_root
        sources: ["operatingSystemRoot"]
        type: string
      - target: physical_screen_height
        sources: ["physicalScreenHeight"]
        type: integer
      - target: physical_screen_width
        sources: ["physicalScreenWidth"]
        type: integer
      - target: screen_colors
        sources: ["screenColors"]
        type: integer
      - target: screen_format
        sources: ["screenFormat"]
        type: string
      - target: screen_height
        sources: ["screenHeight"]
        type: integer
      - target: screen_orientation
        sources: ["screenOrientation"]
        type: integer
      - target: screen_orientation_name
        sources: ["screenOrientationName"]
        type: string
      - target: screen_width
        sources: ["screenWidth"]
        type: integer
      - target: window_client_height
        sources: ["windowClientHeight"]
        type: integer
      - target: window_client_width
        sources: ["windowClientWidth"]
        type: integer

  # --------------------------------------------------------------------------
  # location
  # --------------------------------------------------------------------------
  - name: yandex_metrika_web_lk.location
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: ip_address
        sources: ["ipAddress"]
        type: string
      - target: region_city
        sources: ["regionCity"]
        type: string
      - target: region_country
        sources: ["regionCountry"]
        type: string

  # --------------------------------------------------------------------------
  # offline_calls
  # --------------------------------------------------------------------------
  - name: yandex_metrika_web_lk.offline_calls
    primary_key: [watch_id, date_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: page_view_id
        sources: ["pageViewID"]
        type: integer
      - target: visit_id
        sources: ["visitID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      - target: date_time
        sources: ["dateTime"]
        type: datetime
      - target: offline_call_talk_duration
        sources: ["offlineCallTalkDuration"]
        type: integer
      - target: offline_call_hold_duration
        sources: ["offlineCallHoldDuration"]
        type: integer
      - target: offline_call_missed
        sources: ["offlineCallMissed"]
        type: integer
      - target: offline_call_tag
        sources: ["offlineCallTag"]
        type: string
      - target: offline_call_first_time_caller
        sources: ["offlineCallFirstTimeCaller"]
        type: integer
      - target: offline_call_url
        sources: ["offlineCallURL"]
        type: string
"""

# ---------- Вспомогательная функция для подстановки дат в YAML ----------
def get_yaml_with_dates(template, start_date, end_date):
    return template.format(start_date=start_date, end_date=end_date)

# ---------- Основная функция загрузки данных ----------
def load_data(
    table_name: str,
    date_field: str,
    yaml_template: str,
    source_name: str,
    **context
):
    """
    Универсальная функция для загрузки данных из источника.
    :param table_name: полное имя таблицы (схема.таблица)
    :param date_field: имя поля с датой (например, 'event_datetime' или 'date_time')
    :param yaml_template: строка шаблона YAML с плейсхолдерами {start_date} и {end_date}
    :param source_name: название источника для логов
    """
    hook = PostgresHook(postgres_conn_id='dwh_pg')
    sql = f"SELECT max({date_field}) FROM {table_name}"
    result = hook.get_first(sql)
    max_date = result[0] if result and result[0] else None

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    if max_date is None:
        # Если таблица пуста, загружаем последние 7 дней
        start_date = yesterday - timedelta(days=7)
        end_date = yesterday
        logging.warning(f"Таблица {table_name} пуста, загружаем за последние 7 дней ({start_date} - {end_date})")
    else:
        # Приводим к date (если datetime)
        if isinstance(max_date, datetime):
            max_date = max_date.date()
        if max_date < yesterday:
            start_date = max_date + timedelta(days=1)
            end_date = yesterday
        else:
            logging.info(f"Данные за вчера ({yesterday}) уже есть в {table_name}, пропускаем.")
            return  # не отправляем запрос

    # Формируем YAML с подставленными датами
    yaml_payload = get_yaml_with_dates(
        yaml_template,
        start_date.isoformat(),
        end_date.isoformat()
    )

    # Отправляем запрос
    url = f"{DWH_HELPER_URL}/etl/transformer?start_after_line=0"
    try:
        response = requests.post(url, data=yaml_payload, headers=HEADERS, timeout=72000)
        response.raise_for_status()
        resp_json = response.json()
        if resp_json.get('status') == 'success':
            logging.info(f"Данные для {source_name} успешно загружены за период {start_date} - {end_date}")
        else:
            raise ValueError(f"Неожиданный ответ от API: {resp_json}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке {source_name}: {e}")
        raise

# ---------- DAG ----------
default_args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='etl_yandex_appmetrica',
    default_args=default_args,
    schedule='0 3 * * *',
    description='Ежедневная загрузка данных из AppMetrica и Яндекс.Метрики в DWH',
    tags=['etl', 'appmetrica', 'yandex'],
) as dag:

    task_appmetrica = PythonOperator(
        task_id='load_appmetrica',
        python_callable=load_data,
        op_kwargs={
            'table_name': 'appmetrica.events',
            'date_field': 'event_datetime',
            'yaml_template': APPMETRICA_YAML,
            'source_name': 'AppMetrica'
        },
    )

    task_booking = PythonOperator(
        task_id='load_yandex_booking',
        python_callable=load_data,
        op_kwargs={
            'table_name': 'yandex_metrika_booking.events',
            'date_field': 'date_time',
            'yaml_template': BOOKING_YAML,
            'source_name': 'Yandex Metrika Booking'
        },
    )

    task_web_lk = PythonOperator(
        task_id='load_yandex_web_lk',
        python_callable=load_data,
        op_kwargs={
            'table_name': 'yandex_metrika_web_lk.events',
            'date_field': 'date_time',
            'yaml_template': WEBLK_YAML,
            'source_name': 'Yandex Metrika Web LK'
        },
    )

    task_appmetrica >> task_booking >> task_web_lk

