-- 0当次搜索是否有返回结果select * from tmp_cgy_1018_01
-- select search_id,query,count(1) from (select distinct p_date,search_id,query,search_result_flag from tmp_cgy_1018_01) group by search_id,query having count(1)>1
drop table tmp_cgy_1018_01;
create table tmp_cgy_1018_01 as
select  search_result_tab.p_date
    ,nvl(get_json_object(search_result_tab.value, '$.searchId' ) ,get_json_object( search_result_tab.value, '$.commonEV.searchId' )) as search_id
    ,regexp_replace(get_json_object( value, '$.query' ), '\\u0001', '') AS query-- 搜索关键字
    ,max(if(get_json_object(search_result_tab.value, '$.result' )='true',1,0))    AS search_result_flag -- 搜索是否有结果标识[1是；0否]   -- 取有返回结果优先
FROM csig_medicaldata :: dwd_biz_action_d as search_result_tab
WHERE p_date=20211017
and search_result_tab.event = 'searchoverall.result'
AND nvl(get_json_object( search_result_tab.value, '$.searchId' ) ,get_json_object( search_result_tab.value, '$.commonEV.searchId' )) IS NOT null
group by search_result_tab.p_date
    ,nvl(get_json_object(search_result_tab.value, '$.searchId' ) ,get_json_object( search_result_tab.value, '$.commonEV.searchId' ))
    ,regexp_replace(get_json_object( value, '$.query' ), '\\u0001', '')
;

-- 1搜索 select * from tmp_cgy_1018_02
drop table tmp_cgy_1018_02;
create table tmp_cgy_1018_02 as
SELECT search_tab.p_date
    ,nvl(get_json_object( search_tab.value, '$.searchId' ) ,get_json_object( search_tab.value, '$.commonEV.searchId' )) AS search_id
    ,search_tab.uin
    ,'' AS open_id
    ,'' AS channel
    ,decode(search_tab.product,'health.deluxe','腾讯健康','yidian','腾讯医典','dianshang','电商','未知') AS product -- 产品 腾讯健康，腾讯医典，电商
    ,search_tab.platform                                                                                        -- 平台名称 ：Android，iOS，miniprogram 其他
    ,regexp_replace(get_json_object( search_tab.value, '$.query' ),'\\u0001', '') AS query                      -- 搜索关键字
    ,'' has_result -- 搜索是否有结果
    ,IF(search_tab.event = 'search_go_to' , get_json_object( search_tab.value, '$.position' ),'')AS position    -- 搜索入口
    ,'' AS refresh_num              -- 第几刷
    ,'' AS position_num             -- 第几位
    ,'' AS cid                      -- 内容ID
    ,'' AS title                    -- 内容标题
    ,search_tab.event               -- 事件
    ,'' AS event_detail             -- 事件内容
    ,'' AS card_id                  -- 卡片ID
    ,'' AS intent                  -- 搜索意图
    ,search_tab.url_adtag AS adtag          -- 渠道：
    ,search_tab.stat_url                    -- 当前页面url
    ,search_tab.stat_referer                -- 前置页面url
    ,'' AS tab                   -- 当前tab
    -- ,search_tab.country                     -- 国家
    -- ,search_tab.province                    -- 省份
    ,search_tab.city                        -- 城市
    ,search_tab.net_type                    -- 网络类型 --这个网络类型的取出来全是空的
    ,regexp_extract(user_agent, 'NetType\\/{1}([a-zA-Z0-9]+?) Language\\/',1) as net_type_user_agent -- 网络类型(user_agent中取的)
    -- 从user_agent取网络类型
    ,get_json_object(search_tab.origin_content, '$.brand' )AS brand                 -- 品牌
    ,get_json_object(search_tab.origin_content, '$.device_model')AS device_model   -- 机型
    ,stay_time  AS  duration                -- 停留时长

    ,search_tab.session_id
    ,search_tab.value pri_value             -- 私参
    -- select *
FROM csig_medicaldata :: dwd_biz_action_d as search_tab
WHERE search_tab.p_date=20211017
and search_tab.event = 'search_go_to'
AND nvl(get_json_object( search_tab.value, '$.searchId' ) ,get_json_object( search_tab.value, '$.commonEV.searchId' )) IS NOT null
;

-- 2曝光
drop table tmp_cgy_1018_03;
create table tmp_cgy_1018_03 as
SELECT expo_tab.p_date
    ,nvl(get_json_object( expo_tab.inner_event, '$.searchId' ) ,get_json_object( expo_tab.inner_event, '$.commonEV.searchId' )) AS search_id
    ,expo_tab.uin
    ,'' AS open_id
    ,'' AS channel
    ,decode(expo_tab.product,'health.deluxe','腾讯健康','yidian','腾讯医典','dianshang','电商','未知') AS product -- 产品 腾讯健康，腾讯医典，电商
    ,expo_tab.platform                                                                                        -- 平台名称 ：Android，iOS，miniprogram 其他
    ,regexp_replace(get_json_object( expo_tab.inner_event, '$.query' ),'\\u0001', '') AS search_query                      -- 搜索关键字
    ,'' has_result -- 搜索是否有结果
    ,'' AS position    -- 搜索入口
    ,'' AS refresh_num              -- 第几刷
    ,get_json_object( inner_event, '$.index' ) AS position_num             -- 第几位
    ,get_json_object( inner_event, '$.docid' ) AS cid                      -- 内容ID
    ,get_json_object( inner_event, '$.title' ) AS title                    -- 内容标题
    ,expo_tab.event               -- 事件
    ,'' AS event_detail             -- 事件内容
    ,get_json_object( inner_event, '$.cardId' ) AS card_id                 -- 卡片ID
    ,'' AS intent                  -- 搜索意图
    ,expo_tab.url_adtag AS adtag          -- 渠道：
    ,expo_tab.stat_url                    -- 当前页面url
    ,expo_tab.stat_referer                -- 前置页面url
    ,get_json_object( inner_event, '$.tab' ) AS tab                   -- 当前tab
    -- ,expo_tab.country                     -- 国家
    -- ,expo_tab.province                    -- 省份
    ,expo_tab.city                        -- 城市
    ,expo_tab.net_type                    -- 网络类型 --这个网络类型的取出来全是空的
    ,regexp_extract(user_agent, 'NetType\\/{1}([a-zA-Z0-9]+?) Language\\/',1) as net_type_user_agent -- 网络类型(user_agent中取的)
    -- 从user_agent取网络类型
    ,get_json_object(expo_tab.origin_content, '$.brand' )AS brand                 -- 品牌
    ,get_json_object(expo_tab.origin_content, '$.device_model')AS device_model   -- 机型
    ,stay_time  AS  duration                -- 停留时长

    ,expo_tab.session_id
    ,expo_tab.inner_event pri_value             -- 私参
    -- select *
FROM
(
    SELECT *
    FROM csig_medicaldata :: dwd_biz_action_d
    lateral VIEW explode(split( regexp_extract( get_json_object( value, '$.list' ), '^\\\[(.*)\\\]$', 1 ), '(?<=\\\}),(?=\\\{)' )) event_table AS inner_event
    WHERE p_date = 20211017
    AND event = 'yidian.exposure'
) expo_tab WHERE get_json_object( inner_event, '$.searchId' ) IS NOT null
limit 100



-- 3点击 select * from tmp_cgy_1018_04
drop table tmp_cgy_1018_04;
create table tmp_cgy_1018_04 as
select    clk_tab.p_date
    ,nvl(get_json_object( clk_tab.value, '$.searchId' ) ,get_json_object( clk_tab.value, '$.commonEV.searchId' )) AS search_id
    ,clk_tab.uin
    ,'' AS open_id
    ,'' AS channel
    ,decode(clk_tab.product,'health.deluxe','腾讯健康','yidian','腾讯医典','dianshang','电商','未知') AS product -- 产品 腾讯健康，腾讯医典，电商
    ,clk_tab.platform                                                                                        -- 平台名称 ：Android，iOS，miniprogram 其他
    ,regexp_replace(get_json_object( clk_tab.value, '$.query' ),'\\u0001', '') AS search_query                      -- 搜索关键字
    ,'' has_result -- 搜索是否有结果
    ,'' AS position    -- 搜索入口
    ,'' AS refresh_num              -- 第几刷
    ,get_json_object( value, '$.index' ) AS position_num             -- 第几位
    ,get_json_object( value, '$.docid' ) AS cid                      -- 内容ID
    ,get_json_object( value, '$.title' ) AS title                    -- 内容标题
    ,clk_tab.event               -- 事件
    ,'' AS event_detail             -- 事件内容
    ,get_json_object( value, '$.cardId' ) AS card_id                 -- 卡片ID
    ,'' AS intent                  -- 搜索意图
    ,clk_tab.url_adtag AS adtag          -- 渠道：
    ,clk_tab.stat_url                    -- 当前页面url
    ,clk_tab.stat_referer                -- 前置页面url
    ,get_json_object( value, '$.tab' ) AS tab                   -- 当前tab
    -- ,clk_tab.country                     -- 国家
    -- ,clk_tab.province                    -- 省份
    ,clk_tab.city                        -- 城市
    ,clk_tab.net_type                    -- 网络类型 --这个网络类型的取出来全是空的
    ,regexp_extract(user_agent, 'NetType\\/{1}([a-zA-Z0-9]+?) Language\\/',1) as net_type_user_agent -- 网络类型(user_agent中取的)
    -- 从user_agent取网络类型
    ,get_json_object(clk_tab.origin_content, '$.brand' )AS brand                 -- 品牌
    ,get_json_object(clk_tab.origin_content, '$.device_model')AS device_model   -- 机型
    ,stay_time  AS  duration                -- 停留时长

    ,clk_tab.session_id
    ,clk_tab.value pri_value             -- 私参
    -- select *
FROM csig_medicaldata :: dwd_biz_action_d as clk_tab
WHERE clk_tab.p_date=20211017
and clk_tab.event in ('searchoverall.total', 'searchlist.itemclk') -- 点击事件
AND nvl(get_json_object( clk_tab.value, '$.searchId' ) ,get_json_object( clk_tab.value, '$.commonEV.searchId' )) IS NOT null
;

-- 6给搜索数据打上意图，是否有返回结果标签
-- select * from tmp_cgy_1018_05  -- 130145
drop table tmp_cgy_1018_05;
create table tmp_cgy_1018_05 as
select search_event_tab.p_date
    ,search_event_tab.search_id
    ,search_event_tab.uin
    ,search_event_tab.open_id
    ,search_event_tab.channel
    ,search_event_tab.product                                   -- 产品 腾讯健康，腾讯医典，电商
    ,search_event_tab.platform                                  -- 平台名称 ：Android，iOS，miniprogram 其他
    ,search_event_tab.query                                      -- 搜索关键字
    ,search_result_tab.search_result_flag AS has_result         -- 搜索是否有结果
    ,search_event_tab.position                    -- 搜索入口
    ,search_event_tab.refresh_num                 -- 第几刷
    ,search_event_tab.position_num                -- 第几位
    ,search_event_tab.cid                         -- 内容ID
    ,search_event_tab.title                       -- 内容标题
    ,search_event_tab.event                       -- 事件
    ,search_event_tab.event_detail                -- 事件内容
    ,search_event_tab.card_id                     -- 卡片ID
    ,intent_tab.one_level as intent               -- 搜索意图
    ,intent_tab.two_level as intent2              -- 搜索意图
    ,search_event_tab.adtag                       -- 渠道
    ,search_event_tab.stat_url                    -- 当前页面url
    ,search_event_tab.stat_referer                -- 前置页面url
    ,search_event_tab.tab                         -- 当前tab
    -- ,search_tab.country                        -- 国家
    -- ,search_tab.province                       -- 省份
    ,search_event_tab.city                        -- 城市
    ,search_event_tab.net_type                    -- 网络类型 --这个网络类型的取出来全是空的
    ,search_event_tab.net_type_user_agent         -- 网络类型(user_agent中取的)-- 从user_agent取网络类型
    ,search_event_tab.brand                       -- 品牌
    ,search_event_tab.device_model                -- 机型
    ,search_event_tab.duration                    -- 停留时长
    ,search_event_tab.session_id                  -- session_id
    ,search_event_tab.pri_value                   -- 私参
    -- select count(1)
from tmp_cgy_1018_02 AS  search_event_tab
left join (-- 二、当次搜索是否有结果
    select p_date,
       search_id,
       query,
       search_result_flag -- 用户搜索是否有结果标识
    from tmp_cgy_1018_01
)AS search_result_tab
    on search_event_tab.p_date=search_result_tab.p_date
    and search_event_tab.search_id=search_result_tab.search_id
    and search_event_tab.query=search_result_tab.query
left join ( -- 三、健康意图
    SELECT search_id,
        max(get_json_object(resp, "$.L1intent")) AS one_level,
        max(get_json_object(resp, "$.L2intent")) AS two_level
    FROM
        csig_medicalbaike_search_interface :: t_medsar_svrlog_fdt0
    WHERE
        tdbank_imp_date = 20211017
        AND func = 'SearchSAAggrVideo4Wyw'
    group by search_id
)AS intent_tab
    on search_event_tab.search_id = intent_tab.search_id
;

-- 7搜索结果点击查看更多次数 select * from tmp_cgy_1018_06
drop table tmp_cgy_1018_06;
create table tmp_cgy_1018_06 as
select p_date
    ,event
    ,nvl(get_json_object( clk_more_tab.value, '$.searchId' ) ,get_json_object( clk_more_tab.value, '$.commonEV.searchId' )) AS search_id
    ,regexp_replace(get_json_object( clk_more_tab.value, '$.query' ),'\\u0001', '') AS search_query                      -- 搜索关键字
    ,get_json_object( value, '$.tab' ) AS tab
    ,count(1) as clk_more_cnt
from csig_medicaldata :: dwd_biz_action_d  clk_more_tab
where  p_date = 20211017
AND event = 'searchlist.loadmore'
group by  p_date
    ,event
    ,nvl(get_json_object( clk_more_tab.value, '$.searchId' ) ,get_json_object( clk_more_tab.value, '$.commonEV.searchId' ))
    ,regexp_replace(get_json_object( clk_more_tab.value, '$.query' ),'\\u0001', '')                -- 搜索关键字
    ,get_json_object( value, '$.tab' )
;

-- 8合并搜索+曝光+点击数据（曝光+点击的意图+position从搜索中获取）
-- 1739000 select * from tmp_cgy_1018_07 where refresh_num is not null
drop table tmp_cgy_1018_07;
create table tmp_cgy_1018_07 as
select all_tab.p_date
    ,all_tab.search_id
    ,all_tab.uin
    ,user_open_id_tab.open_id                           -- open_id
    ,all_tab.channel
    ,all_tab.product                                   -- 产品 腾讯健康，腾讯医典，电商
    ,all_tab.platform                                  -- 平台名称 ：Android，iOS，miniprogram 其他
    ,all_tab.query                                      -- 搜索关键字
    ,nvl(search_tab.has_result,0) AS has_result         -- 搜索是否有结果
    ,nvl(search_tab.position,0) AS position                    -- 搜索入口
    ,clk_more_tab.clk_more_cnt as refresh_num                 -- 第几刷
    ,all_tab.position_num                -- 第几位
    ,all_tab.cid                         -- 内容ID
    ,all_tab.title                       -- 内容标题
    ,decode(all_tab.event,'search_go_to','搜索','yidian.expouse','曝光','点击')as  event                      -- 事件
    ,all_tab.event_detail                -- 事件内容
    ,all_tab.card_id                     -- 卡片ID
    ,nvl(search_tab.intent,0) AS intent           -- 搜索意图1
    ,nvl(search_tab.intent2,0) AS intent2         -- 搜索意图2
    ,all_tab.adtag                       -- 渠道
    ,all_tab.stat_url                    -- 当前页面url
    ,all_tab.stat_referer                -- 前置页面url
    ,all_tab.tab                         -- 当前tab
    -- ,all_tab.country                        -- 国家
    -- ,all_tab.province                       -- 省份
    ,all_tab.city                        -- 城市
    ,all_tab.net_type                    -- 网络类型 --这个网络类型的取出来全是空的
    ,all_tab.net_type_user_agent         -- 网络类型(user_agent中取的)-- 从user_agent取网络类型
    ,all_tab.brand                       -- 品牌
    ,all_tab.device_model                -- 机型
    ,all_tab.duration                    -- 停留时长
    ,all_tab.session_id                  -- session_id
    ,all_tab.pri_value                   -- 私参
    -- select count(1)
from(
    select * from tmp_cgy_1018_02
    union all select * from tmp_cgy_1018_03
    union all select * from tmp_cgy_1018_04
)all_tab
left join ( --曝光和点击的 意图+入口+是否有结果  打上和搜索一样的值
    select search_id,query, intent,intent2,position,has_result
    from tmp_cgy_1018_05
) search_tab
    on all_tab.search_id=search_tab.search_id
    and all_tab.query=search_tab.query
-- 用户当次搜索点击了几次查看更多
left join tmp_cgy_1018_06 clk_more_tab
    on all_tab.search_id=clk_more_tab.search_id
    and all_tab.query=clk_more_tab.search_query
    and all_tab.tab=clk_more_tab.tab
left join(
    select uin,max(oid) as open_id
    from dwd_oid2uin
    where p_date =20211017 -- 需要限制最大日期，表只保存7天数据
    group by uin
) user_open_id_tab
    on all_tab.uin=user_open_id_tab.uin
;

