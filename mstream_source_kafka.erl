-module(mstream_source_kafka).

-behaviour(brod_group_subscriber).

-export([
    bootstrap/0,
    kafka_status/0,
    os_time_utc_str/0
]).

%% behabviour callbacks
-export([ init/2
    , handle_message/4
    , get_committed_offsets/3
]).


-include_lib("brod/include/brod.hrl").
-include("mstream.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

-record(state, { group_id :: binary()
    , offset_dir :: file:fd()
    , avro_store :: ets:tid()
}).




kafka_status() ->
    {ok, Hosts} = application:get_env(mstream, kafka_brokers),
    brod:get_metadata(Hosts).


%% @doc This function bootstraps everything to demo of group subscriber.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-demo-group-subscriber-loc">>
%%     having two or more partitions.
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - X group subscribers, X is the number of partitions
%%
%% * consumed sequence numbers are printed to console
%% * consumed offsets are written to file /tmp/T/P.offset
%%   where T is the topic name and X is the partition number
%% @end
-spec bootstrap() -> ok.

bootstrap() ->
    ClientId = ?MODULE,
    {ok, BootstrapHosts} = application:get_env(mstream, kafka_brokers),
    {ok, Topic} = application:get_env(mstream, consumer_topic_name),
    ClientConfig = [],
    GroupId = iolist_to_binary([Topic, "-mstream_source_kafka", "-group-id"]),
    case application:ensure_all_started(brod) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Error} ->
            erlang:throw({error, Error})
    end,
    case brod:start_client(BootstrapHosts, ClientId, ClientConfig) of
        ok ->
            ok;
        {error,{already_started, _PID}} ->
            ok;
        {error, Error2} ->
            erlang:throw({error, Error2})
    end,

    {ok, PartitionCount} = brod:get_partitions_count(ClientId, Topic),
    %% spawn N + 1 consumers, one of them will have no assignment
    %% it should work as a 'standing-by' consumer.
    ok = spawn_consumers(GroupId, ClientId, Topic, PartitionCount + 1),
    ok.

%% @doc Initialize nothing in our case.
init(GroupId, []) ->
    {ok, OffsetDir} = application:get_env(mstream, offset_file_dir),
    Store = avro_schema_store:new([], ["./config/flume_event.avsc"]),
    mstream_stat:set({kafka_consumer_pid, GroupId, erlang:self()}, erlang:self()),
    {ok, #state{ group_id   = GroupId
        , offset_dir = OffsetDir
        , avro_store = Store
    }}.

%% @doc Handle one message (not message-set).
handle_message(Topic, Partition, Message,
    #state{ offset_dir = Dir
        , group_id   = GroupId
        , avro_store =  Store
    } = State) ->
    #kafka_message{ offset = Offset
        , value  = Value
    } = Message,
    %% @todo 流控
    case do_process_message_data(Offset, Value, Store) of
        ok ->
            ok = commit_offset(Dir, GroupId, Topic, Partition, Offset),
            ?INFO_MSG("offset acked: ~p", [Offset]),
            {ok, ack, State};
        error ->
            ok = commit_offset(Dir, GroupId, Topic, Partition, Offset),
            ?ERROR_MSG("offset process failed: ~p", [Offset]),
            {ok, ack, State}
    end.

%% @doc This callback is called whenever there is a new assignment received.
%% e.g. when joining the group after restart, or group assigment rebalance
%% was triggered if other memgers join or leave the group
%% NOTE: A subscriber may get assigned with a random set of topic-partitions
%%       (unless some 'sticky' protocol is introduced to group controller),
%%       meaning, if group members are running in different hosts they may
%%       have to perform 'Local Offset Commit' in a central database or
%%       whatsoever instead of local file system.
%% @end
get_committed_offsets(GroupId, TopicPartitions,
    #state{offset_dir = Dir} = State) ->
    F = fun({Topic, Partition}, Acc) ->
        case file:read_file(filename(Dir, GroupId, Topic, Partition)) of
            {ok, OffsetBin} ->
                OffsetStr = string:strip(binary_to_list(OffsetBin), both, $\n),
                Offset = list_to_integer(OffsetStr),
                [{{Topic, Partition}, Offset} | Acc];
            {error, enoent} ->
                [{{Topic, Partition}, -2} | Acc]
        end
        end,
    {ok, lists:foldl(F, [], TopicPartitions), State}.

%%%_* Internal Functions =======================================================

filename(Dir, GroupId, Topic, Partition) ->
    filename:join([Dir, GroupId, Topic, integer_to_list(Partition)]).

commit_offset(Dir, GroupId, Topic, Partition, Offset) ->
    Filename = filename(Dir, GroupId, Topic, Partition),
    ok = filelib:ensure_dir(Filename),
    ok = file:write_file(Filename, [integer_to_list(Offset), $\n]).

spawn_consumers(GroupId, ClientId, Topic, ConsumerCount) ->
    %% commit offsets to kafka every 10 seconds
    GroupConfig = [{offset_commit_policy, consumer_managed}],
    lists:foreach(
        fun(_I) ->
            %% @todo 监控进程,定时检查状态
            {ok, _Subscriber} =
                brod_group_subscriber:start_link(ClientId, GroupId, [Topic],
                    GroupConfig,
                    _ConsumerConfig  = [],
                    _CallbackModule  = ?MODULE,
                    _CallbackInitArg = []),
            mstream_stat:set({kafka_consumer_subsciber, ClientId}, _Subscriber)
        end, lists:seq(1, ConsumerCount)).

-spec os_time_utc_str() -> string().
os_time_utc_str() ->
    Ts = os:timestamp(),
    {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
    {_, _, Micro} = Ts,
    S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
        [Y, M, D, H, Min, Sec, Micro]),
    lists:flatten(S).


%% 处理消息数据
do_process_message_data(Offset, Data, Store) ->
    try
        ValueDecoded = avro_binary_decoder:decode(Data, "org.apache.flume.source.avro.AvroFlumeEvent", Store),
        [{"headers", Headers}, {"body", Body}] = ValueDecoded,
        Partition = proplists:get_value("partition", Headers),
        Database = proplists:get_value("database", Headers),
        Table = proplists:get_value("table", Headers),
        Fields = proplists:get_value("fields", Headers),
        %% @todo 不同数据的分区并不同 配置化解码?
        %% @todo 后面要求写入kafka的数据中直接带有字段 header : agent_id server_id
        PartitionInfoList = string:tokens(Partition, "/"),
        PartitionInfoMap =
            lists:foldl(
                fun(PartitionInfo, Acc) ->
                    [Key, Value] = string:tokens(PartitionInfo, "="),
                    maps:put(Key, Value, Acc)
                end, maps:new(), PartitionInfoList),
        AppID = maps:get("app_id", PartitionInfoMap, undefined),
        case AppID of
            undefined ->
                ok;
            _ ->
                AgentID = maps:get("agent_id", PartitionInfoMap),
                ServerID = maps:get("server_id", PartitionInfoMap),
                DT = maps:get("dt", PartitionInfoMap),
                case mstream_record:transform({Database, Table, AppID, AgentID, ServerID, DT, Fields, Body}) of
                    {ok, Record} ->
                        mstream_router:route(Offset, Record);
                    {error, Error} ->
                        ?ERROR_MSG("transform failed ~p ~p", [Error, Offset]),
                        ok
                end
        end
    catch
        ErrorType: ErrorInfo  ->
            lager:error("process message data failed: ~p ~n ~p ~n", [ErrorType, ErrorInfo]),
            error
    end.

