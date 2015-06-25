-module(kinetic_stream).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, terminate/2, code_change/3,
         handle_info/2]).

-export([stop/2, start_link/2, put_record/3]).
-export([flush/2]).


% I dislike this soooo much
-ifdef(TEST).
-export([get_stream/2, send_to_kinesis/5]).
-endif.

-include("kinetic.hrl").
-record(kinetic_stream, {
        stream_name                 = <<"dev">> :: binary(),
        base_partition_name         = list_to_binary(net_adm:localhost()) :: binary(),
        partitions_number           = 1000 :: pos_integer() | 'infinity',
        timeout                     = 5000 :: pos_integer(),
        buffer                      = <<"">> :: binary(),
        buffer_size                 = 0 :: pos_integer(),
        current_partition_num       = 0 :: pos_integer(),
        flush_interval              = 1000 :: pos_integer(),
        retries                     = 3 :: pos_integer(),
        compress                    = false :: boolean()
    }).

start_link(StreamName, Config) ->
    gen_server:start_link(?MODULE, [StreamName, Config], []).

stop(StreamName, Config) ->
    Stream = get_stream(StreamName, Config),
    gen_server:call(Stream, stop).


put_record(StreamName, Config, Data) ->
    DataSize = erlang:size(Data),
    case DataSize > ?KINESIS_MAX_PUT_SIZE of
        true ->
            {error, max_size_exceeded};
        false ->
            Stream = get_stream(StreamName, Config),
            gen_server:cast(Stream, {put_record, Data, DataSize}, infinity)
    end.

flush(StreamName, Config) ->
    Stream = get_stream(StreamName, Config),
    Stream ! flush.

% gen_server behavior
init([StreamName, Options]) ->
    process_flag(trap_exit, true),
    case ets:insert_new(?KINETIC_STREAM, {StreamName, self()}) of
        true ->
            Stream = lists:foldl(fun build_stream/2, #kinetic_stream{stream_name = StreamName}, Options),
            {ok, Stream, Stream#kinetic_stream.flush_interval};
        false ->
            ignore
    end.

build_stream({partition, PartitionName}, KS) when is_binary(PartitionName) ->
    KS#kinetic_stream{base_partition_name = PartitionName};
build_stream({partition_count, PartitionCount}, KS) when is_integer(PartitionCount), PartitionCount >= 0; PartitionCount == 'infinity' ->
    KS#kinetic_stream{partitions_number = PartitionCount};
build_stream({timeout, Timeout}, KS) when is_integer(Timeout), Timeout >= 0 ->
    KS#kinetic_stream{timeout = Timeout};
build_stream({flush_interval, FlushInterval}, KS) when is_integer(FlushInterval), FlushInterval >= 0 ->
    KS#kinetic_stream{flush_interval = FlushInterval};
build_stream({retries, Retries}, KS) when is_integer(Retries), Retries >= 0 ->
    KS#kinetic_stream{retries = Retries};
build_stream({compress, Compress}, KS) when is_boolean(Compress) ->
    KS#kinetic_stream{compress = Compress}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({put_record, Data, DataSize}, State=#kinetic_stream{buffer=Buffer, buffer_size=BSize}) ->
    {Buffer2, BSize2} = append_buffer(Data, DataSize, Buffer, BSize),
    if
        % buffer + Data is bigger than (or equal to) ?KINESIS_MAX_PUT_SIZE
        BSize2 >= ?KINESIS_MAX_PUT_SIZE ->
            NewState = internal_flush(State),
            {reply, ok, NewState#kinetic_stream{buffer_size = DataSize, buffer = Data}, NewState#kinetic_stream.flush_interval};
        % buffer + Data is not bigger than ?KINESIS_MAX_PUT_SIZE
        true ->
            {reply, ok, State#kinetic_stream{buffer= Buffer2, buffer_size=BSize2}, State#kinetic_stream.flush_interval}
    end.

terminate(_Reason, #kinetic_stream{stream_name=StreamName}) ->
    ets:delete(?MODULE, StreamName),
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

handle_info(timeout, State) ->
    NewState = internal_flush(State),
    {noreply, NewState, NewState#kinetic_stream.flush_interval}.

% Internal implementation
get_stream(StreamName, Config) ->
    case ets:lookup(?KINETIC_STREAM, StreamName) of
        [] ->
            case supervisor:start_child(kinetic_stream_sup, [StreamName, Config]) of
                {ok, undefined} -> get_stream(StreamName, Config);
                {ok, Pid} -> Pid
            end;
        [{_Name, Pid}] ->
            case is_process_alive(Pid) of
                true -> Pid;
                false ->
                    ets:delete(?KINETIC_STREAM , StreamName),
                    get_stream(StreamName, Config)
            end
    end.

append_buffer(Data, DataSize, <<>>, 0) ->
    {Data, DataSize};
append_buffer(Data, DataSize, Buffer, BufferSize) ->
    Delimiter = <<?KINETIC_DELIMITER>>,
    DelimiterSize = byte_size(Delimiter),
    Buffer2 = <<Buffer/binary, Delimiter/binary, Data/binary>>,
    BufferSize2 = BufferSize + DataSize + DelimiterSize,
    {Buffer2, BufferSize2}.

internal_flush(State=#kinetic_stream{buffer= <<"">>}) ->
    State;
internal_flush(State=#kinetic_stream{stream_name=StreamName,
                                     buffer=Buffer,
                                     timeout=Timeout,
                                     retries=Retries,
                                     compress=Compress}) ->
    PartitionKey = partition_key(State),
    Buffer2 = case Compress of
        true    -> zlib:gzip(Buffer);
        false   -> Buffer
    end,
    spawn(fun() -> send_to_kinesis(StreamName, Buffer2, PartitionKey, Timeout, Retries, 0) end),
    increment_partition_num(State#kinetic_stream{buffer= <<"">>, buffer_size=0}).

increment_partition_num(State=#kinetic_stream{current_partition_num=Number,
                                              partitions_number=Number}) ->
    State#kinetic_stream{current_partition_num=0};
increment_partition_num(State=#kinetic_stream{current_partition_num=Number}) ->
    State#kinetic_stream{current_partition_num=Number+1}.

partition_key(#kinetic_stream{current_partition_num=Number, base_partition_name=BasePartitionName}) ->
    BinNumber = integer_to_binary(Number),
    <<BasePartitionName/binary, "-", BinNumber/binary>>.

send_to_kinesis(StreamName, Buffer, PartitionKey, Timeout, MaxRetries, MaxRetries) ->
    erlang:error(max_retries_reached, [StreamName, PartitionKey, Timeout, Buffer]);
send_to_kinesis(StreamName, Buffer, PartitionKey, Timeout, MaxRetries, Retry) ->
    case kinetic:put_record([{<<"Data">>, base64:encode(Buffer)},
                             {<<"PartitionKey">>, PartitionKey},
                             {<<"StreamName">>, StreamName}], Timeout) of
        {ok, _} ->
            ok;
        {error, Code, Headers, RawBody} ->
            Body = kinetic_utils:decode(RawBody),
            case proplists:get_value(<<"__type">>, Body) of
                <<"ProvisionedThroughputExceededException">> ->
                    timer:sleep(math:pow(2, Retry + 1) * 1000), % not really exponential
                    send_to_kinesis(StreamName, Buffer, PartitionKey, Timeout, MaxRetries, Retry + 1);

                Type ->
                    error({unhandled, Type, [StreamName, Code, Headers, RawBody]})
            end
    end.
