% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(erlfdb_directory).


-export([
    root/0,
    root/1,

    create_or_open/3,
    create_or_open/4,
    create/3,
    create/4,
    open/3,
    open/4,

    list/2,
    list/3,

    exists/2,
    exists/3,

    move/4,
    move_to/3,

    remove/2,
    remove/3,
    remove_if_exists/2,
    remove_if_exists/3,

    get_id/1,
    get_name/1,
    get_root/1,
    get_node_prefix/1,
    get_path/1,
    get_layer/1,
    get_range/1,
    get_range/2,
    get_subspace/1,
    get_subspace/2,

    contains/2,

    pack/2,
    pack_vs/2,
    unpack/2
]).


-include("erlfdb.hrl").


-define(LAYER_VERSION, {1, 0, 0}).
-define(DEFAULT_NODE_PREFIX, <<16#FE>>).
-define(SUBDIRS, 0).


% directory name = human readable string
% node_name = shortened binary
% root version = node_prefix + {"version"}
% root node = node_prefix + {node_prefix}
% partition id = node_name + 16#FE
% partition version = node_name + {"version"}
% partition node = partition_id + {partition_id}
% node_id = node_prefix + {node_name}
% node_layer_id = node_prefix + {node_name, "layer"}
% node_entry_id = node_prefix + {node_name, SUBDIRS, directory name}


root() ->
    init_root([]).


root(Options) ->
    init_root(Options).


create_or_open(Tx, Node, Path) ->
    create_or_open(Tx, Node, Path, <<>>).


create_or_open(Tx, Node, Path, undefined) ->
    create_or_open(Tx, Node, Path, <<>>);

create_or_open(Tx, Node, PathIn, Layer) ->
    Path = path_init(PathIn),
    {ParentPath, [PathName]} = list:split(Path, length(Path) - 1),

    Parent = lists:foldl(fun(Name, CurrNode) ->
        case open_int(Tx, CurrNode, Name, <<>>) of
            not_found ->
                create_int(Tx, CurrNode, Name, <<>>, undefined);
            NextNode ->
                NextNode
        end
    end, Node, ParentPath),

    case open_int(Tx, Parent, PathName, Layer) of
        not_found ->
            create_int(Tx, Parent, PathName, Layer, undefined);
        ExistingNode ->
            ExistingNode
    end.


create(Tx, Node, Path) ->
    create(Tx, Node, Path, []).


create(Tx, Node, Path, Options) ->
    Layer = erlfdb_util:get(Options, layer, <<>>),
    NodeName = erlfdb_util:get(Options, node_name, undefined),
    create_int(Tx, Node, Path, Layer, NodeName).


open(Tx, Node, Path) ->
    open(Tx, Node, Path, []).


open(Tx, Node, Path, Options) ->
    Layer = erlfdb_util:get(Options, layer, <<>>),
    open_int(Tx, Node, Path, Layer).


list(Tx, Node) ->
    list(Tx, Node, {}).


list(Tx, Node, PathIn) ->
    Path = path_init(PathIn),
    case find(Tx, Node, Path) of
        not_found ->
            ?ERLFDB_ERROR({list_error, missing_path, Path});
        ListNode ->
            Subdirs = ?ERLFDB_EXTEND(get_id(ListNode), ?SUBDIRS),
            {SDStart, SDEnd} = ?ERLFDB_RANGE(Subdirs),
            SubDirKVs = erlfdb:wait(erlfdb:get_range(Tx, SDStart, SDEnd)),
            lists:map(fun({Key, NodeName}) ->
                {DName} = ?ERLFDB_EXTRACT(Subdirs, Key),
                ChildNode = init_node(Tx, ListNode, NodeName, DName),
                {DName, ChildNode}
            end, SubDirKVs)
    end.


exists(Tx, Node) ->
    exists(Tx, Node, {}).


exists(Tx, Node, PathIn) ->
    Path = path_init(PathIn),
    case find(Tx, Node, Path) of
        not_found -> false;
        _ChildNode -> true
    end.


move(Tx, Node, OldPathIn, NewPathIn) ->
    OldPath = path_init(OldPathIn),
    NewPath = path_init(NewPathIn),
    check_not_subpath(OldPath, NewPath),

    OldNode = find(Tx, Node, OldPath),
    NewNode = find(Tx, Node, NewPath),

    if OldNode /= not_found -> ok; true ->
        ?ERLFDB_ERROR({move_error, missing_source, OldPath})
    end,

    if NewNode == not_found -> ok; true ->
        ?ERLFDB_ERROR({move_error, target_exists, NewPath})
    end,

    {NewParentPath, [NewName]} = list:split(NewPath, length(NewPath) - 1),
    case find(Tx, Node, NewParentPath) of
        not_found ->
            ?ERLFDB_ERROR({move_error, missing_parent_node, NewParentPath});
        NewParentNode ->
            check_same_partition(OldNode, NewParentNode),

            ParentId = get_id(NewParentNode),
            NodeEntryId = ?ERLFDB_PACK(ParentId, {?SUBDIRS, NewName}),
            erlfdb:set(Tx, NodeEntryId, get_name(OldNode)),
            remove_from_parent(Tx, OldNode),
            OldNode#{path := NewPath}
    end.


move_to(Tx, Node, NewPath) ->
    move(Tx, Node, get_path(Node), NewPath).


remove(Tx, Node) ->
    remove_int(Tx, Node, {}, false).


remove(Tx, Node, Path) ->
    remove_int(Tx, Node, Path, false).


remove_if_exists(Tx, Node) ->
    remove_int(Tx, Node, {}, true).


remove_if_exists(Tx, Node, Path) ->
    remove_int(Tx, Node, Path, true).


get_id(Node) ->
    invoke(Node, get_id, []).


get_name(Node) ->
    invoke(Node, get_name, []).


get_root(Node) ->
    invoke(Node, get_root, []).


get_node_prefix(Node) ->
    invoke(Node, get_node_prefix, []).


get_path(Node) ->
    invoke(Node, get_path, []).


get_layer(Node) ->
    invoke(Node, get_layer, []).


get_range(Node) ->
    invoke(Node, get_range, [{}]).


get_range(Node, PathIn) ->
    Path = list_to_tuple(path_init(PathIn)),
    invoke(Node, get_range, [Path]).


get_subspace(Node) ->
    invoke(Node, get_subspace, [{}]).


get_subspace(Node, PathIn) ->
    Path = list_to_tuple(path_init(PathIn)),
    invoke(Node, get_subspace, [Path]).


pack(Node, KeyTuple) ->
    invoke(Node, pack, [KeyTuple]).


pack_vs(Node, KeyTuple) ->
    invoke(Node, pack_vs, [KeyTuple]).


unpack(Node, Key) ->
    invoke(Node, unpack, [Key]).


contains(Node, Key) ->
    invoke(Node, contains, [Key]).


invoke(Node, FunName, Args) ->
    case Node of
        #{FunName := Fun} ->
            erlang:apply(Fun, [Node | Args]);
        #{} ->
            ?ERLFDB_ERROR({op_not_supported, FunName, Node})
    end.


init_root(Options) ->
    DefNodePref = ?DEFAULT_NODE_PREFIX,
    NodePrefix = erlfdb_util:get_value(Options, node_prefix, DefNodePref),
    ContentPrefix = erlfdb_util:get_value(Options, content_prefix, <<>>),
    AllowManual = erlfdb_util:get_value(Options, allow_manual_names),
    Allocator = erlfdb_hca:create(?ERLFDB_EXTEND(NodePrefix, <<"hca">>)),
    #{
        id => ?ERLFDB_EXTEND(NodePrefix, NodePrefix),
        node_prefix => NodePrefix,
        content_prefix => ContentPrefix,
        allocator => Allocator,
        allow_manual_names => AllowManual,
        is_absolute_root => true,

        get_id => fun(Self) -> maps:get(id, Self) end,
        get_root => fun(Self) -> Self end,
        get_node_prefix => fun(Self) -> maps:get(node_prefix, Self) end,
        get_path => fun(_Self) -> [] end
    }.


init_node(Tx, Node, NodeName, PathName) ->
    NodePrefix = get_node_prefix(Node),
    NodeLayerId = ?ERLFDB_PACK(NodePrefix, {NodeName, <<"layer">>}),
    Layer = case erlfdb:wait(erlfdb:get(Tx, NodeLayerId)) of
        not_found ->
            ?ERLFDB_ERROR({internal_error, missing_node_layer, NodeLayerId});
        LName ->
            LName
    end,
    case Layer of
        <<"partition">> ->
            NewNode = init_partition(Node, NodeName, PathName),
            check_version(Tx, NewNode, read);
        _ ->
            init_directory(Node, NodeName, PathName, Layer)
    end.


init_partition(ParentNode, NodeName, PathName) ->
    NodeNameLen = size(NodeName),
    NodePrefix = <<NodeName:NodeNameLen/binary, 16#FE>>,
    Allocator = erlfdb_hca:create(?ERLFDB_EXTEND(NodePrefix, <<"hca">>)),
    #{
        id => ?ERLFDB_EXTEND(NodePrefix, NodePrefix),
        name => NodePrefix,
        partition_name => NodeName,
        root => get_root(ParentNode),
        node_prefix => NodePrefix,
        content_prefix => NodeName,
        allocator => Allocator,
        allow_manual_names => false,
        path => path_append(get_path(ParentNode), PathName),

        get_id => fun(Self) -> maps:get(id, Self) end,
        get_name => fun(Self) -> maps:get(name, Self) end,
        get_root => fun(Self) -> Self end,
        get_node_prefix => fun(Self) -> maps:get(node_prefix, Self) end,
        get_path => fun(Self) -> maps:get(path, Self) end
    }.


init_directory(ParentNode, NodeName, PathName, Layer) ->
    NodePrefix = get_node_prefix(ParentNode),
    ParentPath = get_path(ParentNode),
    #{
        id => ?ERLFDB_EXTEND(NodePrefix, NodeName),
        name => NodeName,
        root => get_root(ParentNode),
        path => erlfdb_dirpath:append(ParentPath, PathName),
        layer => Layer,

        get_id => fun(Self) -> maps:get(id, Self) end,
        get_name => fun(Self) -> maps:get(name, Self) end,
        get_root => fun(Self) -> maps:get(root, Self) end,
        get_node_prefix => fun(Self) ->
            Root = maps:get(root, Self),
            get_node_prefix(Root)
        end,
        get_path => fun(Self) -> maps:get(path, Self) end,
        get_layer => fun(Self) -> maps:get(layer, Self) end,
        get_range => fun(Self, Tuple) ->
            Name = maps:get(name, Self),
            NameLen = size(Name),
            {Start, End} = erlfdb_tuple:pack(Tuple),
            {
                <<Name:NameLen/binary, Start/binary>>,
                <<Name:NameLen/binary, End/binary>>
            }
        end,
        get_subspace => fun(Self, Tuple) ->
            Name = maps:get(name, Self),
            erlfdb_subspace:create(Tuple, Name)
        end,
        pack => fun(Self, Tuple) ->
            erlfdb_tuple:pack(Tuple, maps:get(name, Self))
        end,
        pack_vs => fun(Self, Tuple) ->
            erlfdb_tuple:pack_vs(Tuple, maps:get(name, Self))
        end,
        unpack => fun(Self, Key) ->
            Subspace = get_subspace(Self),
            erlfdb_subspace:unpack(Subspace, Key)
        end,
        contains => fun(Self, Key) ->
            Subspace = get_subspace(Self),
            erlfdb_subspace:contains(Subspace, Key)
        end
    }.


find(_Tx, Node, []) ->
    Node;

find(Tx, Node, [PathName | RestPath]) ->
    NodeEntryId = ?ERLFDB_PACK(get_id(Node), {?SUBDIRS, PathName}),
    case erlfdb:wait(erlfdb:get(Tx, NodeEntryId)) of
        not_found ->
            not_found;
        ChildNodeName ->
            ChildNode = init_node(Tx, Node, ChildNodeName, PathName),
            find(Tx, ChildNode, RestPath)
    end.


create_int(Tx, Node, PathIn, Layer, NodeNameIn) ->
    Path = path_init(PathIn),
    case open_int(Tx, Node, Path, Layer) of
        not_found ->
            {ParentPath, PathName} = lists:split(Path, length(Path) - 1),
            case find(Tx, Node, ParentPath) of
                not_found ->
                    ?ERLFDB_ERROR({create_error, missing_parent, ParentPath});
                Parent ->
                    check_version(Tx, Parent, write),
                    NodeName = create_node_name(Tx, Parent, NodeNameIn),
                    create_node(Tx, Parent, PathName, NodeName, Layer)
            end;
        _Exists ->
            ?ERLFDB_ERROR({create_error, path_exists, Path})
    end.


create_node(Tx, Parent, PathName, LayerIn, NodeName) ->
    NodeEntryId = ?ERLFDB_PACK(get_id(Parent), {?SUBDIRS, PathName}),
    erlfdb:set(Tx, NodeEntryId, NodeName),

    NodePrefix = get_node_prefix(Parent),
    NodeLayerId = ?ERLFDB_PACK(NodePrefix, {NodeName, <<"layer">>}),
    Layer = if LayerIn == undefined -> <<>>; true -> LayerIn end,
    erlfdb:set(Tx, NodeLayerId, Layer).


open_int(Tx, Node, PathIn, Layer) ->
    Path = path_init(PathIn),
    case find(Tx, Node, Path) of
        not_found ->
            ?ERLFDB_ERROR({open_error, path_missing, Path});
        Opened ->
            NodeLayer = get_layer(Opened),
            if Layer == <<>> orelse Layer == NodeLayer -> ok; true ->
                ?ERLFDB_ERROR({open_error, layer_mismatch, NodeLayer, Layer})
            end,
            Opened
    end.


remove_int(Tx, Node, PathIn, IgnoreMissing) ->
    Path = path_init(PathIn),
    case find(Tx, Node, Path) of
        not_found when IgnoreMissing ->
            ok;
        not_found ->
            ?ERLFDB_ERROR({remove_error, path_not_found, Path});
        #{is_absolute_root := true} ->
            ?ERLFDB_ERROR({remove_error, cannot_remove_root});
        ToRem ->
            remove_recursive(Tx, ToRem),
            remove_from_parent(Tx, ToRem)
    end.


remove_recursive(Tx, Node) ->
    % Remove all subdirectories
    lists:foreach(fun({_DirName, ChildNode}) ->
        remove_recursive(Tx, ChildNode)
    end, list(Tx, Node)),

    Name = get_name(Node),
    NameLen = size(Name),

    % Delete all content for the node. We can't reuse
    % get_range here due to partitions being an edge
    % case.
    ContentStart = <<Name:NameLen/binary, 16#00>>,
    ContentEnd = <<Name:NameLen/binary, 16#FF>>,
    erlfdb:clear_range(Tx, ContentStart, ContentEnd),

    % Delete this node from the tree hierarchy
    Prefix = get_node_prefix(get_root(Node)),
    PrefixLen = size(Prefix),
    NodeStart = <<Prefix:PrefixLen/binary, Name/binary>>,
    NodeEnd = <<Prefix:PrefixLen/binary, Name/binary>>,
    erlfdb:clear_range(Tx, NodeStart, NodeEnd).


remove_from_parent(Tx, Node) ->
    Root = get_root(Node),
    Path = get_path(Node),
    {ParentPath, PathName} = lists:split(Path, length(Path) - 1),
    Parent = find(Tx, Root, ParentPath),

    NodeEntryId = ?ERLFDB_PACK(get_id(Parent), {?SUBDIRS, PathName}),
    erlfdb:clear(Tx, NodeEntryId).


create_node_name(Tx, Parent, NameIn) ->
    #{
        content_prefix := ContentPrefix,
        allow_manual := AllowManual,
        allocator := Allocator
    } = get_root(Parent),
    case NameIn of
        undefined ->
            BaseId = erlfdb_hca:allocate(Allocator, Tx),
            CPLen = size(ContentPrefix),
            NewName = <<ContentPrefix:CPLen/binary, BaseId/binary>>,

            KeysExist = erlfdb:get_range_startswith(Tx, NewName, [{limit, 1}]),
            if KeysExist == [] -> ok; true ->
                ?ERLFDB_ERROR({
                        create_error,
                        keys_exist_for_allocated_name,
                        NewName
                    })
            end,

            IsFree = is_prefix_free(Tx, NewName, true),
            if IsFree -> ok; true ->
                ?ERLFDB_ERROR({
                        create_error,
                        manual_names_conflict_with_allocated_name,
                        NewName
                    })
            end;
        _ when AllowManual andalso is_binary(NameIn) ->
            case is_prefix_free(Tx, NameIn, false) of
                true ->
                    ok;
                false ->
                    ?ERLFDB_ERROR({create_error, node_name_in_use, NameIn})
            end,
            NameIn;
        _ ->
            ?ERLFDB_ERROR({create_error, manual_node_names_prohibited})
    end.


is_prefix_free(Tx, NodeName, Snapshot) ->
    % We have to make sure that NodeName does not interact with
    % anything that currently exists in the tree. This means that
    % it must not be a prefix of any existing node id and also
    % that no existing node id is a prefix of this NodeName.
    %
    % A motivating example for why is that deletion of nodes
    % in the tree would end up deleting unrelated portions
    % of the tree when node ids overlapped. There would also
    % be other badness if keys overlapped with the layer
    % or ?SUBDIRS spaces.

    try
        % An empty name would obviously be kind of bonkers.
        if NodeName /= <<>> -> ok; true ->
            throw(false)
        end,

        Root = get_root(NodeName),
        RootId = get_id(Root),
        NodePrefix = get_node_prefix(Root),
        NPLen = size(NodePrefix),

        % First check that the special case of the root node
        case bin_startswith(NodeName, RootId) of
            true -> throw(false);
            false -> ok
        end,

        % Check if NodeName is a prefix of any existing key
        Start1 = ?ERLFDB_EXTEND(NodePrefix, NodeName),
        End1 = ?ERLFDB_EXTEND(NodePrefix, erlfdb_key:strinc(NodeName)),
        Opts1 = [{snapshot, Snapshot}, {limit, 1}, {streaming_mode, exact}],
        case erlfdb:wait(erlfdb:get_range(Tx, Start1, End1, Opts1)) of
            [_ | _] -> throw(false);
            [] -> ok
        end,

        % Check if any node id is a prefix of NodeName
        Start2 = <<NodePrefix:NPLen/binary, 16#00>>,
        End2 = ?ERLFDB_PACK(NodePrefix, {NodeName, null}),
        Opts2 = [
                {snapshot, Snapshot},
                {reverse, true},
                {limit, 1},
                {streaming_mode, exact}
            ],
        erlfdb:fold_range(Tx, Start2, End2, fun({Key, _}, _) ->
            case bin_startswith(End2, Key) of
                true -> throw(false);
                false -> ok
            end
        end, nil, Opts2),

        true
    catch throw:false ->
        false
    end.


bin_startswith(Subject, Prefix) ->
    PrefixLen = size(Prefix),
    case Subject of
        <<Prefix:PrefixLen/binary, _/binary>> -> true;
        _ -> false
    end.


check_version(Tx, Node, PermLevel) ->
    VsnKey = ?ERLFDB_EXTEND(get_id(Node), <<"version">>),
    {LV1, LV2, _LV3} = ?LAYER_VERSION,
    {Major, Minor, Patch} = case erlfdb:wait(erlfdb:get(Tx, VsnKey)) of
        not_found ->
            initialize_directory(Tx, VsnKey);
        VsnBin ->
            <<
                V1:32/little-unsigned,
                V2:32/little-unsigned,
                V3:32/little-unsigned
            >> = VsnBin,
            {V1, V2, V3}
    end,

    Path = get_path(Node),

    if Major =< LV1 -> ok; true ->
        ?ERLFDB_ERROR({version_error, unreadable, Path, {Major, Minor, Patch}})
    end,

    if not (Minor > LV2 andalso PermLevel /= read) -> ok; true ->
        ?ERLFDB_ERROR({version_error, unwritable, Path, {Major, Minor, Patch}})
    end.


initialize_directory(Tx, VsnKey) ->
    {V1, V2, V3} = ?LAYER_VERSION,
    Packed = <<
            V1:32/little-unsigned,
            V2:32/little-unsigned,
            V3:32/little-unsigned
        >>,
    erlfdb:set(Tx, VsnKey, Packed).


check_same_partition(OldNode, NewParentNode) ->
    OldRoot = get_root(OldNode),
    NewRoot = get_root(NewParentNode),
    if NewRoot == OldRoot -> ok; true ->
        ?ERLFDB_ERROR({move_error, partition_mismatch, OldRoot, NewRoot})
    end.


path_init(Path) when is_tuple(Path) ->
    lists:flatmap(fun(Part) ->
        path_init(Part)
    end, tuple_to_list(Path));

path_init(<<_/utf8>> = Path) ->
    [Path];

path_init({utf8, <<_/utf8>>} = Path) ->
    [Path];

path_init(<<_/binary>> = Part) ->
    ?ERLFDB_ERROR({path_error, invalid_utf8, Part});

path_init({utf8, <<_/binary>>} = Part) ->
    ?ERLFDB_ERROR({path_error, invalid_utf8, Part});

path_init(Else) ->
    ?ERLFDB_ERROR({path_error, invalid_path_component, Else}).


path_append(Path, Part) ->
    Path ++ path_init(Part).


check_not_subpath(OldPath, NewPath) ->
    case lists:prefix(OldPath, NewPath) of
        true ->
            ?ERLFDB_ERROR({move_error, targe_is_directory, OldPath, NewPath});
        false ->
            ok
    end.
