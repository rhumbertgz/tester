defmodule RiakTS.Query do
    @moduledoc """
    Query struct returned from a successfully prepared query. Its fields are:

      * `name` - The name of the prepared statement;
      * `statement` - The prepared statement;
      * `param_formats` - List of formats for each parameters encoded to;
      * `encoders` - List of anonymous functions to encode each parameter;
      * `columns` - The column names;
      * `result_formats` - List of formats for each column is decoded from;
      * `decoders` - List of anonymous functions to decode each column;
      * `types` - The type server table to fetch the type information from;
      * `null` - Atom to use as a stand in for postgres' `NULL`;
      * `copy_data` - Whether the query should send the final parameter as data to
      copy to the database;
    """

    @type t :: %__MODULE__{
      name:           iodata,
      statement:      iodata,
      param_formats:  [:binary | :text] | nil,
      encoders:       [RiakTS.Types.oid] | [(term -> iodata)] | nil,
      columns:        [String.t] | nil,
      result_formats: [:binary | :text] | nil,
      decoders:       [RiakTS.Types.oid] | [(binary -> term)] | nil,
      types:          RiakTS.TypeServer.table | nil,
      null:           atom,
      copy_data:      boolean}

    defstruct [:name, :statement, :param_formats, :encoders, :columns,
      :result_formats, :decoders, :types, :null, :copy_data]
  end

  defimpl DBConnection.Query, for: RiakTS.Query do
    import RiakTS.BinaryUtils
    require RiakTS.Messages

    def parse(%{name: name, statement: statement} = query, opts) do
      copy_data? = opts[:copy_data] || false
      # for query table to match on two identical statements they must be equal
      %{query | name: IO.iodata_to_binary(name),
        statement: IO.iodata_to_binary(statement), copy_data: copy_data?}
    end

    def describe(query, opts) do
      %RiakTS.Query{encoders: poids, decoders: roids,
                      types: types, null: conn_null, copy_data: data?} = query
      {pfs, encoders} = encoders(poids, types)
      encoders = if data?, do: encoders ++ [:copy_data], else: encoders
      {rfs, decoders} = decoders(roids, types)

      null = case Keyword.fetch(opts, :null) do
        {:ok, q_null} -> q_null
        :error -> conn_null
      end

      %RiakTS.Query{query | param_formats: pfs, encoders: encoders,
                              result_formats: rfs, decoders: decoders,
                              null: null}
    end

    def encode(%RiakTS.Query{types: nil} = query, _params, _) do
      raise ArgumentError, "query #{inspect query} has not been prepared"
    end

    def encode(query, params, _) do
      %RiakTS.Query{encoders: encoders, null: null, copy_data: data?} = query
      case do_encode(params || [], encoders, null, []) do
        :error when data? ->
          raise ArgumentError,
            "parameters must be of length #{length encoders}" <>
            " with copy data as final parameter for query #{inspect query}"
        :error ->
          raise ArgumentError,
            "parameters must be of length #{length encoders} for query #{inspect query}"
        params ->
         params
      end
    end

    def decode(%RiakTS.Query{decoders: nil}, res, opts) do
      case res do
        %RiakTS.Result{command: copy, rows: rows}
            when copy in [:copy, :copy_stream] and rows != nil ->
          %RiakTS.Result{res | rows: decode_copy(rows, opts)}
        _ ->
          res
      end
    end
    def decode(%RiakTS.Query{decoders: decoders, null: null}, res, opts) do
      mapper = opts[:decode_mapper] || fn x -> x end
      %RiakTS.Result{rows: rows} = res
      rows = do_decode(rows, decoders, null, mapper, [])
      %RiakTS.Result{res | rows: rows}
    end

    ## helpers

    defp encoders(oids, types) do
      oids
      |> Enum.map(&RiakTS.Types.encoder(&1, types))
      |> :lists.unzip()
    end

    defp decoders(nil, _) do
      {[], nil}
    end
    defp decoders(oids, types) do
      oids
      |> Enum.map(&RiakTS.Types.decoder(&1, types))
      |> :lists.unzip()
    end

    defp do_encode([copy_data | params], [:copy_data | encoders], null, encoded) do
      try do
        RiakTS.Messages.encode_msg(RiakTS.Messages.msg_copy_data(data: copy_data))
      else
        packet ->
          do_encode(params, encoders, null, [packet | encoded])
      rescue
        ArgumentError ->
          raise ArgumentError,
            "expected iodata to copy to database, got: " <> inspect(copy_data)
      end
    end
    defp do_encode([null | params], [_encoder | encoders], null, encoded) do
      do_encode(params, encoders, null, [<<-1::int32>> | encoded])
    end

    defp do_encode([param | params], [encoder | encoders], null, encoded) do
      param = encoder.(param)
      encoded = [[<<IO.iodata_length(param)::int32>> | param] | encoded]
      do_encode(params, encoders, null, encoded)
    end

    defp do_encode([], [], _, encoded), do: Enum.reverse(encoded)
    defp do_encode(params, _, _, _) when is_list(params), do: :error

    defp do_decode([row | rows], decoders, null, mapper, decoded) do
      decoded = [mapper.(decode_row(row, decoders, null, [])) | decoded]
      do_decode(rows, decoders, null, mapper, decoded)
    end
    defp do_decode([], _, _, _, decoded), do: decoded

    defp decode_row(<<-1 :: int32, rest :: binary>>, [_ | decoders], null, decoded) do
      decode_row(rest, decoders, null, [null | decoded])
    end
    defp decode_row(<<len :: uint32, value :: binary(len), rest :: binary>>, [decode | decoders], null, decoded) do
      decode_row(rest, decoders, null, [decode.(value) | decoded])
    end
    defp decode_row(<<>>, [], _, decoded), do: Enum.reverse(decoded)

    defp decode_copy(data, opts) do
      case opts[:decode_mapper] do
        nil    -> Enum.reverse(data)
        mapper -> decode_copy(data, mapper, [])
      end
    end

    defp decode_copy([row | data], mapper, decoded) do
      decode_copy(data, mapper, [mapper.(row) | decoded])
    end
    defp decode_copy([], _, decoded) do
      decoded
    end
  end

  defimpl String.Chars, for: RiakTS.Query do
    def to_string(%RiakTS.Query{statement: statement}) do
      IO.iodata_to_binary(statement)
    end
  end
