defmodule RiakTS.Stream do
  defstruct [:conn, :options, :params, :portal, :query, :ref, state: :bind, num_rows: 0, max_rows: 500]
end

defmodule RiakTS.CopyData do
  defstruct [:query, :params, :ref]
end

defimpl Enumerable, for: RiakTS.Stream do
  def reduce(stream, acc, fun) do
    Stream.resource(fn() -> start(stream) end, &next/1, &close/1).(acc, fun)
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def count(_) do
    {:error, __MODULE__}
  end

  defp start(stream) do
    %RiakTS.Stream{conn: conn, params: params, options: options} = stream
    stream = maybe_generate_portal(stream)
    _ = RiakTS.execute!(conn, stream, params, options)
    %RiakTS.Stream{stream | state: :out}
  end

  defp next(%RiakTS.Stream{state: :done} = stream) do
    {:halt, stream}
  end
  defp next(stream) do
    %RiakTS.Stream{conn: conn, params: params, options: options,
                     state: state, num_rows: num_rows} = stream
    case RiakTS.execute!(conn, stream, params, options) do
      %RiakTS.Result{command: :stream, rows: rows} = result
          when state in [:out, :suspended] ->
        stream =  %RiakTS.Stream{stream | state: :suspended,
                                            num_rows: num_rows + length(rows)}
        {[result], stream}
      %RiakTS.Result{command: :copy_stream} = result when state == :out ->
        {[result], %RiakTS.Stream{stream | state: :copy_out}}
      %RiakTS.Result{command: :copy_stream} = result when state == :copy_out ->
        {[result], stream}
      %RiakTS.Result{} = result ->
        {[result], %RiakTS.Stream{stream | state: :done}}
    end
  end

  defp close(%RiakTS.Stream{conn: conn, options: options} = stream) do
    DBConnection.close(conn, stream, options)
  end

  defp maybe_generate_portal(%RiakTS.Stream{portal: nil} = stream) do
    ref = make_ref()
    %RiakTS.Stream{stream | portal: inspect(ref), ref: ref}
  end
  defp maybe_generate_portal(stream) do
    %RiakTS.Stream{stream | ref: make_ref()}
  end
end


defimpl Collectable, for: RiakTS.Stream do

  def into(stream) do
    %RiakTS.Stream{conn: conn, params: params, options: options} = stream
    copy_stream = %RiakTS.Stream{stream | state: :copy_in, ref: make_ref()}
    _ = RiakTS.execute!(conn, copy_stream, params, options)
    {:ok, make_into(copy_stream, stream)}
  end

  defp make_into(copy_stream, stream) do
    %RiakTS.Stream{conn: conn, query: query, params: params, ref: ref,
                     options: options} = copy_stream
    copy = %RiakTS.CopyData{query: query, params: params, ref: ref}
    fn
      :ok, {:cont, data} ->
        _ = RiakTS.execute!(conn, copy, data, options)
        :ok
      :ok, :done ->
        done_stream = %RiakTS.Stream{copy_stream | state: :copy_done}
        RiakTS.execute!(conn, done_stream, params, options)
        stream
      :ok, :halt ->
        fail_stream = %RiakTS.Stream{copy_stream | state: :copy_fail}
        RiakTS.execute(conn, fail_stream, params, options)
    end
  end
end

defimpl DBConnection.Query, for: RiakTS.Stream do

  def parse(stream, _) do
    raise "can not prepare #{inspect stream}"
  end

  def describe(stream, _) do
    raise "can not describe #{inspect stream}"
  end

  def encode(%RiakTS.Stream{query: %RiakTS.Query{types: nil} = query}, _, _) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(%RiakTS.Stream{query: query, state: :bind}, params, opts) do
    DBConnection.Query.encode(query, params, opts)
  end

  def encode(%RiakTS.Stream{query: query, state: :copy_in}, params, opts) do
    case query do
      %RiakTS.Query{encoders: [_|_] = encoders, copy_data: true} ->
        {encoders, [:copy_data]} = Enum.split(encoders, -1)
        query = %RiakTS.Query{query | encoders: encoders}
        DBConnection.Query.encode(query, params, opts)
      %RiakTS.Query{} = query ->
        raise ArgumentError, "query #{inspect query} has not enabled copy data"
    end
  end

  def encode(%RiakTS.Stream{state: state}, params, _)
      when state in [:out, :suspended, :copy_out, :copy_done, :copy_fail] do
    params
  end

  def decode(%RiakTS.Stream{state: state}, result, _)
      when state in [:bind, :copy_in] do
    result
  end
  def decode(%RiakTS.Stream{query: query}, result, opts) do
    DBConnection.Query.decode(query, result, opts)
  end
end

defimpl DBConnection.Query, for: RiakTS.CopyData do
  require RiakTS.Messages

  def parse(copy_data, _) do
    raise "can not prepare #{inspect copy_data}"
  end

  def describe(copy_data, _) do
    raise "can not describe #{inspect copy_data}"
  end

  def encode(_, data, _) do
    try do
      RiakTS.Messages.encode_msg(RiakTS.Messages.msg_copy_data(data: data))
    rescue
      ArgumentError ->
        raise ArgumentError,
          "expected iodata to copy to database, got: " <> inspect(data)
    end
  end

  def decode(_, result, _) do
    result
  end
end

defimpl String.Chars, for: RiakTS.Stream do

  def to_string(%RiakTS.Stream{query: query}) do
    String.Chars.to_string(query)
  end
end

defimpl String.Chars, for: RiakTS.CopyData do
  def to_string(%RiakTS.CopyData{query: query}) do
    String.Chars.to_string(query)
  end
end
