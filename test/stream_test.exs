defmodule StreamTest do
  use ExUnit.Case, async: true
  import RiakTS.TestHelper
  alias RiakTS.Result

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
             prepare: context[:prepare] || :named]
    {:ok, pid} = RiakTS.start_link(options)
    {:ok, [pid: pid, options: options]}
  end

  test "MAY take part of stream", context do
    query = prepare("", "SELECT * FROM generate_series(1, 3)")
    transaction(fn(conn) ->
      assert [[[1]]] = stream(query, [], max_rows: 1)
        |> Stream.map(fn(%Result{rows: rows}) -> rows end)
        |> Enum.take(1)
    end)
  end

  test "streams query in chunks", context do
    query = prepare("", "SELECT * FROM generate_series(1, 3)")
    transaction(fn(conn) ->
      assert [[[1], [2]], [[3]]] = stream(query, [], max_rows: 2)
        |> Stream.map(fn(%Result{rows: rows}) -> rows end)
        |> Enum.to_list()
    end)
  end

  test "results contain num rows and no final chunk with empty rows", context do
    query = prepare("", "SELECT * FROM generate_series(1, 2)")
    transaction(fn(conn) ->
      assert [%{command: :stream, rows: [[1]], num_rows: :stream},
              %{command: :stream, rows: [[2]], num_rows: :stream},
              %{command: :select, rows: [], num_rows: 2}] =
        stream(query, [], max_rows: 1)
        |> Enum.to_list()

      assert [%{command: :stream, rows: [[1], [2]], num_rows: :stream},
              %{command: :select, rows: [], num_rows: 2}] =
        stream(query, [], max_rows: 2)
        |> Enum.to_list()

      assert [%{command: :select, rows: [[1], [2]], num_rows: 2}] =
        stream(query, [], max_rows: 3)
        |> Enum.to_list()
    end)
  end

  test "rebind named portal fails", context do
    query = prepare("", "SELECT 42")
    transaction(fn(conn) ->
      stream = stream(query, [])
      stream = %RiakTS.Stream{stream | portal: "E2MANY"}

      _ = for _ <- stream do
        assert_raise RiakTS.Error, ~r"ERROR \(duplicate_cursor\)",
          fn() -> Enum.take(stream, 1) end
      end
    end)
  end

  test "stream closes named portal ", context do
    query = prepare("", "SELECT 42")

    transaction(fn(conn) ->
      stream = stream(query, [])
      stream = %RiakTS.Stream{stream | portal: "CLOSES"}

      assert [%Result{rows: [[42]]}] = stream |> Enum.take(1)
      assert [%Result{rows: [[42]]}] = stream |> Enum.take(1)
    end)
  end

  test "prepare, stream and close", context do
    query = prepare("S42", "SELECT 42")
    transaction(fn(conn) ->
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
      assert RiakTS.close(conn, query) == :ok
    end)
  end

  test "prepare query and stream different query with same name raises", context do
    query = prepare("ENOENT", "SELECT 42")
    :ok = close(query)
    _ = prepare("ENOENT", "SELECT 41")
    transaction(fn(conn) ->
      assert_raise RiakTS.Error, ~r"ERROR \(duplicate_prepared_statement\)",
        fn -> stream(query, []) |> Enum.take(1) end
    end)
  end

  test "prepare, close and stream", context do
    query = prepare("S42", "SELECT 42")
    :ok = close(query)
    transaction(fn(conn) ->
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
    end)
  end

  @tag prepare: :unnamed
  test "stream named is unnamed when named not allowed", context do
    assert (%RiakTS.Query{name: ""} = query) = prepare("42", "SELECT 42")
    transaction(fn(conn) ->
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
      assert :ok = RiakTS.close(conn, query)
      assert %Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
    end)
  end

  test "stream query prepared query on another connection", context do
    query = prepare("S42", "SELECT 42")

    {:ok, pid2} = RiakTS.start_link(context[:options])
    RiakTS.transaction(pid2, fn(conn) ->
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
      assert {:ok, %Result{rows: [[41]]}} = RiakTS.query(conn, "SELECT 41", [])
    end)
  end

  test "raise when executing prepared query on connection with different types", context do
    query = prepare("S42", "SELECT 42")

    {:ok, pid2} = RiakTS.start_link([decode_binary: :reference] ++ context[:options])

    RiakTS.transaction(pid2, fn(conn) ->
      assert_raise ArgumentError, ~r"invalid types for the connection",
        fn() -> stream(query, []) |> Enum.take(1) end
    end)
  end

  test "connection works after failure in binding state", context do
    query = prepare("", "insert into uniques values (CAST($1::text AS int))")

    transaction(fn(conn) ->
      assert_raise RiakTS.Error, ~r"ERROR \(invalid_text_representation\)",
        fn -> stream(query, ["EBADF"]) |> Enum.take(1) end
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure in executing state", context do
    query = prepare("", "insert into uniques values (1), (1)")

    transaction(fn(conn) ->
      assert_raise RiakTS.Error, ~r"ERROR \(unique_violation\)",
        fn -> stream(query, []) |> Enum.take(1) end
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "connection reuses prepared query after query", context do
    query = prepare("", "SELECT 41")
    transaction(fn(conn) ->
      assert %Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
      assert [%Result{rows: [[41]]}] = stream(query, []) |> Enum.take(1)
    end)
  end

  test "connection forces prepare on stream after prepare of same name", context do
    query41 = prepare("", "SELECT 41")
    query42 = prepare("", "SELECT 42")
    transaction(fn(conn) ->
      assert %Result{rows: [[42]]} = RiakTS.execute!(conn, query42, [])
      assert [%Result{rows: [[41]]}] = stream(query41, []) |> Enum.take(1)
    end)
  end

  test "raise when trying to stream unprepared query", context do
    query = %RiakTS.Query{name: "ENOENT", statement: "SELECT 42"}

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r/has not been prepared/,
        fn -> stream(query, []) |> Enum.take(1) end
    end)
  end

  test "raise when trying to stream reserved query", context do
    query = prepare("", "BEGIN")

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r/uses reserved name/,
        fn -> stream(%{query | name: "POSTGREX_COMMIT"}, []) |> Enum.take(1) end
    end)
  end

  test "stream struct interpolates to statement", context do
    query = prepare("", "BEGIN")
    transaction(fn(conn) ->
      assert "#{stream(query, [])}" == "BEGIN"
    end)
  end

  test "connection_id", context do
    query = prepare("", "SELECT pg_backend_pid()")
    {:ok, connection_id} = transaction(fn(conn) ->
      assert [%Result{connection_id: connection_id, rows: [[backend_pid]]}] =
        stream(query, []) |> Enum.take(1)
      assert is_integer(connection_id)
      assert connection_id == backend_pid
      connection_id
    end)

    query = prepare("", "insert into uniques values (1), (1)")

    try do
      transaction(fn(conn) -> stream(query, []) |> Enum.take(1) end)
    rescue
      err ->
        assert %RiakTS.Error{connection_id: ^connection_id} = err
    end
  end

  defp range(conn, name, x, y) do
    {:ok, q} = RiakTS.prepare(conn, name, "SELECT * FROM generate_series(CAST($1 as int), $2)")
    stream(q, [x, y], max_rows: 1)
    |> Stream.map(fn (res) -> :lists.flatten(res.rows) end)
  end

  # nest two ranges in a transaction, first query has name1, second has name2
  #
  defp range_x_range(conn, name1, name2, x, y) do
    map =
      fn
        [x] -> range(conn, name2, 1, x) |> Enum.flat_map(&(&1))
        []  -> []
      end
    conn
    |> range(name1, x, y)
    |> Stream.map(map)
    |> Enum.to_list
  end

  test "streams can be nested using named queries", context do
    transaction(fn(conn) ->
      assert [[1], [1, 2], []] = range_x_range(conn, "S1", "S2", 1, 2)
    end)
  end

  test "streams can be nested using unnamed queries", context do
    transaction(fn(conn) ->
      assert [[1], [1, 2], []] = range_x_range(conn, "", "", 1, 2)
    end)
  end

  @tag prepare: :unnamed
  test "streams can be nested using named queries when names not allowed", context do
    transaction(fn(conn) ->
      assert [[1], [1, 2], []] = range_x_range(conn, "S1", "S2", 1, 2)
    end)
  end

  defp range_x_cast(pid, name1, name2) do
    q1 = RiakTS.prepare!(pid, name1, "SELECT * FROM generate_series(1, 2)")
    q2 = RiakTS.prepare!(pid, name2, "SELECT CAST($1 as int)")

    RiakTS.transaction(pid, fn(conn) ->
      map =
        fn
          %{rows: [[x]]} ->
          stream(q2, [x], max_rows: 1) |> Enum.flat_map(fn res -> res.rows end)
          %{rows: []} ->
            []
        end
      stream(q1, [], max_rows: 1)
      |> Stream.map(map)
      |> Enum.to_list
      |> :lists.flatten
    end)
  end

  test "transaction with nested named stream", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid, "S1", "S2")
  end

  test "transaction with nested unnamed stream", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid, "", "")
  end

  @tag prepare: :unnamed
  test "transaction with nested named stream when names not allowed", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid, "S1", "S2")
  end

  test "COPY empty TO STDOUT", context do
    query = prepare("", "COPY uniques TO STDOUT")
    transaction(fn(conn) ->
      assert [%RiakTS.Result{command: :copy, rows: [], num_rows: 0}] =
        stream(query, []) |> Enum.to_list()
    end)
  end

  test "COPY TO STDOUT", context do
    query1 = prepare("", "COPY (VALUES (1, 2)) TO STDOUT")
    query2 = prepare("", "COPY (VALUES (1, 2), (3, 4)) TO STDOUT")
    transaction(fn(conn) ->
      assert [%RiakTS.Result{rows: ["1\t2\n"], num_rows: 1}] =
        stream(query1, []) |> Enum.to_list()

      assert [%RiakTS.Result{rows: ["1\t2\n", "3\t4\n"], num_rows: 2}] =
        stream(query2, [], [max_rows: 0]) |> Enum.to_list()
    end)
  end

  test "COPY TO STDOUT with decoder_mapper", context do
    query2 = prepare("", "COPY (VALUES (1, 2), (3, 4)) TO STDOUT")
    transaction(fn(conn) ->
      assert [%RiakTS.Result{rows: [["1","2"], ["3","4"]]}] =
        stream(query2, [], [decode_mapper: &String.split/1]) |> Enum.to_list()
    end)
  end

  test "COPY TO STDOUT with max_rows splitting", context do
    query1 = prepare("", "COPY (VALUES (1, 2)) TO STDOUT")
    query2 = prepare("", "COPY (VALUES (1, 2), (3, 4)) TO STDOUT")
    transaction(fn(conn) ->
      assert [%{command: :copy_stream, rows: ["1\t2\n"], num_rows: :copy_stream},
              %{command: :copy, rows: [], num_rows: 1}] =
        stream(query1, [], [max_rows: 1]) |> Enum.to_list()

      assert [%{command: :copy_stream, rows: ["1\t2\n"], num_rows: :copy_stream},
              %{command: :copy_stream, rows: ["3\t4\n"], num_rows: :copy_stream},
              %{command: :copy, rows: [], num_rows: 2}] =
        stream(query2, [], [max_rows: 1]) |> Enum.to_list()

      assert [%{command: :copy_stream, rows: ["1\t2\n", "3\t4\n"], num_rows: :copy_stream},
              %{command: :copy, rows: [], num_rows: 2}] =
        stream(query2, [], [max_rows: 2]) |> Enum.to_list()

      assert [%{command: :copy, rows: ["1\t2\n", "3\t4\n"], num_rows: 2}] =
        stream(query2, [], [max_rows: 3]) |> Enum.to_list()
    end)
  end

  test "COPY TO STDOUT with stream halting before copy done", context do
    Process.flag(:trap_exit, true)
    query = prepare("", "COPY (VALUES (1, 2), (3, 4)) TO STDOUT")
    capture_log fn ->
      transaction(fn(conn) ->
        assert [%{command: :copy_stream, rows: ["1\t2\n"], num_rows: :copy_stream}] =
          stream(query, [], [max_rows: 1]) |> Enum.take(1)
      end)
      pid = context[:pid]
      assert_receive {:EXIT, ^pid, {:shutdown, %RuntimeError{}}}
    end
  end

  test "COPY TO STDOUT locks connection", context do
    query = prepare("", "COPY (VALUES (1, 2), (3, 4)) TO STDOUT")
    transaction(fn(conn) ->
      map =
        fn
          %{command: :copy_stream}= result ->
            assert_raise RuntimeError, ~r"connection is locked",
              fn() -> RiakTS.prepare(conn, "", "BEGIN") end
            assert_raise RuntimeError, ~r"connection is locked",
              fn() -> RiakTS.execute(conn, query, []) end
            assert_raise RuntimeError, ~r"connection is locked",
              fn() -> RiakTS.close(conn, query, []) end
            result
          %{command: :copy} = result ->
            result
        end

      assert [%{command: :copy_stream, rows: ["1\t2\n"], num_rows: :copy_stream},
              %{command: :copy_stream, rows: ["3\t4\n"], num_rows: :copy_stream},
              %{command: :copy, rows: [], num_rows: 2}] =
        stream(query, [], [max_rows: 1])
        |> Stream.map(map)
        |> Enum.to_list()
    end)
  end

  test "COPY empty FROM STDIN", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert Enum.into([], stream) == stream
      assert %RiakTS.Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "COPY FROM STDIN", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert Enum.into(["2\n", "3\n4\n"], stream) == stream

      stream = stream(query, [], [log: &send(self(), &1)])
      assert Enum.into(["5\n"], stream) == stream

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query
      assert {:ok, %{command: :copy, rows: nil, num_rows: 1}} = entry.result

      assert %RiakTS.Result{rows: [[2], [3], [4], [5]]} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "COPY FROM STDIN failure", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      _ = RiakTS.query!(conn, "SAVEPOINT bad_copy", [])

      stream = stream(query, [])
      map =
        fn
          "3\n" -> raise "hello"
          other -> other
        end
      assert_raise RuntimeError, "hello",
        fn() -> Enum.into(Stream.map(["2\n", "3\n"], map), stream) end
      assert_raise RiakTS.Error, ~r"\(in_failed_sql_transaction\)",
        fn() -> RiakTS.query!(conn, "SELECT * FROM uniques", []) end

      _ = RiakTS.query!(conn, "ROLLBACK TO SAVEPOINT bad_copy", [])

      assert %RiakTS.Result{rows: []} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
    end)
  end

  test "COPY FROM STDIN with savepoint", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [], [mode: :savepoint])
      assert Enum.into(["2\n", "3\n4\n"], stream) == stream
      assert Enum.into(["5\n"], stream) == stream
      assert %RiakTS.Result{rows: [[2], [3], [4], [5]]} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "COPY FROM STDIN failure with savepoint", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [], [mode: :savepoint])
      map =
        fn
          "3\n" -> raise "hello"
          other -> other
        end
      assert_raise RuntimeError, "hello",
        fn() -> Enum.into(Stream.map(["2\n", "3\n"], map), stream) end
      assert %RiakTS.Result{rows: []} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "prepare query and stream into different query with same name raises", context do
    query = prepare("ENOENT", "COPY uniques FROM STDIN", [copy_data: true])
    :ok = close(query)
    _ = prepare("ENOENT", "SELECT 41")
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert_raise RiakTS.Error, ~r"ERROR \(duplicate_prepared_statement\)",
        fn -> Enum.into(["1\n"], stream) end
    end)
  end

  test "prepare query and stream into different query with same name and savepoint raises", context do
    query = prepare("ENOENT", "COPY uniques FROM STDIN", [copy_data: true])
    :ok = close(query)
    _ = prepare("ENOENT", "SELECT 41")
    transaction(fn(conn) ->
      stream = stream(query, [], [mode: :savepoint])
      assert_raise RiakTS.Error, ~r"ERROR \(duplicate_prepared_statement\)",
        fn -> Enum.into(["1\n"], stream) end
      assert %RiakTS.Result{rows: [[42]]} =
        RiakTS.query!(conn, "SELECT 42", [])
    end)
  end

  test "prepare, close and stream into COPY FROM", context do
    query = prepare("copy", "COPY uniques FROM STDIN", [copy_data: true])
    :ok = close(query)
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert Enum.into(["2\n"], stream) == stream
      assert %RiakTS.Result{rows: [[2]]} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  @tag prepare: :unnamed
  test "stream named COPY FROM is unnamed when named not allowed", context do
    assert (%RiakTS.Query{name: ""} = query) =
      prepare("copy", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert Enum.into(["2\n", "3\n4\n"], stream) == stream
      assert Enum.into(["5\n"], stream) == stream
      assert %RiakTS.Result{rows: [[2], [3], [4], [5]]} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "COPY FROM prepared query on another connection", context do
    query = prepare("copy", "COPY uniques FROM STDIN", [copy_data: true])
    {:ok, pid2} = RiakTS.start_link(context[:options])
    RiakTS.transaction(pid2, fn(conn) ->
      stream = stream(query, [])
      assert Enum.into(["2\n"], stream) == stream
      assert %RiakTS.Result{rows: [[2]]} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "raise when executing prepared COPY FROM on connection with different types", context do
    query = prepare("copy", "COPY uniques FROM STDIN", [copy_data: true])

    {:ok, pid2} = RiakTS.start_link([decode_binary: :reference] ++
    context[:options])

    RiakTS.transaction(pid2, fn(conn) ->
      assert_raise ArgumentError, ~r"invalid types for the connection",
        fn() -> Enum.into(["1\n"], stream(query, [])) end
    end)
  end

  test "connection reuses prepared for COPY FROM after query", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert %Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
      assert Enum.into(["5\n"], stream) == stream
      RiakTS.rollback(conn, :done)
    end)
  end

  test "connection forces prepare on COPY FROM after prepare of same name", context do
    query_select = prepare("", "SELECT 42")
    query_copy = prepare("", "COPY uniques FROM STDIN", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query_copy, [])
      assert %Result{rows: [[42]]} = RiakTS.execute!(conn, query_select, [])
      assert Enum.into(["5\n"], stream) == stream

      stream = stream(query_copy, [], [mode: :savepoint])
      assert %Result{rows: [[42]]} = RiakTS.execute!(conn, query_select, [])
      assert Enum.into(["6\n"], stream) == stream

      assert %Result{rows: [[5], [6]]} =
        RiakTS.query!(conn, "SELECT * FROM uniques", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "raise when trying to COPY FROM unprepared query", context do
    query = %RiakTS.Query{name: "ENOENT", statement: "COPY uniques FROM STDIN",
                            copy_data: true}

    transaction(fn(conn) ->
      stream = stream(query, [])
      assert_raise ArgumentError, ~r/has not been prepared/,
        fn -> Enum.into(["5\n"], stream) end
    end)
  end

  test "raise when trying to COPY FROM reserved query", context do
    query = prepare("", "COPY uniques FROM STDIN", [copy_data: true])

    transaction(fn(conn) ->
      stream = stream(%RiakTS.Query{query | name: "POSTGREX_BEGIN"}, [])
      assert_raise ArgumentError, ~r/uses reserved name/,
        fn -> Enum.into(["5\n"], stream) end
    end)
  end

  test "stream into SELECT ignores data", context do
    query = prepare("", "SELECT 42", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert Enum.into(["42\n", "42\n"], stream) == stream
      assert %Result{rows: [[41]]} = RiakTS.query!(conn, "SELECT 41", [])

      stream = stream(query, [], [mode: :savepoint])
      assert Enum.into(["42\n", "42\n"], stream) == stream
      assert %Result{rows: [[41]]} = RiakTS.query!(conn, "SELECT 41", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "stream into COPY TO STDOUT ignores data", context do
    query = prepare("", "COPY (VALUES (1), (2)) TO STDOUT", [copy_data: true])
    transaction(fn(conn) ->
      stream = stream(query, [])
      assert Enum.into(["42\n", "42\n"], stream) == stream
      assert %Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])

      stream = stream(query, [], [mode: :savepoint])
      assert Enum.into(["42\n", "42\n"], stream) == stream
      assert %Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
      RiakTS.rollback(conn, :done)
    end)
  end

  test "connection works after stream into with failure in binding state", context do
    query = prepare("", "insert into uniques values (CAST($1::text AS int))", [copy_data: true])

    transaction(fn(conn) ->
      stream = stream(query, ["EBADF"])
      assert_raise RiakTS.Error, ~r"ERROR \(invalid_text_representation\)",
        fn() -> Enum.into(["42\n"], stream) end
    end)

    assert [[42]] = query("SELECT 42", [])

    transaction(fn(conn) ->
      stream = stream(query, ["EBADF"], [mode: :savepoint])
      assert_raise RiakTS.Error, ~r"ERROR \(invalid_text_representation\)",
        fn() -> Enum.into(["42\n"], stream) end

      assert %RiakTS.Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after stream into failure in executing state", context do
    query = prepare("", "insert into uniques values (1), (1)", [copy_data: true])

    transaction(fn(conn) ->
      stream = stream(query, [])
      assert_raise RiakTS.Error, ~r"ERROR \(unique_violation\)",
        fn() -> Enum.into(["42\n"], stream) end
    end)

    assert [[42]] = query("SELECT 42", [])

    transaction(fn(conn) ->
      stream = stream(query, [], [mode: :savepoint])
      assert_raise RiakTS.Error, ~r"ERROR \(unique_violation\)",
        fn() -> Enum.into(["42\n"], stream) end

      assert %RiakTS.Result{rows: [[42]]} = RiakTS.query!(conn, "SELECT 42", [])
    end)
  end

  test "empty query", context do
    query_out = prepare("out", "")
    query_in = prepare("in", "", [copy_data: true])
    transaction(fn(conn) ->
      assert [%RiakTS.Result{command: nil, rows: nil, num_rows: 0}] =
        stream(query_out, []) |> Enum.to_list()

      stream = stream(query_in, [], [log: &send(self(), &1)])

      assert Enum.into(["2\n3\n"], stream) == stream

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: nil, rows: nil, num_rows: 0}} = entry.result
    end)
  end

  test "savepoint query", context do
    query_out = prepare("out", "SAVEPOINT streaming_test")
    query_in = prepare("in", query_out.statement, [copy_data: true])
    transaction(fn(conn) ->
      assert [%RiakTS.Result{command: :savepoint, rows: nil, num_rows: 0}] =
        stream(query_out, []) |> Enum.to_list()

      stream = stream(query_in, [], [log: &send(self(), &1)])

      assert Enum.into(["2\n3\n"], stream) == stream

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :savepoint, rows: nil, num_rows: 0}} = entry.result
    end)
  end

  test "INSERT .. RETURNING", context do
    query_out = prepare("out", "INSERT INTO uniques (a) VALUES (2), (3) RETURNING a")
    query_in = prepare("in", query_out.statement, [copy_data: true])
    transaction(fn(conn) ->
      assert [%RiakTS.Result{command: :insert, rows: [[2], [3]], num_rows: 2}] =
        stream(query_out, [], [max_rows: 3]) |> Enum.to_list()
      RiakTS.rollback(conn, :done)
    end)

    transaction(fn(conn) ->
      assert [%{command: :stream, rows: [[2], [3]], num_rows: :stream},
              %{command: :insert, rows: [], num_rows: 2}] =
        stream(query_out, [], [max_rows: 2]) |> Enum.to_list()
      RiakTS.rollback(conn, :done)
    end)

    transaction(fn(conn) ->
      stream = stream(query_in, [], log: &send(self(), &1))
      assert Enum.into(["2\n3\n"], stream) == stream

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :copy_stream, rows: nil, num_rows: :copy_stream}} = entry.result

      assert_received %DBConnection.LogEntry{} = entry
      assert (entry.query).query == query_in
      assert {:ok, %{command: :insert, rows: [[2], [3]], num_rows: 2}} = entry.result

      RiakTS.rollback(conn, :done)
    end)
  end
end
