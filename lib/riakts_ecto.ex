defmodule RiakTS do
  @moduledoc """
  Riak TS driver for Elixir.

  This module handles the connection to RiakTS, providing support
  for queries, transactions, connection backoff, logging, pooling and
  more.

  """
  alias RiakTS.Query

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3`.
  """
  @type conn :: DBConnection.conn

  # attributes
  @pool_timeout 5000
  @timeout 15_000
  @max_rows 500


  @doc """
  Start the connection process and connect to RiakTS.

  ## Options

  * `:hostname` - Server hostname (default: RiakTS_HOST env variable, then localhost);
  * `:port` - Server port (default: RiakTS_PORT env variable, then 5432);
  * `:database` - Database (required);
  * `:username` - Username (default: RiakTS_USER env variable, then USER env var);
  * `:password` - User password (default RiakTS_PASSWORD);
  * `:parameters` - Keyword list of connection parameters;
  * `:timeout` - Connect timeout in milliseconds (default: `#{@timeout}`);
  * `:ssl` - Set to `true` if ssl should be used (default: `false`);
  * `:ssl_opts` - A list of ssl options, see ssl docs;
  * `:socket_options` - Options to be given to the underlying socket;
  * `:extensions` - A list of `{module, opts}` pairs where `module` is
  implementing the `RiakTS.Extension` behaviour and `opts` are the
  extension options;
  * `:decode_binary` - Either `:copy` to copy binary values when decoding with
  default extensions that return binaries or `:reference` to use a reference
  counted binary of the binary received from the socket. Referencing a
  potentially larger binary can be more efficient if the binary value is going
  to be garbaged collected soon because a copy is avoided. However the larger
  binary can not be garbage collected until all references are garbage
  collected (defaults to `:copy`);
  * `:prepare` - How to prepare queries, either `:named` to use named queries
  or `:unnamed` to force unnamed queries (default: `:named`);
  * `:transactions` - Set to `:strict` to error on unexpected transaction
  state, otherwise set to `naive` (default: `:naive`);
  * `:pool` - The pool module to use, see `DBConnection` for pool dependent
  options, this option must be included with all requests contacting the pool
  if not `DBConnection.Connection` (default: `DBConnection.Connection`);
  * `:null` - The atom to use as a stand in for postgres' `NULL` in encoding
  and decoding (default: `nil`);

  `RiakTS` uses the `DBConnection` framework and supports all `DBConnection`
  options. See `DBConnection` for more information.
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, RiakTS.Error.t | term}
  def start_link(opts) do
    opts = [types: true] ++ RiakTS.Utils.default_opts(opts)
    DBConnection.start_link(RiakTS.Protocol, opts)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %RiakTS.Result{}}`
  or `{:error, %RiakTS.Error{}}` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how RiakTS
  encodes and decodes Elixir values by default. See `RiakTS.Result` for the
  result data.

  ## Options

  * `:pool_timeout` - Time to wait in the queue for the connection
  (default: `#{@pool_timeout}`)
  * `:queue` - Whether to wait for connection in a queue (default: `true`);
  * `:timeout` - Query request timeout (default: `#{@timeout}`);
  * `:decode_mapper` - Fun to map each row in the result to a term after
  decoding, (default: `fn x -> x end`);
  * `:pool` - The pool module to use, must match that set on
  `start_link/1`, see `DBConnection`
  * `:null` - The atom to use as a stand in for postgres' `NULL` in encoding
  and decoding;
  * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
  query on error, otherwise set to `:transaction` (default: `:transaction`);
  * `:copy_data` - Whether to add copy data as a final parameter for use
  with `COPY .. FROM STDIN` queries, if the query is not copying to the
  database the data is sent but silently discarded (default: `false`);

  ## Examples

  RiakTS.query(conn, "CREATE TABLE posts (id serial, title text)", [])

  RiakTS.query(conn, "INSERT INTO posts (title) VALUES ('my title')", [])

  RiakTS.query(conn, "SELECT title FROM posts", [])

  RiakTS.query(conn, "SELECT id FROM posts WHERE title like $1", ["%my%"])

  RiakTS.query(conn, "COPY posts TO STDOUT", [])

  RiakTS.query(conn, "COPY ints FROM STDIN", ["1\n2\n"], [copy_data: true])
  """
  @spec query(conn, iodata, list, Keyword.t) :: {:ok, RiakTS.Result.t} | {:error, RiakTS.Error.t}
  def query(conn, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement}
    case DBConnection.prepare_execute(conn, query, params, defaults(opts)) do
      {:ok, _, result} ->
        {:ok, result}
        {:error, %ArgumentError{} = err} ->
          raise err
          {:error, %RuntimeError{} = err} ->
            raise err
            {:error, _} = error ->
              error
            end
          end

          @doc """
          Runs an (extended) query and returns the result or raises `RiakTS.Error` if
          there was an error. See `query/3`.
          """
          @spec query!(conn, iodata, list, Keyword.t) :: RiakTS.Result.t
          def query!(conn, statement, params, opts \\ []) do
            query = %Query{name: "", statement: statement}
            {_, result} = DBConnection.prepare_execute!(conn, query, params, defaults(opts))
            result
          end

          @doc """
          Prepares an (extended) query and returns the result as
          `{:ok, %RiakTS.Query{}}` or `{:error, %RiakTS.Error{}}` if there was an
          error. Parameters can be set in the query as `$1` embedded in the query
          string. To execute the query call `execute/4`. To close the prepared query
          call `close/3`. See `RiakTS.Query` for the query data.

          ## Options

          * `:pool_timeout` - Time to wait in the queue for the connection
          (default: `#{@pool_timeout}`)
          * `:queue` - Whether to wait for connection in a queue (default: `true`);
          * `:timeout` - Prepare request timeout (default: `#{@timeout}`);
          * `:pool` - The pool module to use, must match that set on
          `start_link/1`, see `DBConnection`
          * `:null` - The atom to use as a stand in for postgres' `NULL` in encoding
          and decoding;
          * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
          prepare on error, otherwise set to `:transaction` (default: `:transaction`);
          * `:copy_data` - Whether to add copy data as the final parameter for use
          with `COPY .. FROM STDIN` queries, if the query is not copying to the
          database then the data is sent but ignored (default: `false`);

          ## Examples

          RiakTS.prepare(conn, "CREATE TABLE posts (id serial, title text)")
          """
          @spec prepare(conn, iodata, iodata, Keyword.t) :: {:ok, RiakTS.Query.t} | {:error, RiakTS.Error.t}
          def prepare(conn, name, statement, opts \\ []) do
            query = %Query{name: name, statement: statement}
            case DBConnection.prepare(conn, query, defaults(opts)) do
              {:error, %ArgumentError{} = err} ->
                raise err
                {:error, %RuntimeError{} = err} ->
                  raise err
                  other ->
                    other
                  end
                end

                @doc """
                Prepares an (extended) query and returns the prepared query or raises
                `RiakTS.Error` if there was an error. See `prepare/4`.
                """
                @spec prepare!(conn, iodata, iodata, Keyword.t) :: RiakTS.Query.t
                def prepare!(conn, name, statement, opts \\ []) do
                  DBConnection.prepare!(conn, %Query{name: name, statement: statement}, defaults(opts))
                end

                @doc """
                Runs an (extended) prepared query and returns the result as
                `{:ok, %RiakTS.Result{}}` or `{:error, %RiakTS.Error{}}` if there was an
                error. Parameters are given as part of the prepared query, `%RiakTS.Query{}`.
                See the README for information on how RiakTS encodes and decodes Elixir
                values by default. See `RiakTS.Query` for the query data and
                `RiakTS.Result` for the result data.

                ## Options

                * `:pool_timeout` - Time to wait in the queue for the connection
                (default: `#{@pool_timeout}`)
                * `:queue` - Whether to wait for connection in a queue (default: `true`);
                * `:timeout` - Execute request timeout (default: `#{@timeout}`);
                * `:decode_mapper` - Fun to map each row in the result to a term after
                decoding, (default: `fn x -> x end`);
                * `:pool` - The pool module to use, must match that set on
                `start_link/1`, see `DBConnection`
                * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
                execute on error, otherwise set to `:transaction` (default: `:transaction`);

                ## Examples

                query = RiakTS.prepare!(conn, "CREATE TABLE posts (id serial, title text)")
                RiakTS.execute(conn, query, [])

                query = RiakTS.prepare!(conn, "SELECT id FROM posts WHERE title like $1")
                RiakTS.execute(conn, query, ["%my%"])
                """
                @spec execute(conn, RiakTS.Query.t, list, Keyword.t) ::
                {:ok, RiakTS.Result.t} | {:error, RiakTS.Error.t}
                def execute(conn, query, params, opts \\ []) do
                  case DBConnection.execute(conn, query, params, defaults(opts)) do
                    {:error, %ArgumentError{} = err} ->
                      raise err
                      {:error, %RuntimeError{} = err} ->
                        raise err
                        other ->
                          other
                        end
                      end

                      @doc """
                      Runs an (extended) prepared query and returns the result or raises
                      `RiakTS.Error` if there was an error. See `execute/4`.
                      """
                      @spec execute!(conn, RiakTS.Query.t, list, Keyword.t) :: RiakTS.Result.t
                      def execute!(conn, query, params, opts \\ []) do
                        DBConnection.execute!(conn, query, params, defaults(opts))
                      end

                      @doc """
                      Closes an (extended) prepared query and returns `:ok` or
                      `{:error, %RiakTS.Error{}}` if there was an error. Closing a query releases
                      any resources held by postgresql for a prepared query with that name. See
                      `RiakTS.Query` for the query data.

                      ## Options

                      * `:pool_timeout` - Time to wait in the queue for the connection
                      (default: `#{@pool_timeout}`)
                      * `:queue` - Whether to wait for connection in a queue (default: `true`);
                      * `:timeout` - Close request timeout (default: `#{@timeout}`);
                      * `:pool` - The pool module to use, must match that set on
                      `start_link/1`, see `DBConnection`
                      * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
                      close on error, otherwise set to `:transaction` (default: `:transaction`);

                      ## Examples

                      query = RiakTS.prepare!(conn, "CREATE TABLE posts (id serial, title text)")
                      RiakTS.close(conn, query)
                      """
                      @spec close(conn, RiakTS.Query.t, Keyword.t) :: :ok | {:error, RiakTS.Error.t}
                      def close(conn, query, opts \\ []) do
                        case DBConnection.close(conn, query, defaults(opts)) do
                          {:ok, _} ->
                            :ok
                            {:error, %ArgumentError{} = err} ->
                              raise err
                              {:error, %RuntimeError{} = err} ->
                                raise err
                                {:error, _} = error ->
                                  error
                                end
                              end

                              @doc """
                              Closes an (extended) prepared query and returns `:ok` or raises
                              `RiakTS.Error` if there was an error. See `close/3`.
                              """
                              @spec close!(conn, RiakTS.Query.t, Keyword.t) :: :ok
                              def close!(conn, query, opts \\ []) do
                                DBConnection.close!(conn, query, defaults(opts))
                              end

                              @doc """
                              Acquire a lock on a connection and run a series of requests inside a
                              transaction. The result of the transaction fun is return inside an `:ok`
                              tuple: `{:ok, result}`.

                              To use the locked connection call the request with the connection
                              reference passed as the single argument to the `fun`. If the
                              connection disconnects all future calls using that connection
                              reference will fail.

                              `rollback/2` rolls back the transaction and causes the function to
                              return `{:error, reason}`.

                              `transaction/3` can be nested multiple times if the connection
                              reference is used to start a nested transaction. The top level
                              transaction function is the actual transaction.

                              ## Options

                              * `:pool_timeout` - Time to wait in the queue for the connection
                              (default: `#{@pool_timeout}`)
                              * `:queue` - Whether to wait for connection in a queue (default: `true`);
                              * `:timeout` - Transaction timeout (default: `#{@timeout}`);
                              * `:pool` - The pool module to use, must match that set on
                              `start_link/1`, see `DBConnection`;
                              * `:mode` - Set to `:savepoint` to use savepoints instead of an SQL
                              transaction, otherwise set to `:transaction` (default: `:transaction`);


                              The `:timeout` is for the duration of the transaction and all nested
                              transactions and requests. This timeout overrides timeouts set by internal
                              transactions and requests. The `:pool` and `:mode` will be used for all
                              requests inside the transaction function.

                              ## Example

                              {:ok, res} = RiakTS.transaction(pid, fn(conn) ->
                              RiakTS.query!(conn, "SELECT title FROM posts", [])
                            end)
                            """
                            @spec transaction(conn, ((DBConnection.t) -> result), Keyword.t) ::
                            {:ok, result} | {:error, any} when result: var
                            def transaction(conn, fun, opts \\ []) do
                              DBConnection.transaction(conn, fun, defaults(opts))
                            end

                            @doc """
                            Rollback a transaction, does not return.

                            Aborts the current transaction fun. If inside multiple `transaction/3`
                            functions, bubbles up to the top level.

                            ## Example

                            {:error, :oops} = RiakTS.transaction(pid, fn(conn) ->
                            DBConnection.rollback(conn, :bar)
                            IO.puts "never reaches here!"
                          end)
                          """
                          @spec rollback(DBConnection.t, any) :: no_return()
                          defdelegate rollback(conn, any), to: DBConnection

                          @doc """
                          Returns a cached map of connection parameters.

                          ## Options

                          * `:pool_timeout` - Call timeout (default: `#{@pool_timeout}`)
                          * `:pool` - The pool module to use, must match that set on
                          `start_link/1`, see `DBConnection`

                          """
                          @spec parameters(conn, Keyword.t) :: %{binary => binary}
                          def parameters(conn, opts \\ []) do
                            DBConnection.execute!(conn, %RiakTS.Parameters{}, nil, defaults(opts))
                          end

                          @doc """
                          Returns a supervisor child specification for a DBConnection pool.
                          """
                          @spec child_spec(Keyword.t) :: Supervisor.Spec.spec
                          def child_spec(opts) do
                            opts = [types: true] ++ RiakTS.Utils.default_opts(opts)
                            DBConnection.child_spec(RiakTS.Protocol, opts)
                          end

                          @doc """
                          Returns a stream for a prepared query on a connection.

                          Stream consumes memory in chunks of at most `max_rows` rows (see Options).
                          This is useful for processing _large_ datasets.

                          A stream must be wrapped in a transaction and may be used as an `Enumerable`
                          or a `Collectable`.

                          When used as an `Enumerable` with a `COPY .. TO STDOUT` SQL query no other
                          queries or streams can be interspersed until the copy has finished. Otherwise
                          it is possible to intersperse enumerable streams and queries.

                          When used as a `Collectable` the query must have been prepared with
                          `copy_data: true`, otherwise it will raise. Instead of using an extra
                          parameter for the copy data, the data from the enumerable is copied to the
                          database. No other queries or streams can be interspersed until the copy has
                          finished. If the query is not copying to the database the copy data will still
                          be sent but is silently discarded.

                          ### Options

                          * `:max_rows` - Maximum numbers of rows in a result (default to `#{@max_rows}`)
                          * `:decode_mapper` - Fun to map each row in the result to a term after
                          decoding, (default: `fn x -> x end`);
                          * `:mode` - set to `:savepoint` to use a savepoint to rollback to before an
                          execute on error, otherwise set to `:transaction` (default: `:transaction`);

                          ## Examples

                          RiakTS.transaction(pid, fn(conn) ->
                          query = RiakTS.prepare!(conn, "COPY posts TO STDOUT")
                          stream = RiakTS.stream(conn, query, [])
                          Enum.into(stream, File.stream!("posts"))
                        end)

                        RiakTS.transaction(pid, fn(conn) ->
                        query = RiakTS.prepare!(conn, "COPY posts FROM STDIN", [copy_data: true])
                        stream = RiakTS.stream(conn, query, [])
                        Enum.into(File.stream!("posts"), stream)
                      end)
                      """
                      @spec stream(DBConnection.t, RiakTS.Query.t, list, Keyword.t) :: RiakTS.Stream.t
                      def stream(%DBConnection{} = conn, query, params, options \\ [])  do
                        max_rows = options[:max_rows] || @max_rows
                        %RiakTS.Stream{conn: conn, max_rows: max_rows, options: options,
                        params: params, query: query}
                      end

                      ## Helpers
                      defp defaults(opts) do
                        Keyword.put_new(opts, :timeout, @timeout)
                      end

                    end
