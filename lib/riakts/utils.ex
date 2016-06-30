defmodule RiakTS.Utils do
  @moduledoc false

  @extensions [
    RiakTS.Extensions.Boolean,
    RiakTS.Extensions.Timestamp,
    RiakTS.Extensions.SInt64,
    RiakTS.Extensions.Double]

#TODO check this
  @doc """
  Converts pg major.minor.patch (http://www.postgresql.org/support/versioning) version to an integer
  """
  def parse_version(version) do
    list =
      version
      |> String.split(".")
      |> Enum.map(&elem(Integer.parse(&1), 0))

    case list do
      [major, minor, patch] -> {major, minor, patch}
      [major, minor] -> {major, minor, 0}
      [major] -> {major, 0, 0}
    end
  end

  @doc """
  Fills in the given `opts` with default options.
  """
  @spec default_opts(Keyword.t) :: Keyword.t
  def default_opts(opts) do
    opts
    |> Keyword.put_new(:username, System.get_env("RiakTS_USER") || System.get_env("USER"))
    |> Keyword.put_new(:password, System.get_env("RiakTS_PASSWORD"))
    |> Keyword.put_new(:hostname, System.get_env("RiakTS_HOST") || "localhost")
    |> Keyword.update(:port, normalize_port(System.get_env("RiakTS_PORT")), &normalize_port/1)
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp normalize_port(port) when is_binary(port), do: String.to_integer(port)
  defp normalize_port(port), do: port

  @doc """
  List all default extensions.
  """
  @spec default_extensions(Keyword.t) :: [{module(), Keyword.t}]
  def default_extensions(opts \\ []) do
    Enum.map(@extensions, &{&1, opts})
  end

  @doc """
  Return encode error message.
  """
  def encode_msg(%RiakTS.TypeInfo{type: type}, observed, expected) do
    "RiakTS expected #{to_desc(expected)} that can be encoded/cast to " <>
    "type #{inspect type}, got #{inspect observed}. Please make sure the " <>
    "value you are passing matches the definition in your table or in your " <>
    "query or convert the value accordingly."
  end

  ## Helpers

  defp to_desc(struct) when is_atom(struct), do: "%#{inspect struct}{}"
  defp to_desc(%Range{} = range), do: "an integer in #{inspect range}"
  defp to_desc({a, b}), do: to_desc(a) <> " or " <> to_desc(b)
  defp to_desc(desc) when is_binary(desc), do: desc
end
