defmodule RiakTS.Extensions.Bool do
  @moduledoc false
  import RiakTS.BinaryUtils

  use RiakTS.BinaryExtension, send: "bool_send"

  def encode(_, true, _, _),
    do: <<1>>
  def encode(_, false, _, _),
    do: <<0>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      RiakTS.Utils.encode_msg(type_info, value, "a boolean")
  end

  def decode(_, <<1 :: int8>>, _, _),
    do: true
  def decode(_, <<0 :: int8>>, _, _),
    do: false
end
