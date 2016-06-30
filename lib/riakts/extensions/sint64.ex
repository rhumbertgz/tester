defmodule RiakTS.Extensions.SInt64 do
  @moduledoc false
  import RiakTS.BinaryUtils
  use RiakTS.BinaryExtension, send: "sint64_send"

  @int64_range -9223372036854775808..9223372036854775807

  def encode(_, n, _, _) when is_integer(n) and n in @int64_range,
    do: <<n :: int64>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      RiakTS.Utils.encode_msg(type_info, value, @int64_range)
  end

  def decode(_, <<n :: int64>>, _, _),
    do: n
end
