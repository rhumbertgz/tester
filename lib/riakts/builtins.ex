defmodule RiakTS.Timestamp do
  @moduledoc """
  Struct for RiakTS timestamp.

  ## Fields
    * `year`
    * `month`
    * `day`
    * `hour`
    * `min`
    * `sec`
    * `usec`
  """

  #TODO check this

  @type t :: %__MODULE__{year: 0..10000, month: 1..12, day: 1..31,
                         hour: 0..23, min: 0..59, sec: 0..59, usec: 0..999_999}

  defstruct [
    year: 0,
    month: 1,
    day: 1,
    hour: 0,
    min: 0,
    sec: 0,
    usec: 0]
end
