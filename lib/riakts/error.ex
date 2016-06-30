defmodule RiakTS.Error do

  alias RiakTS.{Error, ErrorCode}

  defexception [:message, :riakts, :connection_id]

  @nonposix_errors [:closed, :timeout]
  @metadata [:table, :column, :constraint]

  def message(e) do
    if kw = e.postgres do
      "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
      <> build_metadata(kw)
      <> build_detail(kw)
    else
      e.message
    end
  end

  defp build_metadata(kw) do
    metadata = for k <- @metadata, v = kw[k], do: "\n    #{k}: #{v}"
    case metadata do
      [] -> ""
      _  -> "\n" <> Enum.join(metadata)
    end
  end

  defp build_detail(kw) do
    if v = kw[:detail], do: "\n\n" <> v, else: ""
  end

  def exception([riakts: fields]) do
    fields = Enum.into(fields, %{})
             |> Map.put(:rts_code, fields[:code])
             |> Map.update!(:code, &ErrorCode.code_to_name/1)

    %Error{riakts: fields}
  end

  def exception([tag: :ssl, action: action, reason: :timeout]) do
    %Error{message: "ssl #{action}: timeout"}
  end

  def exception([tag: :ssl, action: action, reason: reason]) do
    formatted_reason = :ssl.format_error(reason)
    %Error{message: "ssl #{action}: #{formatted_reason} - #{inspect(reason)}"}
  end

  def exception([tag: :tcp, action: action, reason: reason]) when not reason in @nonposix_errors do
    formatted_reason = :inet.format_error(reason)
    %Error{message: "tcp #{action}: #{formatted_reason} - #{inspect(reason)}"}
  end

  def exception([tag: :tcp, action: action, reason: reason]) do
    %Error{message: "tcp #{action}: #{reason}"}
  end

  def exception(arg) do
    super(arg)
  end
end
