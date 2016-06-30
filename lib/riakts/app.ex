defmodule RiakTS.App do
  @moduledoc false
  use Application

  def start(_, _) do
    import Supervisor.Spec
    opts = [strategy: :one_for_one, name: RiakTS.Supervisor]
    children = [worker(RiakTS.TypeServer, []),
                worker(RiakTS.Parameters, [])]
    Supervisor.start_link(children, opts)
  end
end
