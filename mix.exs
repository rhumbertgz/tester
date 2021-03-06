defmodule RiakTS.Mixfile do
  use Mix.Project

  @version "0.11.2"

  def project do
    [app: :riakts,
     version: @version,
     elixir: "~> 1.0",
     deps: deps(),
     name: "RiakTS-Ecto Client",
     source_url: "https://github.com/rhumbertgz/riakts-ecto",
     docs: [source_ref: "v#{@version}", main: "readme", extras: ["README.md"]],
     description: description(),
     package: package()]
  end

  # Configuration for the OTP application
  def application do
    [applications: [:logger, :db_connection, :decimal],
     mod: {RiakTS.App, []},
     env: [type_server_reap_after: 3 * 60_000]]
  end

  defp deps do
    [{:ex_doc, "~> 0.12", only: :dev},
     {:decimal, "~> 1.0"},
     {:db_connection, "~> 1.0-rc"},
     {:connection, "~> 1.0"}]
  end

  defp description do
    "Riak TS driver for Elixir."
  end

  defp package do
    [maintainers: ["Humberto Rodriguez Avila"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/rhumbertgz/riakts-ecto"}]
  end
end
