class Clickpulse < Formula
  desc "A heartbeat monitor for ClickHouse — Prometheus metrics exporter"
  homepage "https://github.com/ppiankov/clickpulse"
  url "https://github.com/ppiankov/clickpulse/archive/refs/tags/v0.1.0.tar.gz"
  sha256 ""
  license "MIT"
  version "0.1.0"

  depends_on "go" => :build

  def install
    ldflags = "-s -w -X main.version=#{version}"
    system "go", "build", *std_go_args(ldflags:), "./cmd/clickpulse"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/clickpulse version")
  end
end
