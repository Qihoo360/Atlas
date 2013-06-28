require("time.ticker")

module("proxy.ticker", package.seeall)

function tick()
	return ticker.tick()
end
