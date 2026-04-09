const http = require("http");
const { greet } = require("@example/utils");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ message: greet("world") }));
});

server.listen(3000, "127.0.0.1", () => {
  console.log("Web server listening on 127.0.0.1:3000");
});
