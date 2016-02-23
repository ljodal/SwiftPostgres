# SwiftPostgres

This is a simple wrapper around `libpq`, which operates 100% asynchronously.
Async operation is achieved by setting the connection to non-blocking mode and
using `libdispatch` to monitor the underlaying socket.
