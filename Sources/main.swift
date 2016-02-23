import CoreFoundation

let connection = try! PGConnection(host: "localhost", port: 5432, database: "test")

print(connection)

sleep(10)