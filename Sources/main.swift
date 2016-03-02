import CoreFoundation
import CLibpq

let connection = try! PGConnection(host: "localhost", port: 5432, database: "test")

public class Q : Query {
    public let sql: String

    init(_ sql: String) {
        self.sql = sql
    }
}

/*
print(connection)

connection.execute(Q("SELECT * FROM generate_series(0, 10)"), onSuccess: { result in

    print("Got a result of \(result.count) rows")

    for row in result {
        let a: Int = row[0]
        print("Got int \(a)")
    }
}, onFailure: { error in
    print("Error: \(error)")
})

connection.execute(Q("SELECT 123::smallint"), onSuccess: { result in

    print("Got a result of \(result.count) rows")

    for row in result {
        let a: Int = row[0]
        print("Got int \(a)")
    }
}, onFailure: { error in
    print("Error: \(error)")
})

connection.execute(Q("SELECT 1234::int"), onSuccess: { result in

    print("Got a result of \(result.count) rows")

    for row in result {
        let a: Int = row[0]
        print("Got int \(a)")
    }
}, onFailure: { error in
    print("Error: \(error)")
})

connection.execute(Q("SELECT 12345::bigint"), onSuccess: { result in

    print("Got a result of \(result.count) rows")

    for row in result {
        let a: Int = row[0]
        print("Got int \(a)")
    }
}, onFailure: { error in
    print("Error: \(error)")
})

connection.execute(Q("SELECT 'HELLO FROM POSTGRES'::varchar"), onSuccess: { result in

    print("Got a result of \(result.count) rows")

    for row in result {
        let a: String? = row[0]
        print("Got string \(a)")
    }
}, onFailure: { error in
    print("Error: \(error)")
})
*/

connection.execute(Q("SELECT '{0,1,2,3,4,3,2,1}'::int[]"), onSuccess: { result in

    print("Got a result of \(result.count) rows")

    for row in result {
        let a: [Int?] = row[0]
        print("Array: \(a)")
    }

}, onFailure: { error in
    print("Error: \(error)")
})

sleep(10)
